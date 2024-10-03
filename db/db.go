package db

import (
	"bytes"
	"encoding/gob"
	"sync"
)

type BPlusTreeNode struct {
	Keys     []int
	Children []*BPlusTreeNode
	IsLeaf   bool
	Next     *BPlusTreeNode
	Mutex    sync.RWMutex
}

type BPlusTree struct {
	Root  *BPlusTreeNode
	Order int
	Mutex sync.RWMutex
}

// NewBPlusTree creates a new B+Tree with a dynamically set order.
func NewBPlusTree(size int) *BPlusTree {
	order := calculateOrder(size)
	root := &BPlusTreeNode{
		Keys:     make([]int, 0, order),
		Children: make([]*BPlusTreeNode, 0, order+1),
		IsLeaf:   true,
	}
	return &BPlusTree{Root: root, Order: order}
}

// calculateOrder determines the order of the B+Tree based on the size.
func calculateOrder(size int) int {
	// Assuming each key-pointer pair is 16 bytes and the block size is 4 KB
	const blockSize = 4096
	const keyPointerSize = 16

	// Calculate the maximum number of key-pointer pairs that fit in a block
	maxOrder := blockSize / keyPointerSize

	// Ensure the order is within the range of 32 to 256
	if maxOrder > 256 {
		maxOrder = 256
	} else if maxOrder < 32 {
		maxOrder = 32
	}

	// Adjust the order based on the size
	if size < 1000 {
		return maxOrder / 4
	} else if size < 10000 {
		return maxOrder / 2
	} else {
		return maxOrder
	}
}

func (tree *BPlusTree) Insert(key int) {
	tree.Mutex.Lock()
	defer tree.Mutex.Unlock()

	root := tree.Root
	if len(root.Keys) == 0 {
		root.Keys = append(root.Keys, key)
		return
	}

	if len(root.Keys) == tree.Order {
		newRoot := &BPlusTreeNode{
			Children: []*BPlusTreeNode{root},
		}
		tree.splitChild(newRoot, 0)
		tree.Root = newRoot
	}
	tree.insertNonFull(tree.Root, key)
}

func (tree *BPlusTree) insertNonFull(node *BPlusTreeNode, key int) {
	node.Mutex.Lock()
	defer node.Mutex.Unlock()

	if node.IsLeaf {
		i := 0
		for i < len(node.Keys) && node.Keys[i] < key {
			i++
		}
		node.Keys = append(node.Keys[:i], append([]int{key}, node.Keys[i:]...)...)
	} else {
		i := 0
		for i < len(node.Keys) && node.Keys[i] < key {
			i++
		}
		child := node.Children[i]
		child.Mutex.Lock()
		if len(child.Keys) == tree.Order {
			child.Mutex.Unlock()
			tree.splitChild(node, i)
			if key > node.Keys[i] {
				i++
			}
		} else {
			child.Mutex.Unlock()
		}
		tree.insertNonFull(node.Children[i], key)
	}
}

func (tree *BPlusTree) splitChild(parent *BPlusTreeNode, index int) {
	child := parent.Children[index]
	mid := len(child.Keys) / 2
	midKey := child.Keys[mid]

	newChild := &BPlusTreeNode{
		Keys:   append([]int(nil), child.Keys[mid+1:]...),
		IsLeaf: child.IsLeaf,
	}

	if len(child.Children) > 0 {
		newChild.Children = append([]*BPlusTreeNode(nil), child.Children[mid+1:]...)
		child.Children = child.Children[:mid+1]
	}

	child.Keys = child.Keys[:mid]

	if len(parent.Keys) == 0 {
		parent.Keys = append(parent.Keys, midKey)
	} else {
		parent.Keys = append(parent.Keys[:index], append([]int{midKey}, parent.Keys[index:]...)...)
	}
	parent.Children = append(parent.Children[:index+1], append([]*BPlusTreeNode{newChild}, parent.Children[index+1:]...)...)

	if child.IsLeaf {
		newChild.Next = child.Next
		child.Next = newChild
	}
}

func (tree *BPlusTree) Search(key int) bool {
	tree.Mutex.RLock()
	defer tree.Mutex.RUnlock()
	return tree.search(tree.Root, key)
}

func (tree *BPlusTree) search(node *BPlusTreeNode, key int) bool {
	node.Mutex.RLock()
	defer node.Mutex.RUnlock()

	i := 0
	for i < len(node.Keys) && key > node.Keys[i] {
		i++
	}
	if i < len(node.Keys) && key == node.Keys[i] {
		return true
	}
	if node.IsLeaf {
		return false
	}
	return tree.search(node.Children[i], key)
}

func (tree *BPlusTree) Delete(key int) {
	tree.Mutex.Lock()
	defer tree.Mutex.Unlock()
	tree.delete(tree.Root, key)
}

func (tree *BPlusTree) delete(node *BPlusTreeNode, key int) {
	node.Mutex.Lock()
	defer node.Mutex.Unlock()

	i := 0
	for i < len(node.Keys) && key > node.Keys[i] {
		i++
	}

	if node.IsLeaf {
		if i < len(node.Keys) && node.Keys[i] == key {
			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
		}
	} else {
		if i < len(node.Keys) && node.Keys[i] == key {
			if len(node.Children[i].Keys) > tree.Order/2 {
				node.Keys[i] = tree.getPredecessor(node.Children[i])
				tree.delete(node.Children[i], node.Keys[i])
			} else if len(node.Children[i+1].Keys) > tree.Order/2 {
				node.Keys[i] = tree.getSuccessor(node.Children[i+1])
				tree.delete(node.Children[i+1], node.Keys[i])
			} else {
				tree.mergeChildren(node, i)
				tree.delete(node.Children[i], key)
			}
		} else {
			if len(node.Children[i].Keys) == tree.Order/2 {
				tree.fixChild(node, i)
			}
			tree.delete(node.Children[i], key)
		}
	}
}

func (tree *BPlusTree) getPredecessor(node *BPlusTreeNode) int {
	for !node.IsLeaf {
		node = node.Children[len(node.Children)-1]
	}
	return node.Keys[len(node.Keys)-1]
}

func (tree *BPlusTree) getSuccessor(node *BPlusTreeNode) int {
	for !node.IsLeaf {
		node = node.Children[0]
	}
	return node.Keys[0]
}

func (tree *BPlusTree) fixChild(parent *BPlusTreeNode, index int) {
	child := parent.Children[index]
	if index > 0 && len(parent.Children[index-1].Keys) > tree.Order/2 {
		leftSibling := parent.Children[index-1]
		child.Keys = append([]int{parent.Keys[index-1]}, child.Keys...)
		parent.Keys[index-1] = leftSibling.Keys[len(leftSibling.Keys)-1]
		leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
		if len(leftSibling.Children) > 0 {
			child.Children = append([]*BPlusTreeNode{leftSibling.Children[len(leftSibling.Children)-1]}, child.Children...)
			leftSibling.Children = leftSibling.Children[:len(leftSibling.Children)-1]
		}
	} else if index < len(parent.Children)-1 && len(parent.Children[index+1].Keys) > tree.Order/2 {
		rightSibling := parent.Children[index+1]
		child.Keys = append(child.Keys, parent.Keys[index])
		parent.Keys[index] = rightSibling.Keys[0]
		rightSibling.Keys = rightSibling.Keys[1:]
		if len(rightSibling.Children) > 0 {
			child.Children = append(child.Children, rightSibling.Children[0])
			rightSibling.Children = rightSibling.Children[1:]
		}
	} else {
		if index > 0 {
			tree.mergeChildren(parent, index-1)
		} else {
			tree.mergeChildren(parent, index)
		}
	}
}

func (tree *BPlusTree) mergeChildren(parent *BPlusTreeNode, index int) {
	leftChild := parent.Children[index]
	rightChild := parent.Children[index+1]

	leftChild.Keys = append(leftChild.Keys, parent.Keys[index])
	leftChild.Keys = append(leftChild.Keys, rightChild.Keys...)
	leftChild.Children = append(leftChild.Children, rightChild.Children...)

	parent.Keys = append(parent.Keys[:index], parent.Keys[index+1:]...)
	parent.Children = append(parent.Children[:index+1], parent.Children[index+2:]...)

	if leftChild.IsLeaf {
		leftChild.Next = rightChild.Next
	}

	if parent == tree.Root && len(parent.Keys) == 0 {
		tree.Root = leftChild
	}
}

func (node *BPlusTreeNode) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(node.Keys); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.Children); err != nil {
		return nil, err
	}
	if err := enc.Encode(node.IsLeaf); err != nil {
		return nil, err
	}
	if node.Next != nil {
		if err := enc.Encode(true); err != nil {
			return nil, err
		}
		if err := enc.Encode(node.Next); err != nil {
			return nil, err
		}
	} else {
		if err := enc.Encode(false); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (node *BPlusTreeNode) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&node.Keys); err != nil {
		return err
	}
	if err := dec.Decode(&node.Children); err != nil {
		return err
	}
	if err := dec.Decode(&node.IsLeaf); err != nil {
		return err
	}
	var hasNext bool
	if err := dec.Decode(&hasNext); err != nil {
		return err
	}
	if hasNext {
		node.Next = &BPlusTreeNode{}
		if err := dec.Decode(node.Next); err != nil {
			return err
		}
	} else {
		node.Next = nil
	}
	return nil
}

func (tree *BPlusTree) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(tree.Root); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (tree *BPlusTree) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&tree.Root); err != nil {
		return err
	}
	return nil
}

func init() {
	gob.Register(&BPlusTree{})
	gob.Register(&BPlusTreeNode{})
}