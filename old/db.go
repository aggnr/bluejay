package dataframe_row

import (
"sync"
//"fmt"
)

const (
	maxKeys = 4 // Maximum number of keys in a node
)

type BPlusTreeNode struct {
	keys     []int
	children []*BPlusTreeNode
	isLeaf   bool
	next     *BPlusTreeNode
	mutex    sync.RWMutex
}

type BPlusTree struct {
	root  *BPlusTreeNode
	mutex sync.RWMutex
}

func NewBPlusTree() *BPlusTree {
	root := &BPlusTreeNode{
		keys:     make([]int, 0, maxKeys),
		children: make([]*BPlusTreeNode, 0, maxKeys+1),
		isLeaf:   true,
	}
	return &BPlusTree{root: root}
}

func (tree *BPlusTree) Insert(key int) {
	tree.mutex.Lock()
	defer tree.mutex.Unlock()

	root := tree.root
	if len(root.keys) == maxKeys {
		newRoot := &BPlusTreeNode{
			children: []*BPlusTreeNode{root},
		}
		tree.splitChild(newRoot, 0)
		tree.root = newRoot
	}
	tree.insertNonFull(tree.root, key)
}

func (tree *BPlusTree) insertNonFull(node *BPlusTreeNode, key int) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	if node.isLeaf {
		i := 0
		for i < len(node.keys) && node.keys[i] < key {
			i++
		}
		node.keys = append(node.keys[:i], append([]int{key}, node.keys[i:]...)...)
	} else {
		i := 0
		for i < len(node.keys) && node.keys[i] < key {
			i++
		}
		child := node.children[i]
		child.mutex.Lock()
		if len(child.keys) == maxKeys {
			child.mutex.Unlock()
			tree.splitChild(node, i)
			if key > node.keys[i] {
				i++
			}
		} else {
			child.mutex.Unlock()
		}
		tree.insertNonFull(node.children[i], key)
	}
}

func (tree *BPlusTree) splitChild(parent *BPlusTreeNode, index int) {
	child := parent.children[index]
	mid := len(child.keys) / 2

	// Store the middle key before modifying the child.keys slice
	midKey := child.keys[mid]

	newChild := &BPlusTreeNode{
		keys:   append([]int(nil), child.keys[mid+1:]...),
		isLeaf: child.isLeaf,
	}

	if len(child.children) > 0 {
		newChild.children = append([]*BPlusTreeNode(nil), child.children[mid+1:]...)
		child.children = child.children[:mid+1]
	}

	child.keys = child.keys[:mid]

	if len(parent.keys) == 0 {
		parent.keys = append(parent.keys, midKey)
	} else {
		parent.keys = append(parent.keys[:index], append([]int{midKey}, parent.keys[index:]...)...)
	}
	parent.children = append(parent.children[:index+1], append([]*BPlusTreeNode{newChild}, parent.children[index+1:]...)...)

	if child.isLeaf {
		newChild.next = child.next
		child.next = newChild
	}
}

func (tree *BPlusTree) Search(key int) bool {
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()
	return tree.search(tree.root, key)
}

func (tree *BPlusTree) search(node *BPlusTreeNode, key int) bool {
	node.mutex.RLock()
	defer node.mutex.RUnlock()

	i := 0
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}
	if i < len(node.keys) && key == node.keys[i] {
		return true
	}
	if node.isLeaf {
		return false
	}
	return tree.search(node.children[i], key)
}

func (tree *BPlusTree) Delete(key int) {
	tree.mutex.Lock()
	defer tree.mutex.Unlock()
	tree.delete(tree.root, key)
}

func (tree *BPlusTree) delete(node *BPlusTreeNode, key int) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	// Find the key in the current node
	i := 0
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}

	if node.isLeaf {
		// If the node is a leaf, remove the key if it exists
		if i < len(node.keys) && node.keys[i] == key {
			node.keys = append(node.keys[:i], node.keys[i+1:]...)
		}
	} else {
		// If the node is not a leaf, recurse into the appropriate child
		child := node.children[i]
		child.mutex.Lock()
		if len(child.keys) == maxKeys/2 {
			child.mutex.Unlock()
			tree.fixChild(node, i)
		} else {
			child.mutex.Unlock()
		}
		tree.delete(node.children[i], key)
	}
}

func (tree *BPlusTree) fixChild(parent *BPlusTreeNode, index int) {
	child := parent.children[index]
	if index > 0 && len(parent.children[index-1].keys) > maxKeys/2 {
		// Borrow a key from the left sibling
		leftSibling := parent.children[index-1]
		leftSibling.mutex.Lock()
		defer leftSibling.mutex.Unlock()

		child.keys = append([]int{parent.keys[index-1]}, child.keys...)
		parent.keys[index-1] = leftSibling.keys[len(leftSibling.keys)-1]
		leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]

		if !child.isLeaf {
			child.children = append([]*BPlusTreeNode{leftSibling.children[len(leftSibling.children)-1]}, child.children...)
			leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]
		}
	} else if index < len(parent.children)-1 && len(parent.children[index+1].keys) > maxKeys/2 {
		// Borrow a key from the right sibling
		rightSibling := parent.children[index+1]
		rightSibling.mutex.Lock()
		defer rightSibling.mutex.Unlock()

		child.keys = append(child.keys, parent.keys[index])
		parent.keys[index] = rightSibling.keys[0]
		rightSibling.keys = rightSibling.keys[1:]

		if !child.isLeaf {
			child.children = append(child.children, rightSibling.children[0])
			rightSibling.children = rightSibling.children[1:]
		}
	} else {
		// Merge with a sibling
		if index > 0 {
			index--
		}
		tree.mergeChildren(parent, index)
	}
}

func (tree *BPlusTree) mergeChildren(parent *BPlusTreeNode, index int) {
	leftChild := parent.children[index]
	rightChild := parent.children[index+1]

	leftChild.keys = append(leftChild.keys, parent.keys[index])
	leftChild.keys = append(leftChild.keys, rightChild.keys...)
	leftChild.children = append(leftChild.children, rightChild.children...)

	parent.keys = append(parent.keys[:index], parent.keys[index+1:]...)
	parent.children = append(parent.children[:index+1], parent.children[index+2:]...)

	if leftChild.isLeaf {
		leftChild.next = rightChild.next
	}

	if parent == tree.root && len(parent.keys) == 0 {
		tree.root = leftChild
	}
}