package db

import (
	"sync"
)

const (
	maxKeys = 4 // Maximum number of keys in a node
)

type BPlusTreeNode struct {
	keys     []int
	values   [][2]int // Store tuples of (rowChunkIndex, columnChunkIndex)
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
		values:   make([][2]int, 0, maxKeys),
		children: make([]*BPlusTreeNode, 0, maxKeys+1),
		isLeaf:   true,
	}
	return &BPlusTree{root: root}
}

func (tree *BPlusTree) Insert(key, rowChunkIndex, columnChunkIndex int) {
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
	tree.insertNonFull(tree.root, key, rowChunkIndex, columnChunkIndex)
}

func (tree *BPlusTree) insertNonFull(node *BPlusTreeNode, key, rowChunkIndex, columnChunkIndex int) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	if node.isLeaf {
		i := len(node.keys) - 1
		node.keys = append(node.keys, 0)
		node.values = append(node.values, [2]int{})
		for i >= 0 && key < node.keys[i] {
			node.keys[i+1] = node.keys[i]
			node.values[i+1] = node.values[i]
			i--
		}
		node.keys[i+1] = key
		node.values[i+1] = [2]int{rowChunkIndex, columnChunkIndex}
	} else {
		i := len(node.keys) - 1
		for i >= 0 && key < node.keys[i] {
			i--
		}
		i++
		node.mutex.Unlock()
		node.children[i].mutex.Lock()
		if len(node.children[i].keys) == maxKeys {
			node.children[i].mutex.Unlock()
			tree.splitChild(node, i)
			if key > node.keys[i] {
				i++
			}
		} else {
			node.children[i].mutex.Unlock()
		}
		node.mutex.Lock()
		tree.insertNonFull(node.children[i], key, rowChunkIndex, columnChunkIndex)
	}
}

func (tree *BPlusTree) splitChild(parent *BPlusTreeNode, index int) {
	child := parent.children[index]
	mid := len(child.keys) / 2

	midKey := child.keys[mid]
	midValue := child.values[mid]

	newChild := &BPlusTreeNode{
		keys:     append([]int(nil), child.keys[mid+1:]...),
		values:   append([][2]int(nil), child.values[mid+1:]...),
		isLeaf:   child.isLeaf,
	}

	if !child.isLeaf {
		newChild.children = append([]*BPlusTreeNode(nil), child.children[mid+1:]...)
		child.children = child.children[:mid+1]
	}

	child.keys = child.keys[:mid]
	child.values = child.values[:mid]

	parent.keys = append(parent.keys[:index], append([]int{midKey}, parent.keys[index:]...)...)
	parent.values = append(parent.values[:index], append([][2]int{midValue}, parent.values[index:]...)...)
	parent.children = append(parent.children[:index+1], append([]*BPlusTreeNode{newChild}, parent.children[index+1:]...)...)

	if child.isLeaf {
		newChild.next = child.next
		child.next = newChild
	}
}

func (tree *BPlusTree) Search(key int) ([2]int, bool) {
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()
	return tree.search(tree.root, key)
}

func (tree *BPlusTree) search(node *BPlusTreeNode, key int) ([2]int, bool) {
	node.mutex.RLock()
	defer node.mutex.RUnlock()

	i := 0
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}
	if i < len(node.keys) && key == node.keys[i] {
		return node.values[i], true
	}
	if node.isLeaf {
		return [2]int{}, false
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

	i := 0
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}

	if node.isLeaf {
		if i < len(node.keys) && node.keys[i] == key {
			node.keys = append(node.keys[:i], node.keys[i+1:]...)
			node.values = append(node.values[:i], node.values[i+1:]...)
		}
	} else {
		if i < len(node.keys) && node.keys[i] == key {
			if len(node.children[i].keys) >= maxKeys/2 {
				// Handle deletion from internal node
			} else {
				// Handle merging or borrowing
			}
		} else {
			if len(node.children[i].keys) < maxKeys/2 {
				tree.fixChild(node, i)
			}
			tree.delete(node.children[i], key)
		}
	}
}

func (tree *BPlusTree) fixChild(parent *BPlusTreeNode, index int) {
	child := parent.children[index]
	if index > 0 && len(parent.children[index-1].keys) > maxKeys/2 {
		leftSibling := parent.children[index-1]
		child.keys = append([]int{parent.keys[index-1]}, child.keys...)
		child.values = append([][2]int{parent.values[index-1]}, child.values...)
		if !child.isLeaf {
			child.children = append([]*BPlusTreeNode{leftSibling.children[len(leftSibling.children)-1]}, child.children...)
			leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]
		}
		parent.keys[index-1] = leftSibling.keys[len(leftSibling.keys)-1]
		parent.values[index-1] = leftSibling.values[len(leftSibling.values)-1]
		leftSibling.keys = leftSibling.keys[:len(leftSibling.keys)-1]
		leftSibling.values = leftSibling.values[:len(leftSibling.values)-1]
	} else if index < len(parent.children)-1 && len(parent.children[index+1].keys) > maxKeys/2 {
		rightSibling := parent.children[index+1]
		child.keys = append(child.keys, parent.keys[index])
		child.values = append(child.values, parent.values[index])
		if !child.isLeaf {
			child.children = append(child.children, rightSibling.children[0])
			rightSibling.children = rightSibling.children[1:]
		}
		parent.keys[index] = rightSibling.keys[0]
		parent.values[index] = rightSibling.values[0]
		rightSibling.keys = rightSibling.keys[1:]
		rightSibling.values = rightSibling.values[1:]
	} else {
		if index < len(parent.children)-1 {
			tree.mergeChildren(parent, index)
		} else {
			tree.mergeChildren(parent, index-1)
		}
	}
}

func (tree *BPlusTree) mergeChildren(parent *BPlusTreeNode, index int) {
	leftChild := parent.children[index]
	rightChild := parent.children[index+1]

	leftChild.keys = append(leftChild.keys, parent.keys[index])
	leftChild.values = append(leftChild.values, parent.values[index])
	leftChild.keys = append(leftChild.keys, rightChild.keys...)
	leftChild.values = append(leftChild.values, rightChild.values...)
	leftChild.children = append(leftChild.children, rightChild.children...)

	parent.keys = append(parent.keys[:index], parent.keys[index+1:]...)
	parent.values = append(parent.values[:index], parent.values[index+1:]...)
	parent.children = append(parent.children[:index+1], parent.children[index+2:]...)

	if leftChild.isLeaf {
		leftChild.next = rightChild.next
	}

	if parent == tree.root && len(parent.keys) == 0 {
		tree.root = leftChild
	}
}