package byodb

import (
	"bytes"
	"encoding/binary"
)

const (
	HEADER          = 4
	BTREE_PAGE_SIZE = 4096 // 4KB

	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max > BTREE_PAGE_SIZE {
		panic("BTree node size exceeds page size")
	}
}

type BNode struct {
	// | type | nkeys | pointers   | offsets | key-values
	// | 2B   | 2B    | nkeys * 8B | nkeys * 2B | ...
	// the format of the key-values is:
	// | klen | vlen | key | val |
	// | 2B   | 2B   | ... | ... |
	data []byte // can dump the data to disk
}

const (
	BNODE_NODE = iota + 1
	BNODE_LEAF
)

// decode the header of a BNode
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointer
func (node BNode) getPtr(idx uint16) uint64 {
	if idx > node.nkeys() {
		panic("index out of bounds")
	}
	pos := HEADER + idx*8
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(idx uint16, value uint64) {
	if idx > node.nkeys() {
		panic("index out of bounds")
	}
	pos := HEADER + idx*8
	binary.LittleEndian.PutUint64(node.data[pos:], value)
}

// offset
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

func offsetPos(node BNode, idx uint16) uint16 {
	if idx < 1 || idx > node.nkeys() {
		panic("index out of bounds")
	}
	return HEADER + node.nkeys()*8 + (idx-1)*2
}

// key-values
func (node BNode) kvPos(idx uint16) uint16 {
	if idx > node.nkeys() {
		panic("index out of bounds")
	}
	return HEADER + node.nkeys()*8 + node.nkeys()*2 + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	if idx > node.nkeys() {
		panic("index out of bounds")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	if idx > node.nkeys() {
		panic("index out of bounds")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen : pos+4+klen+vlen]
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

type BTree struct {
	// pointer(a nonzero page number) to the root node
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode
	new func(BNode) uint64
	del func(uint64)
}

// nodeLookupGE finds the first key in the node that is greater than or equal to the given key.
// todo: bisect
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp > 0 {
			break
		}
	}
	return found
}

func leafInsert(new BNode, old BNode, idx uint16,
	key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func leafUpdate(new BNode, old BNode, idx uint16,
	key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx-1)
}

// nodeAppendRange appends a range of pointers and offsets from the old node to the new node.
func nodeAppendRange(new BNode, old BNode,
	dstNew uint16, srcOld uint16, n uint16) {
	if srcOld+n > old.nkeys() || dstNew+n > new.nkeys() {
		panic("index out of bounds")
	}
	if n == 0 {
		return
	}
	// copy pointers
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}
	// copy offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(0); i < n; i++ {
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}
	// copy key-values
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// kvs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)
	// offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+uint16(4+len(key)+len(val)))
}

func (tree *BTree) Insert(key []byte, val []byte) {
	if len(key) <= 0 || len(val) <= 0 {
		panic("key or value cannot be empty")
	}
	if len(key) > BTREE_MAX_KEY_SIZE || len(val) > BTREE_MAX_VAL_SIZE {
		panic("key or value too large")
	}

	if tree.root == 0 {
		// create a new root node
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	node := tree.get(tree.root)
	tree.del(tree.root)

	node = treeInsert(tree, node, key, val)
	nsplit, splitted := nodeSplit3(node)

	if nsplit > 1 {
		// the root was split, create a new level
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range splitted[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(splitted[0])
	}
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val)
		} else {
			leafInsert(new, node, idx, key, val)
		}
	case BNODE_NODE:
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("unknown node type")
	}
	return new
}

func nodeInsert(tree *BTree, new BNode, node BNode, idx uint16,
	key []byte, val []byte) {
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)
	knode = treeInsert(tree, knode, key, val)
	// split the result
	nsplit, splited := nodeSplit3(knode)
	nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

// nodeSplit2 split a bigger-than-allowed node into two.
// the second node always fits on a page.
func nodeSplit2(left BNode, right BNode, old BNode) {
	if old.nkeys() < 2 {
		panic("nodeSplit2 called on a node with less than 2 keys")
	}

	nkeys := old.nkeys()
	// 从中间开始分裂
	nleft := nkeys / 2

	// 计算左半部分大小的辅助函数
	leftBytes := func() uint16 {
		return HEADER + nleft*8 + nleft*2 + old.getOffset(nleft)
	}

	// 计算右半部分大小的辅助函数
	rightBytes := func() uint16 {
		return HEADER + (nkeys-nleft)*8 + (nkeys-nleft)*2 +
			old.kvPos(nkeys) - old.kvPos(nleft)
	}

	// 调整分裂点,使左右两边都能装入页面
	// 优先确保右边装入,因为右边要求必须装入
	for rightBytes() > BTREE_PAGE_SIZE && nleft < nkeys-1 {
		nleft++
	}
	// 如果右边仍然太大,说明无法分裂
	if rightBytes() > BTREE_PAGE_SIZE {
		panic("cannot split: right node too big")
	}

	// 尝试平衡左右大小,但保持右边必须装入
	for nleft > 1 && leftBytes() > BTREE_PAGE_SIZE &&
		rightBytes()+old.kvPos(nleft)-old.kvPos(nleft-1) <= BTREE_PAGE_SIZE {
		nleft--
	}

	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nkeys-nleft)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nkeys-nleft)

	// 验证右节点一定能装入页面
	if right.nbytes() > BTREE_PAGE_SIZE {
		panic("split failed: right node too big")
	}
}

// nodeSplit3 splits a node into three parts if it is too big.
// the results are 1~3 nodes
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}

	left := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)} // might be split again
	right := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	// the left node is too big, split it again
	leftleft := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftleft, middle, left)
	if leftleft.nbytes() > BTREE_PAGE_SIZE {
		panic("leftleft node is too big")
	}
	return 3, [3]BNode{leftleft, middle, right}
}

func nodeReplaceKid1ptr(new BNode, old BNode, idx uint16, ptr uint64) {
	copy(new.data, old.data[:old.nbytes()])
	new.setPtr(idx, ptr)
}

// nodeReplace2Kid replaces two child nodes at position idx in the parent node with a new child node and key.
func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-idx-2)
}
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, kid := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(kid), kid.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-idx-1)
}

func (tree *BTree) Delete(key []byte) bool {
	if len(key) <= 0 {
		panic("key cannot be empty")
	}
	if len(key) > BTREE_MAX_KEY_SIZE {
		panic("key too large")
	}

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false // key not found
	}

	tree.del(tree.root)
	if updated.btype() == BNODE_LEAF && updated.nkeys() == 1 {
		// the root node is now empty, remove it
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true
}

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{data: nil} // key not found
		}
		new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("unknown node type")
	}
}

func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-idx-1)
}

// nodeDelete deletes a key from a node.
// tips: this function is merge nodes instead of splitting nodes.
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated.data) == 0 {
		return BNode{data: nil} // key not found
	}
	tree.del(kptr)

	new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch mergeDir {
	case -1:
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case +1:
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case 0:
		if updated.nkeys() <= 0 {
			return BNode{data: nil}
		}
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

// nodeMerge merges two nodes into a new node.
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(BNODE_LEAF, left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{data: nil} // no merge needed
	}
	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if idx+1 < node.nkeys() {
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}
	}
	return 0, BNode{data: nil} // no merge possible
}
