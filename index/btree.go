package index

import (
	"bytes"
	"go-bitcask/data"
	"sort"
	"sync"

	"github.com/google/btree"
)

// BTree 封装了Google开源的Btree: https://github.com/google/btree
type BTree struct {
	tree *btree.BTree // 多个goroutine对btree的写操作不是并发安全的
	lock *sync.RWMutex
}

// NewBTree 初始化Btree索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

// Put 向索引中存储key对应的数据位置信息
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

// Get 根据key取出对应的索引位置信息
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

// Delete 根据key删除对应的索引位置信息
func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	return oldItem != nil
}

// Size 索引中的数据大小
func (bt *BTree) Size() int {
	return bt.tree.Len()
}

// Iterator 返回索引迭代器
func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil

	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)

}

// BTree 索引迭代器
type btreeIterator struct {
	currIndex int     // 当前索引下标位置
	reverse   bool    // 是否逆序遍历
	values    []*Item // 索引值
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// 将所有的数据存放到数组中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器起点
func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

// Seek 根据传入的 key 查找第一个大于（或小于）等于目标 key， 根据这个 key 开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个 key
func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

// Valid 是否已经遍历完了所有 key ，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

// Key 当前遍历位置的 Key 数据
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

// Value 当前遍历位置的 Value 数据
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

// Close 关闭迭代器，释放相关资源
func (bti *btreeIterator) Close() {
	bti.values = nil
}
