package index

import (
	"go-bitcask/data"
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
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
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
