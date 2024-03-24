package index

import (
	"bytes"
	"go-bitcask/data"

	"github.com/google/btree"
)

// Indexer 索引接口，方便接入其他的数据结构
type Indexer interface {
	// Put 向索引中存储key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据key取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据key删除对应的索引位置信息
	Delete(key []byte) bool

	// Iterator 返回索引迭代器
	Iterator(reverse bool) Iterator

	// Size 索引中的数据
	Size() int
}

type IndexType = int8

// 索引类型枚举
const (
	// Btree 索引
	Btree IndexType = iota + 1

	// B+ tree 索引
	BPTree
)

// NewIndexer 根据具体类型初始化索引
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
}

// BTree中的元素
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 实现Google btree的Item接口
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
