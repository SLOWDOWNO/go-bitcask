package index

import (
	"bytes"
	"go-bitcask/data"

	"github.com/google/btree"
)

// 索引接口，方便接入其他的数据结构
type Indexer interface {
	// 向索引中存储key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// 根据key取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// 根据key删除对应的索引位置信息
	Delete(key []byte) bool
}

// BTree中的元素
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// 实现Google btree的Item接口
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}