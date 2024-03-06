package index

import "go-bitcask/data"

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器起点
	Rewind()

	// Seek 根据传入的 key 查找第一个大于（或小于）等于目标 key， 根据这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否已经遍历完了所有 key ，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 Key 数据
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放相关资源
	Close()
}
