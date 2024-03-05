package gobitcask

import "os"

type Options struct {
	// 数据库存放数据的目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 内存索引类型
	IndexType IndexerType

	// 每次写数据是否持久化
	syncWrite bool
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART 自适应基数树索引
	ART
)

var DefaultOption = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	IndexType:    BTree,
	syncWrite:    false,
}
