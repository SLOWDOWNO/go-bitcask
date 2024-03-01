package gobitcask

type Options struct {
	// 数据库存放数据的目录
	DirPath string

	// 数据文件的大小
	DataFileSize uint64

	// 每次写数据是否持久化
	syncWrite bool
}
