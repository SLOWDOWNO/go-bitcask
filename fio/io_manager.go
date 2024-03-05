package fio

const DataFilePerm = 0644

type IOManager interface {
	// 从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	// 写入字节数组到文件中
	Write([]byte) (int, error)

	// 持久化数据
	Sync() error

	// 关闭文件
	Close() error

	// 获取文件大小
	Size() (int64, error)
}

// 初始化IOManager， 目前仅支持标准文件IO
func NewIOManager(filName string) (IOManager, error) {
	return NewFileIOManager(filName)
}
