package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	StandardFIO FileIOType = iota
	MemoryMap
)

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
func NewIOManager(filName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(filName)
	case MemoryMap:
		return NewMMapIOManager(filName)
	default:
		panic("unsupoorted io type")
	}
}
