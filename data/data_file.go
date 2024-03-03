package data

import "go-bitcask/fio"

const DataFileNameSuffix = ".data"

// DataFile 磁盘中数据文件的结构体
// 会有多个磁盘文件
type DataFile struct {
	FileId    uint32        // 文件id
	WriteOff  uint64        // 文件写到哪个位置
	IoManager fio.IOManager // io 读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// TODO
	return nil, nil
}

// ReadLogRecord根据 偏移量 读取数据文件
func (df *DataFile) ReadLogRecord(offset uint64) (*LogRecord, uint64, error) {
	// TODO
	return nil, 0, nil
}

// Write 将buf写入数据文件
func (df *DataFile) Write(buf []byte) error {
	// TODO
	return nil
}

// Sync 持久化数据文件到磁盘
func (df *DataFile) Sync() error {
	// TODO
	return nil
}
