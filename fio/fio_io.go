package fio

import "os"

// 标准文件IO，封装Go提供的标准系统文件接口
type FileIO struct {
	fd *os.File // 系统文件描述符号
}

// NewFileIOManager 初始化标准文件IO
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

// Read 从 offset 处开始读取 len(b) 个字节
// 返回读取的字节数和错误
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Write 将 b 中的 len(b) 个字节写入文件
// 返回写入的字节数和错误
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// Sync 将持久化数据
// 一般意义上意味着 将文件系统的内存中最近写入的的数据写入磁盘
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close 关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
