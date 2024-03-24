package fio

import (
	"os"

	"golang.org/x/exp/mmap"
)

// MMap IO, 内存文件映射， 仅用于启动流程建立索引
type MMap struct {
	readerAt *mmap.ReaderAt
}

// NewMMapIOManager 初始化 MMap IO
func NewMMapIOManager(filename string) (*MMap, error) {
	_, err := os.OpenFile(filename, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

// 从文件的给定位置读取对应的数据
func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

// 写入字节数组到文件中
func (mmap *MMap) Write([]byte) (int, error) {
	panic("not implemented")
}

// 持久化数据
func (mmap *MMap) Sync() error {
	panic("not implemented")
}

// 关闭文件
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

// 获取文件大小
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
