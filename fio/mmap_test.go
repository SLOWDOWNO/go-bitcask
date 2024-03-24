package fio

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("/tmp", "mmap-read_test.data")
	defer destoryFile(path)

	mmapIO, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	// 文件为空
	b1 := make([]byte, 10)
	n1, err := mmapIO.Read(b1, 0)
	assert.Zero(t, n1)
	assert.Equal(t, io.EOF, err)

	// 借用标准文件iox写入数据
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("adad"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("fgdf"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("liul"))
	assert.Nil(t, err)

	mmapIO2, err := NewMMapIOManager(path)
	assert.Nil(t, err)
	size, err := mmapIO2.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(12), size)

	b2 := make([]byte, 4)
	n2, err := mmapIO2.Read(b2, 4)
	assert.Equal(t, 4, n2)
	assert.Nil(t, err)
	assert.Equal(t, "fgdf", string(b2))

}
