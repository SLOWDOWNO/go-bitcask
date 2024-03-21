package index

import (
	"go-bitcask/data"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBPTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-Put")
	_ = os.Mkdir(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
}

func TestBPTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-Get")
	_ = os.Mkdir(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos1 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos1)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 1234})
	pos2 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos2)

}

func TestBPTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-Delete")
	_ = os.Mkdir(path, os.ModePerm)
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)

	res1 := tree.Delete([]byte("not exits"))
	assert.False(t, res1)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	res2 := tree.Delete([]byte("aac"))
	assert.True(t, res2)

	pos1 := tree.Get([]byte("aac"))
	assert.Nil(t, pos1)

}

func TestBPTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-Size")
	_ = os.Mkdir(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	assert.Zero(t, tree.Size())

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 111})
	tree.Put([]byte("aag"), &data.LogRecordPos{Fid: 123, Offset: 111})
	tree.Put([]byte("aah"), &data.LogRecordPos{Fid: 123, Offset: 111})
	assert.Equal(t, 3, tree.Size())
}

func TestBpTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-Iter")
	_ = os.Mkdir(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 111})
	tree.Put([]byte("yut"), &data.LogRecordPos{Fid: 123, Offset: 111})
	tree.Put([]byte("aah"), &data.LogRecordPos{Fid: 123, Offset: 111})
	tree.Put([]byte("adc"), &data.LogRecordPos{Fid: 123, Offset: 111})
	tree.Put([]byte("nhh"), &data.LogRecordPos{Fid: 123, Offset: 111})
	tree.Put([]byte("rey"), &data.LogRecordPos{Fid: 123, Offset: 111})

	it := tree.Iterator(false)
	for it.Rewind(); it.Valid(); it.Next() {
		assert.NotNil(t, it.Key())
		assert.NotNil(t, it.Value())
	}
}
