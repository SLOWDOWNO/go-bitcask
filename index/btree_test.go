package index

import (
	"go-bitcask/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBtree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, uint64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.Nil(t, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, uint64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 2, Offset: 888})
	assert.Nil(t, res3)
	res4 := bt.Delete([]byte("a"))
	assert.True(t, res4)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	// BTree 为空时，Iterator 无效
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// BTree 有数据的情况
	bt1.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	// t.Log(iter2.Key(), iter2.Value())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 有多条数据
	bt1.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 1})
	bt1.Put([]byte("b"), &data.LogRecordPos{Fid: 1, Offset: 2})
	bt1.Put([]byte("c"), &data.LogRecordPos{Fid: 1, Offset: 3})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		// t.Log(iter3.Key(), iter3.Value())
		assert.NotNil(t, iter3.Key())
		assert.NotNil(t, iter3.Value())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		// t.Log(iter4.Key(), iter4.Value())
		assert.NotNil(t, iter4.Key())
		assert.NotNil(t, iter4.Value())
	}

	// 测试 Seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("b")); iter5.Valid(); iter5.Next() {
		// t.Log(iter5.Key(), iter5.Value())
		assert.NotNil(t, iter5.Key())
	}

	// 反向遍历 Seek
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("b")); iter6.Valid(); iter6.Next() {
		// t.Log(iter6.Key(), iter6.Value())
		assert.NotNil(t, iter6.Key())
	}

}
