package gobitcask

import (
	"go-bitcask/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	it1 := db.NewIterator(DefaultIteratorOption)
	assert.NotNil(t, it1)
	assert.Equal(t, false, it1.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	it1 := db.NewIterator(DefaultIteratorOption)
	assert.NotNil(t, it1)
	assert.Equal(t, true, it1.Valid())
	assert.Equal(t, utils.GetTestKey(10), it1.Key())
	var1, err := it1.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(10), var1)
}

func TestDB_Iterator_Multi_Value(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go-3")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("ababa"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ayabb"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("gfebc"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bywbd"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("lawbd"), utils.RandomValue(10))
	assert.Nil(t, err)

	// 正向迭代
	it1 := db.NewIterator(DefaultIteratorOption)
	assert.NotNil(t, it1)
	for it1.Rewind(); it1.Valid(); it1.Next() {
		// t.Log("key = ", string(it1.Key()))
		assert.NotNil(t, it1.Key())
	}
	it1.Rewind()
	for it1.Seek([]byte("c")); it1.Valid(); it1.Next() {
		// t.Log("key = ", string(it1.Key()))
		assert.NotNil(t, it1.Key())

	}

	// 反向迭代
	it_ops1 := DefaultIteratorOption
	it_ops1.Reverse = true
	it2 := db.NewIterator(it_ops1)
	assert.NotNil(t, it2)
	for it2.Rewind(); it2.Valid(); it2.Next() {
		// t.Log("key = ", string(it2.Key()))
		assert.NotNil(t, it2.Key())
	}
	it2.Rewind()
	for it2.Seek([]byte("c")); it2.Valid(); it2.Next() {
		// t.Log("key = ", string(it2.Key()))
		assert.NotNil(t, it2.Key())
	}

	// 指定了 Prefix
	it_ops2 := DefaultIteratorOption
	it_ops2.Prefix = []byte("ab")
	it3 := db.NewIterator(it_ops2)
	for it3.Rewind(); it3.Valid(); it3.Next() {
		// t.Log("key = ", string(it3.Key()))
		assert.NotNil(t, it3.Key())
	}
}
