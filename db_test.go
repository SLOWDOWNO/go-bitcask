package gobitcask

import (
	"go-bitcask/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 测试完成销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.activeFile.Close()
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestDB_Open(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go")
	t.Log(dir)
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 正常 Put 一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	value1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, value1)

	// 重复 Put 相同数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	value2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, value2)

	// key 为空
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// value 为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	value3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, len(value3), 0)

	// 写到数据文件进行转换的情况
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	// t.Log(len(db.oldFiles))
	assert.Equal(t, 2, len(db.oldFiles))

	// 重启数据库
	db2, err := Open(opts)
	// defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	value4 := utils.RandomValue(128)
	err = db2.Put(utils.GetTestKey(55), value4)
	assert.Nil(t, err)
	value5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, value4, value5)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 正常读一条数据
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
	assert.Nil(t, err)
	value1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, value1)

	// 读一个不存在的 key
	value2, err := db.Get([]byte("some key unknow"))
	assert.Nil(t, value2)
	assert.Equal(t, ErrKeyNotFound, err)

	// 值被反复 Put 再读取
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	value3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, value3)

	// 值被删除再 Get
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	value4, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(value4))
	assert.Equal(t, ErrKeyNotFound, err)

	// 当前活跃文件转换为旧的数据文件，从旧的数据文件读取 value
	for i := 100; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.oldFiles))
	value5, err := db.Get(utils.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, value5)

	// 重启 DB 后，前面 Put 的数据都能 Get 到
	err = db.activeFile.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	value6, err := db2.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, value6)
	assert.Equal(t, value1, value6)

	value7, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, value7)
	assert.Equal(t, value3, value7)

	value8, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(value8))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 删除一个存在的 key
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	// 删除一个不存在的 key
	err = db.Delete([]byte("unknown-key"))
	assert.Nil(t, err)

	// 删除一个空的 ket
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 被删除重新 Put
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	value1, err := db.Get(utils.GetTestKey(22))
	assert.NotNil(t, value1)
	assert.Nil(t, err)

	// 重启数据库
	err = db.activeFile.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	_, err = db2.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	value2, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, value1, value2)
}
