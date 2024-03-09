package gobitcask

import (
	"go-bitcask/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_WriteBatch1(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写数据并但不 Commit
	wb := db.NewWriteBtach(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// 正常提交数据
	err = wb.Commit()
	assert.Nil(t, err)

	value1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, value1)
	assert.Nil(t, err)

	// 删除有效的数据
	wb2 := db.NewWriteBtach(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	value2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, value2)
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultOption
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBtach(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(11), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// 重启DB
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	_, err = db2.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// 校验事务序列号
	assert.Equal(t, 2, db.seqNo)
}
