package gobitcask

import (
	"go-bitcask/data"
	"go-bitcask/index"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前唯一的活跃数据文件
	oldFiles   map[uint32]*data.DataFile // 旧的数据文件，只读
	index      index.Indexer             // 内存索引
}

// Put 写入Key-Value数据，key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到活跃文件
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); ok != nil {
		return ErrIndexUpdateFaild
	}

	return nil
}

// Get 根据Key读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	// 判断key是否有效
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存索引中取出 key对应的的内存索引信息
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件id找到对应数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFiles[logRecordPos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量读取对应的数据
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// 判断数据是否已被删除
	if logRecord.Type == data.LogRecordDelete {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// 追加写入数据到活跃文件
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前活跃文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	// 如果不存在初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 数据编码
	// 如果写入数据编码已经到达活跃文件的阈值，关闭活跃文件，打开新的活跃文件
	encRecord, size := data.EncodeLogRecord(logRecord)
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 持久化当前活跃文件到磁盘
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转化成旧的数据文件
		db.oldFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入编码数据到数据文件
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if db.options.syncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造内存索引信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// setActiveDataFile 设置当前活跃文件
// 在使用此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialLFileId uint32 = 0
	if db.activeFile == nil {
		initialLFileId = db.activeFile.FileId + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialLFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
