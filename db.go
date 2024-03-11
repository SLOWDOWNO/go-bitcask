package gobitcask

import (
	"errors"
	"go-bitcask/data"
	"go-bitcask/index"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int                     // 文件id只能在加载索引的时候使用
	activeFile *data.DataFile            // 当前唯一的活跃数据文件
	oldFiles   map[uint32]*data.DataFile // 旧的数据文件
	index      index.Indexer             // 内存索引
	seqNo      uint64                    // 事务序列号， 全局递增
	isMerging  bool                      // 是否正在 merge
}

// Open 打开bitcask存储引擎实例并返回
func Open(options Options) (*DB, error) {
	// 校验用户传入的配置项
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在，创建目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化DB实例结构体
	db := &DB{
		options:  options,
		mu:       new(sync.RWMutex),
		oldFiles: make(map[uint32]*data.DataFile),
		index:    index.NewIndexer(options.IndexType),
	}

	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	// 从 hint file 加载索引
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	// 从数据文件中构建索引
	if err := db.loadIndexFromDataFile(); err != nil {
		return nil, err
	}

	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	if db.activeFile != nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧的数据文件
	for _, file := range db.oldFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 同步数据到磁盘
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Put 写入Key-Value数据，key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord结构体
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到活跃文件
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFaild
	}

	return nil
}

// Delete 根据key删除对应的数据
func (db *DB) Delete(key []byte) error {
	// 判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 检查key是否存在，不存在直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造LogRecord，标识数据已被删除
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDelete,
	}
	// 写入数据文件
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}

	// 删除对应key的内存索引
	ok := db.index.Delete(key)
	if !ok {
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

	// 从数据文件中获取 value
	return db.getValueByPosition(logRecordPos)
}

// ListKey 获取数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// 获取所有的数据, 并执行用户指定操作
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// 根据索引信息获取对应的 value
func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	// 根据文件id找到对应数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFiles[pos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量读取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	// 判断数据是否已被删除
	if logRecord.Type == data.LogRecordDelete {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// appendLogRecordWithLock 加锁版本的追加写入数据到活跃文件
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord 追加写入数据到活跃文件
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
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
	// 当前存在活跃文件的情况
	if db.activeFile != nil {
		initialLFileId = db.activeFile.FileId + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialLFileId)
	if err != nil {
		return err
	}
	// 更新当前文件为活跃文件
	db.activeFile = dataFile
	return nil
}

// checkOptions 校验用户传入的配置项
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than zero")
	}
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int

	// 遍历目录中的数据文件，找到所有以'.data'为结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录有可能损坏
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件Id进行从小到大排序
	sort.Ints(fileIds)
	// 后续加载索引时复用
	db.fileIds = fileIds

	// 遍历每个文件Id，打开对应的数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { // 当前是活跃文件
			db.activeFile = dataFile
		} else { // 当前是旧的文件
			db.oldFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// loadIndexFromDataFile 从数据文件中构建索引
// 遍历数据文件中的所有记录，更新到内存索引
func (db *DB) loadIndexFromDataFile() error {
	// 数据库是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}
	//  查看是否发生过 merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDelete {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}

	// 暂存事务数据
	transcationRecords := make(map[uint64][]*data.TranscationRecord)
	var currentSeqNo = nonTransactionSeqNo

	// 遍历所有的文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		// 如果最近未参与 merge 的文件id更小，则说明已经从 hint file加载索引了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.oldFiles[fileId]
		}

		var offset int64 = 0
		// 处理每个文件中的数据项
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构造内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}

			// 解析 key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务提交，直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 事务完成，对应的 seq no 的数据可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transcationRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transcationRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transcationRecords[seqNo] = append(transcationRecords[seqNo], &data.TranscationRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 递增offset，下一次从新的位置开始读取
			offset += size
		}

		// 如果当前是活跃文件，更新这个文件的WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新事务序列号
	db.seqNo = currentSeqNo
	return nil
}
