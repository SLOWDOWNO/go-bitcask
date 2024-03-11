package gobitcask

import (
	"go-bitcask/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge 清理无效数据，生成 Hint File
func (db *DB) Merge() error {
	// 如果数据库为空，直接返回
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	// 如果 Merge 正在进行，直接返回
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 将当前活跃文件转换为旧的数据文件
	db.oldFiles[db.activeFile.FileId] = db.activeFile
	// 打开新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return nil
	}
	// 记录最近没有参与 merge 的文件 id
	nonMergeFileIId := db.activeFile.FileId

	// 取出需要 merge 的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.oldFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	// 从小到大排序待 merge 的file，依次merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	// 如果目录存在，说明发生过Merge，将其删除
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// 新建 merge path
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// 打开一个新的临时 bitcask 实例
	mergeOption := db.options
	mergeOption.DirPath = mergePath
	mergeOption.syncWrite = false
	mergeDB, err := Open(mergeOption)
	if err != nil {
		return err
	}

	// 打开 hint file 存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 遍历处理每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析拿到实际的 key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			// 与内存索引位置进行比较，如果有效则重写
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				// 清除事务标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// 将当前位置索引写入 Hint File
				if err = hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}

			}
			// 递增 offset
			offset += size
		}

	}
	//  sync 保证数据持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 写标识 merge 完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileIId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)

	return filepath.Join(dir, base+mergeDirName)
}

// loadMergeFiles 加载 merge 数据目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// merge 目录不存在直接返回
	if _, err := os.Stat(mergePath); os.IsExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找merge 完成的文件，判断merge是否处理完了
	var mergeFinished bool
	var mergeFileNames []string

	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// 没有merge完成则直接返回
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return nil
	}

	// 删除旧的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDatafleName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err != nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动到数据目录中
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

// loadIndexFromHintFile 从 hint file 中加载索引
func (db *DB) loadIndexFromHintFile() error {
	// 查看 hint file是否存在
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// 打开 hint file
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	// 读取 hint file中的索引
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 解码拿到实际位置的索引
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}
