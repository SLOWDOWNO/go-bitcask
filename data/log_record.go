package data

// LogRecordType 数据文件类型
type LogRecordType = byte

// 数据文件类型枚举
const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

// LogRecord磁盘中数据记录的结构体
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// 描述数据在磁盘上的位置，内存中的数据索引，
type LogRecordPos struct {
	Fid    uint32 // 文件id， 标识数据在哪个文件
	Offset uint64 // 偏移量，数据在数据文件中的位置
	// Size   uint32 // 标识数据在磁盘上的大小
}

// EncodeLogRecord对LogRecord进行编码
// 返回字节数组和长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, uint64) {
	// TODO
	return nil, 0
}
