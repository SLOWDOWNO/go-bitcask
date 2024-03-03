package data

import "encoding/binary"

// LogRecordType 数据文件类型
type LogRecordType = byte

// 数据文件类型枚举
const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

// crc type key_size value_size
// 4 +  1  +   5   +    5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 磁盘中数据记录的结构体
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecord 的头部信息
type logRecordHeader struct {
	crc        uint32        // crc 校验码
	recordType LogRecordType // LogRecord 类型
	keySize    uint32        // key 长度
	valueSize  uint32        // value 长度
}

// LogRecordPos 描述数据在磁盘上的位置，内存中的数据索引，
type LogRecordPos struct {
	Fid    uint32 // 文件id， 标识数据在哪个文件
	Offset int64  // 偏移量，数据在数据文件中的位置
	// Size   int32 // 标识数据在磁盘上的大小
}

// EncodeLogRecord 对 LogRecord进行编码
// 返回字节数组和长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// TODO
	return nil, 0
}

// decodeLogRecordHeader 对字节数组 进行解码
// 返回 loRecordHeader 信息
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	// TODO
	return nil, 0
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	// TODO
	return 0
}
