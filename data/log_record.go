package data

import (
	"encoding/binary"
	"hash/crc32"
)

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

// LogRecord 磁盘文件中数据记录的结构体
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

// EncodeLogRecord 对 LogRecord 进行编码，返回编码后的数据和对应长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化 header 部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	// 从第五个字节存储 Type
	header[4] = logRecord.Type
	var index = 5
	// 5 字节后存储 key 和 value 的长度信息
	// 使用变长类型
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	// 将 header 部分拷贝到 encBytes
	copy(encBytes[:index], header[:index])
	// 将 key 和 value 数据拷贝到字节数组
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 对整个LogRecord 进行 crc 校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// decodeLogRecordHeader 解码 logRecoredHeader 字节数组，
// 返回 loRecordHeader 和对应长度
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	// 取出 key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n
	// 取出 value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// getLogRecordCRC 返回 LogRecord 中的 crc 值
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
