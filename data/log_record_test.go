package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogRecord_EncodeLogRecord(t *testing.T) {
	// 正常情况--------------------------------------------------------
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("go-bitcask"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	// t.Log(res1)
	// t.Log(n1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	// value 为空的情况-------------------------------------------------
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))
	// t.Log(res2)
	// t.Log(n2)

	// Type 为 Deleted 的情况------------------------------------------
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("go-bitcask"),
		Type:  LogRecordDelete,
	}
	res3, n3 := EncodeLogRecord(rec3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
	t.Log(res3)
	t.Log(n3)

}

func TestLogRecord_DecodeLogRecordHeader(t *testing.T) {
	// 正常情况--------------------------------------------------------
	headerBuf1 := []byte{133, 18, 89, 36, 0, 8, 20} // 测试 header 取前7个字节就可以了
	h1, size1 := decodeLogRecordHeader(headerBuf1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(609817221), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)

	// value 为空的情况-------------------------------------------------
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := decodeLogRecordHeader(headerBuf2)
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	// Type 为 Deleted 的情况------------------------------------------
	headerBuf3 := []byte{198, 217, 255, 163, 1, 8, 20}
	h3, size3 := decodeLogRecordHeader(headerBuf3)
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(2751453638), h3.crc)
	assert.Equal(t, LogRecordDelete, h3.recordType)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(10), h3.valueSize)
}

func TestLogRecord_GetLogRecordCRC(t *testing.T) {
	// 正常情况--------------------------------------------------------
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("go-bitcask"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{133, 18, 89, 36, 0, 8, 20}
	crc1 := getLogRecordCRC(rec1, headerBuf1[crc32.Size:])
	// t.Log(crc1)
	assert.Equal(t, uint32(609817221), crc1)

	// value 为空的情况------------------------------------------------
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(rec2, headerBuf2[crc32.Size:])
	// t.Log(crc2)
	assert.Equal(t, uint32(240712713), crc2)

	// Type 为 Deleted 的情况-------------------------------------------
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("go-bitcask"),
		Type:  LogRecordDelete,
	}
	headerBuf3 := []byte{198, 217, 255, 163, 1, 8, 20}
	crc3 := getLogRecordCRC(rec3, headerBuf3[crc32.Size:])
	// t.Log(crc3)
	assert.Equal(t, uint32(2751453638), crc3)
}
