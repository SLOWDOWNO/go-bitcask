package data

// 描述数据在磁盘上的位置，内存中的数据索引，
type LogRecordPos struct {
	Fid    uint32 // 文件id， 标识数据在哪个文件
	Offset uint64 // 偏移量，数据在数据文件中的位置
}
