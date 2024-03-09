package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	// randStr 生成一个基于当前时间的随机数序列
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	// letters 用于生成随机字节
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey 获取测试使用的 key
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("go-bitcask-key-%09d", i))
}

// RandomValue 生成随机 value 字节数组，用于测试
func RandomValue(length int) []byte {
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("go-bitcask-value-" + string(b))
}
