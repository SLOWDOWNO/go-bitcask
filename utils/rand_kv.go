package utils

import "fmt"

func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("go-bitcask-key-%09d", i))
}
