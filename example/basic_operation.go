package main

import (
	"fmt"
	bitcask "go-bitcask"
)

func main() {
	opts := bitcask.DefaultOption
	opts.DirPath = "/tmp/go-bitcask"
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}

	fmt.Println("val : ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
