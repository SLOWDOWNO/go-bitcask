package main

import (
	"fmt"
	gobitcask "go-bitcask"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

var db *gobitcask.DB

// init 初始化 DB 实例
func init() {
	var err error
	options := gobitcask.DefaultOption
	dir, _ := os.MkdirTemp("", "go-bitcask-http")
	options.DirPath = dir
	db, err = gobitcask.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open bitcask: %v", err))
	}
}

func main() {
	r := gin.Default()
	r.POST("/bitcask/put", HandlerPut)
	r.GET("/bitcask/get", HandlerGet)
	r.DELETE("/bitcask/delete", HandlerDelete)
	r.GET("/bitcask/listkeys", HandlerListKeys)

	r.Run("localhost:8081")
}

func HandlerPut(c *gin.Context) {
	var data map[string]string
	if err := c.ShouldBind(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error:": err.Error()})
		return
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			log.Printf("failed to put value in db: %v\n", err)
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func HandlerGet(c *gin.Context) {
	key := c.Query("key")
	value, err := db.Get([]byte(key))
	if err != nil && err != gobitcask.ErrKeyNotFound {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		log.Printf("failed to get value in db: %v\n", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"value": string(value)})
}

func HandlerDelete(c *gin.Context) {
	key := c.Query("key")
	err := db.Delete([]byte(key))
	if err != nil && err != gobitcask.ErrKeyNotFound {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		log.Printf("failed to delete value in db: %v\n", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success to delete"})
}

func HandlerListKeys(c *gin.Context) {
	keys := db.ListKeys()
	var result []string
	for _, key := range keys {
		result = append(result, string(key))
	}

	c.JSON(http.StatusOK, gin.H{"keys": result})
}
