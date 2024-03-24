package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// copyDir 拷贝数据目录
func CopyDir(src, dest string, exclude []string) error {
	// 目标文件夹不存在则直接创建
	if _, err := os.Stat(dest); err != nil {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		// 截取文件名
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}

		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		// 是目录的情况
		if info.IsDir() {
			os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
