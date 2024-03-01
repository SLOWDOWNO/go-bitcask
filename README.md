# go-bitcask

这个项目是在学习**KV存储**时实现的mini KV存储数据库
在阅读[《数据库系统内幕》](https://book.douban.com/subject/35078474/)这本书的时候接触到了LSM-Tree和bitcask，并阅读了相关的论文，由于前者已经有了经典的实现leveldb，而且bitcask模型实现起来更加简单，所以有了这个项目。