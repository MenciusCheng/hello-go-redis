package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

func setKey(client *redis.Client, key string, value string) {
	// 不断设置key的值，并限制有效期为900秒
	for {
		err := client.SetEX(ctx, key, value, 900*time.Second).Err()
		if err != nil {
			fmt.Println("Error setting value for key:", key, err)
		}
		fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "Set key:", key, "value:", value)
		//time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	// 创建Redis连接
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 密码为空
		DB:       0,  // 使用默认数据库
	})

	go setKey(client, "mykey", "hello world")

	// 让程序一直运行
	select {}
}
