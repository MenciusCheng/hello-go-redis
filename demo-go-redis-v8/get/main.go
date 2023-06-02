package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

var ctx = context.Background()

func getKey(client *redis.Client, key string) {
	// 不断get该key的值，如果没有值则打印未命中
	for {
		value, err := client.Get(ctx, key).Result()
		if err != nil {
			fmt.Println("No value found for key:", key, err)
			panic(err)
		} else {
			fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "Value found for key:", key, "is", value)
		}
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

	go getKey(client, "mykey")

	// 让程序一直运行
	select {}
}
