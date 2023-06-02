package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"
)

var ctx = context.Background()

func ttlKey(client *redis.Client, key string) {
	// 不断ttl该key，并且打印返回值
	for {
		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			fmt.Println("Error getting TTL for key:", key, err)
			panic(err)
		} else {
			fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "TTL for key:", key, "is", ttl.Seconds(), "seconds")
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

	go ttlKey(client, "mykey")

	// 让程序一直运行
	select {}
}
