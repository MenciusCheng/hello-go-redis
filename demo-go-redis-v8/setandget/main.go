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
		fmt.Println("Set key:", key, "value:", value)
		//time.Sleep(1 * time.Second)
	}
}

func getKey(client *redis.Client, key string) {
	// 不断get该key的值，如果没有值则打印未命中
	for {
		value, err := client.Get(ctx, key).Result()
		if err != nil {
			fmt.Println("No value found for key:", key, err)
			panic(err)
		} else {
			fmt.Println("Value found for key:", key, "is", value)
		}
		//time.Sleep(1 * time.Second)
	}
}

func ttlKey(client *redis.Client, key string) {
	// 不断ttl该key，并且打印返回值
	for {
		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			fmt.Println("Error getting TTL for key:", key, err)
			panic(err)
		} else {
			fmt.Println("TTL for key:", key, "is", ttl.Seconds(), "seconds")
		}
		//time.Sleep(1 * time.Second)
	}
}

func main() {
	// 创建Redis连接
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 密码为空
		DB:       0,  // 使用默认数据库
	})

	// 启动三个协程执行不同的操作
	go setKey(client, "mykey", "hello world")
	time.Sleep(1 * time.Second)
	go getKey(client, "mykey")
	go ttlKey(client, "mykey")

	// 让程序一直运行
	select {}
}
