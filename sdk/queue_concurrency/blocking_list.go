package queue_concurrency

import "github.com/go-redis/redis"

// 阻塞队列
type blockingListEntity struct {
	Name     string        // 名称
	edisConn *redis.Client // Redis 连接
	QueueKey string        // 队列Key
	handlers []Handler     // 消息处理方法
}

func (b *blockingListEntity) Push() error {
	return nil
}
