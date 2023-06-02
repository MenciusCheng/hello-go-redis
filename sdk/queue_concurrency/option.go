package queue_concurrency

import "github.com/go-redis/redis"

type OptionFunc func(*QueueConcurrencyEntity)

func NewQueueConcurrencyEntity(options ...OptionFunc) *QueueConcurrencyEntity {
	c := &QueueConcurrencyEntity{}

	for _, f := range options {
		f(c)
	}

	c.initQueueConcurrency()
	return c
}

func ConfigLevelNames(names ...string) OptionFunc {
	return func(c *QueueConcurrencyEntity) {
		//for i := 0; i < len(names) && i < 3; i++ {
		//	switch i {
		//	case 0:
		//		c.OneLevelName = names[0]
		//	case 1:
		//		c.TwoLevelName = names[1]
		//	case 2:
		//		c.ThreeLevelName = names[2]
		//	}
		//}
	}
}

func ConfigMaxQueueConcurrencyNum(num int64) OptionFunc {
	return func(c *QueueConcurrencyEntity) {
		c.MaxQueueConcurrencyNum = num
	}
}

func ConfigRedisConn(redisConn *redis.Client) OptionFunc {
	return func(c *QueueConcurrencyEntity) {
		c.RedisConn = redisConn
	}
}

func ConfigHandler(f Handler) OptionFunc {
	return func(c *QueueConcurrencyEntity) {
		if f != nil {
			c.handlers = append(c.handlers, f)
		}
	}
}

func ConfigBatchSize(batchSize int64) OptionFunc {
	return func(c *QueueConcurrencyEntity) {
		c.BatchSize = batchSize
	}
}
