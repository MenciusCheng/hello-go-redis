package task_queue

import (
	"github.com/go-redis/redis"
)

type OptionFunc func(*TaskQueueEntity)

func NewTaskQueueEntity(options ...OptionFunc) (Queue, error) {
	c := &TaskQueueEntity{}
	for _, f := range options {
		f(c)
	}

	if err := c.initQueue(); err != nil {
		return nil, err
	}
	return c, nil
}

func ConfigName(name string) OptionFunc {
	return func(c *TaskQueueEntity) {
		c.Name = name
	}
}

func ConfigMaxQueueConcurrencyNum(num int64) OptionFunc {
	return func(c *TaskQueueEntity) {
		c.MaxQueueConcurrencyNum = num
	}
}

func ConfigRedisConn(redisConn *redis.Client) OptionFunc {
	return func(c *TaskQueueEntity) {
		c.RedisConn = redisConn
	}
}

func ConfigConsumerFunc(f ConsumerFunc) OptionFunc {
	return func(c *TaskQueueEntity) {
		if f != nil {
			c.ConsumerFuncs = append(c.ConsumerFuncs, f)
		}
	}
}

func ConfigMaxQueueLen(num int64) OptionFunc {
	return func(c *TaskQueueEntity) {
		c.MaxQueueLen = num
	}
}
