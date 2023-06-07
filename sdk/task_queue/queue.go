package task_queue

import (
	"errors"
	"fmt"
	"github.com/MenciusCheng/hello-go-redis/util"
	"github.com/go-redis/redis"
	"log"
	"sync"
	"time"
)

type Queue interface {
	// Publish 推送消息
	Publish(payload string) error
	// AddConsumerFunc 添加消费者函数
	AddConsumerFunc(consumerFunc ConsumerFunc)
	// StartConsuming 开启消费
	StartConsuming() error
}

type Delivery interface {
	// Payload 消息内容
	Payload() string
}

type ConsumerFunc func(Delivery)

func newRedisDelivery(payload string) Delivery {
	return &redisDelivery{
		payload: payload,
	}
}

type redisDelivery struct {
	payload string
}

func (r *redisDelivery) Payload() string {
	return r.payload
}

// TaskQueueEntity 队列参数结构体
type TaskQueueEntity struct {
	Name                   string         // 名称
	QueueKey               string         // 队列Key，根据名称计算
	MaxQueueConcurrencyNum int64          // 并发处理数
	RedisConn              *redis.Client  // Redis 连接
	ConsumerFuncs          []ConsumerFunc // 消息处理方法
	MaxQueueLen            int64          // 队列最大长度

	lock     sync.Mutex // protects the fields
	consumed bool       // 是否已开启消费
}

func (q *TaskQueueEntity) initQueue() error {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.RedisConn == nil {
		return errors.New("redisConn is nil")
	}

	if q.Name == "" {
		return errors.New("queue name is empty")
	}
	q.QueueKey = fmt.Sprintf(TaskQueueKey, q.Name)

	if q.MaxQueueConcurrencyNum <= 0 {
		q.MaxQueueConcurrencyNum = DefaultMaxQueueConcurrencyNum
	}
	if q.MaxQueueLen <= 0 {
		q.MaxQueueLen = DefaultMaxQueueLen
	}

	return nil
}

func (q *TaskQueueEntity) Publish(payload string) error {
	err := q.RedisConn.LPush(q.QueueKey, payload).Err()
	if err != nil {
		log.Println("LPush err", err)
		return err
	}
	return nil
}

func (q *TaskQueueEntity) AddConsumerFunc(consumerFunc ConsumerFunc) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if consumerFunc != nil {
		q.ConsumerFuncs = append(q.ConsumerFuncs, consumerFunc)
	}
}

func (q *TaskQueueEntity) StartConsuming() error {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.consumed {
		return errors.New("must not call StartConsuming() multiple times")
	}
	q.consumed = true

	if q.MaxQueueConcurrencyNum <= 0 {
		return errors.New("maxQueueConcurrencyNum is zero")
	}

	// consumerID 范围为 [1, MaxQueueConcurrencyNum]
	for i := int64(1); i <= q.MaxQueueConcurrencyNum; i++ {
		go func(consumerID int64) {
			q.consumeApi(consumerID)
		}(i)
	}

	// 清理消息，防止队列过长
	go func() {
		q.preventQueueTooLong()
	}()

	return nil
}

func (q *TaskQueueEntity) consumeApi(consumerID int64) {
	defer util.CheckGoPanic()

	tick := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-tick.C:
			q.consumeTask(consumerID)
		}
	}
}

func (q *TaskQueueEntity) consumeTask(consumerID int64) {
	defer util.CheckGoPanic()

	for {
		log.Println("GetMessage start", "consumerID", consumerID)
		resp, err := q.GetMessage()
		if err == redis.Nil {
			return
		} else if err != nil {
			log.Println("consumeTask GetMessages error", err)
			return
		}

		if len(resp) == 0 {
			log.Println("resp is empty")
			return
		}

		for _, consumerFunc := range q.ConsumerFuncs {
			delivery := newRedisDelivery(resp)
			consumerFunc(delivery)
		}
	}
}

func (q *TaskQueueEntity) preventQueueTooLong() {
	defer util.CheckGoPanic()

	tick := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-tick.C:
			if err := q.RedisConn.LTrim(q.QueueKey, 0, q.MaxQueueLen).Err(); err != nil {
				log.Println("preventQueueTooLong LTrim err", err)
			}
		}
	}
}

// 读取队列数据
func (q *TaskQueueEntity) GetMessage() (string, error) {
	result, err := q.RedisConn.BRPop(time.Second, q.QueueKey).Result()
	if err == redis.Nil {
		return "", err
	} else if err != nil {
		log.Println("GetMessage BRPop err", err)
		return "", err
	}
	if len(result) < 2 {
		log.Println("GetMessage result", result)
		return "", errors.New("GetMessage result failed")
	}
	return result[1], nil
}
