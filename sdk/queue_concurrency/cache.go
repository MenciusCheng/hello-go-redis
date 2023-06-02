package queue_concurrency

import (
	"errors"
	"github.com/go-redis/redis"
	"math/rand"
	"time"
)

type Message struct {
	Data        string  // 消息数据
	Score       float64 // 分数，值越小优先级越高，为0时默认为当前时间戳
	PartitionID int64   // 分区ID，根据队列数量取模，为0时随机指定队列
}

// 推送队列数据
func (a *QueueConcurrencyEntity) PushMessage(msg Message) error {
	var queueID int64 // queueID 范围为 [1, MaxQueueConcurrencyNum]
	if msg.PartitionID > 0 {
		queueID = msg.PartitionID % a.MaxQueueConcurrencyNum
		if queueID == 0 {
			queueID = a.MaxQueueConcurrencyNum
		}
	} else {
		// 随机队列
		rand.Seed(time.Now().UnixNano())
		queueID = rand.Int63n(a.MaxQueueConcurrencyNum) + 1
	}
	if !a.checkQueueID(queueID) {
		//log.Error("PushMessage checkQueueID error", zap.Int64("queueID", queueID))
		return errors.New("queueID invalid")
	}

	score := msg.Score
	if score == 0 {
		score = float64(time.Now().Unix())
	}

	key := GetQueueConcurrencyKey(a.QueueKey, queueID)
	v := redis.Z{
		Score:  score,
		Member: msg.Data,
	}
	err := a.RedisConn.ZAdd(key, v).Err()
	if err != nil {
		//log.Error("PushMessage Failed:", zap.Error(err))
		return err
	}
	return nil
}

// 读取队列数据
func (a *QueueConcurrencyEntity) GetMessages(queueID int64) ([]string, error) {
	key := GetQueueConcurrencyKey(a.QueueKey, queueID)
	result, err := a.RedisConn.ZRange(key, 0, a.BatchSize-1).Result()
	if err != nil {
		//log.Error("GetMessages Failed:", zap.Error(err))
		return nil, err
	}
	return result, nil
}

// 删除队列数据
func (a *QueueConcurrencyEntity) DelMessages(queueID int64, value ...string) error {
	key := GetQueueConcurrencyKey(a.QueueKey, queueID)
	err := a.RedisConn.ZRem(key, value).Err()
	if err != nil {
		//log.Error("DelMessages Failed:", zap.Error(err))
		return err
	}
	return nil
}
