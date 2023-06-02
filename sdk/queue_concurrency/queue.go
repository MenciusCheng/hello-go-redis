package queue_concurrency

import (
	"errors"
	"github.com/go-redis/redis"
	"time"
)

type Queue interface {
}

// 队列参数结构体
type QueueConcurrencyEntity struct {
	Name                   string        // 名称
	MaxQueueConcurrencyNum int64         // 队列数
	RedisConn              *redis.Client // Redis 连接
	QueueKey               string        // 队列Key，根据名称计算
	handlers               []Handler     // 消息处理方法
	BatchSize              int64         // 一次取出处理的任务数，默认为10
}

// 初始化维度的队列名，按照配置生成队列数，对应按配置生成消费协程
func (a *QueueConcurrencyEntity) initQueueConcurrency() {
	if a.QueueKey == "" {
		a.QueueKey = GetQueueKey(a)
	}
	if a.BatchSize == 0 {
		a.BatchSize = DefaultBatchSize
	}
}

// 初始化调用队列数据
func (a *QueueConcurrencyEntity) InitQueue() error {
	if a.MaxQueueConcurrencyNum <= 0 {
		return errors.New("MaxQueueConcurrencyNum is zero")
	}
	// queueID 范围为 [1, MaxQueueConcurrencyNum]
	for i := int64(1); i <= a.MaxQueueConcurrencyNum; i++ {
		go func(queueID int64) {
			a.consumeApi(queueID)
		}(i)
	}
	return nil
}

func (a *QueueConcurrencyEntity) AddHandler(f Handler) {
	if f != nil {
		a.handlers = append(a.handlers, f)
	}
}

func (a *QueueConcurrencyEntity) consumeApi(queueID int64) {
	//common.CheckGoPanic()
	tick := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-tick.C:
			a.consumeTask(queueID)
		}
	}
}

func (a *QueueConcurrencyEntity) consumeTask(queueID int64) {
	//queueKey := GetQueueConcurrencyKey(a.QueueKey, queueID)
	//log.Debug("consumeTask start", zap.String("queueKey", queueKey))

	// 从队列读出数据
	if !a.checkQueueID(queueID) {
		//log.Error("consumeTask checkQueueID error", zap.Int64("queueID", queueID), zap.String("queueKey", queueKey))
		return
	}
	resp, err := a.GetMessages(queueID)
	if err != nil {
		//log.Error("consumeTask GetMessages error", zap.Error(err), zap.String("queueKey", queueKey))
		return
	}
	if len(resp) <= 0 {
		//log.Debug("consumeTask end empty", zap.String("queueKey", queueKey))
		return
	}

	for _, data := range resp {
		param := HandlerParam{
			QueueID: queueID,
			Data:    data,
		}
		for _, handler := range a.handlers {
			handler(&param)
		}

		// 处理完一个就删除一个
		err := a.DelMessages(queueID, data)
		if err != nil {
			//log.Error("consumeTask DelMessages error", zap.Error(err), zap.String("queueKey", queueKey))
		}
	}

	//log.Debug("consumeTask end finish", zap.String("queueKey", queueKey))
}

func (a *QueueConcurrencyEntity) checkQueueID(queueID int64) bool {
	if queueID <= 0 || queueID > a.MaxQueueConcurrencyNum {
		return false
	}
	return true
}

type HandlerParam struct {
	QueueID int64
	Data    string
}

type Handler func(*HandlerParam)
