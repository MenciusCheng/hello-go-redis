package queue_concurrency

import "fmt"

const QueueConcurrency = "Platform:Queue:Concurrency"

const (
	DefaultBatchSize = 10
)

// 队列的key
func GetQueueKey(entity *QueueConcurrencyEntity) string {
	lastNum := ":Num:%d"
	if entity == nil {
		return QueueConcurrency + lastNum
	}
	//if entity.OneLevelName != "" && entity.TwoLevelName != "" && entity.ThreeLevelName != "" {
	//	key := fmt.Sprintf(QueueConcurrency+":OneLevel:%s:TwoLevel:%s:ThreeLevel:%s", entity.OneLevelName, entity.TwoLevelName, entity.ThreeLevelName)
	//	return key + lastNum
	//}
	//if entity.OneLevelName != "" && entity.TwoLevelName != "" {
	//	key := fmt.Sprintf(QueueConcurrency+":OneLevel:%s:TwoLevel:%s", entity.OneLevelName, entity.TwoLevelName)
	//	return key + lastNum
	//}
	//if entity.OneLevelName != "" {
	//	key := fmt.Sprintf(QueueConcurrency+":OneLevel:%s", entity.OneLevelName)
	//	return key + lastNum
	//}
	return QueueConcurrency + lastNum
}

// 并发生成的队列数
func GetQueueConcurrencyKey(queue string, num int64) string {
	return fmt.Sprintf(queue, num)
}
