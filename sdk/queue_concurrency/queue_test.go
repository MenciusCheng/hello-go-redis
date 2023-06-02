package queue_concurrency

import (
	"fmt"
	"log"
	"reflect"
	"sync"
	"testing"
	"time"
)

func InitAut() {
}

// 测试运行消息队列
func TestQueueConcurrencyEntity_RunQueue(t *testing.T) {
	InitAut()

	wg := &sync.WaitGroup{}
	lock := &sync.RWMutex{}
	got := make(map[string]bool)
	myHandler := func(param *HandlerParam) {
		defer func() {
			wg.Done()
		}()
		//log.Info("处理任务", zap.Int64("queueID", param.QueueID), zap.String("data", param.Data))
		lock.Lock()
		got[param.Data] = true
		lock.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
	total := 400
	wg.Add(total)

	queueEntity := NewQueueConcurrencyEntity(
		ConfigLevelNames("TestQueue", "A", "B"),
		ConfigMaxQueueConcurrencyNum(3),
		//ConfigRedisConn(common.GetRedisClient(common.ToAreaType(rpc.AreaEnv))),
		ConfigHandler(myHandler),
		ConfigBatchSize(100),
	)

	// 初始化调用队列数据
	queueEntity.InitQueue()

	want := make(map[string]bool)
	go func() {
		// 推送消息
		for i := 0; i < total; i++ {
			data := fmt.Sprintf("{\"id\":%d}", i)
			_ = queueEntity.PushMessage(Message{
				Data: data,
			})
			want[data] = true
		}
		log.Println("推送消息完成")
	}()

	wg.Wait()
	time.Sleep(1 * time.Second)

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got = %v, want %v", got, want)
		return
	}
	log.Println("finish", "got len", len(got), "want len", len(want))
}

// 指定消息分区和优先级
func TestQueueConcurrencyEntity_PartitionAndScoreQueue(t *testing.T) {
	InitAut()

	wg := &sync.WaitGroup{}
	got := make([]string, 0)
	myHandler := func(param *HandlerParam) {
		defer func() {
			wg.Done()
		}()
		log.Println("处理任务", "queueID", param.QueueID, "data", param.Data)
		got = append(got, param.Data)
	}

	queueEntity := NewQueueConcurrencyEntity(
		ConfigLevelNames("TestA"),
		ConfigMaxQueueConcurrencyNum(3),
		//ConfigRedisConn(common.GetRedisClient(common.ToAreaType(rpc.AreaEnv))),
		ConfigHandler(myHandler),
		ConfigBatchSize(5),
	)

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		_ = queueEntity.PushMessage(Message{
			Data:        fmt.Sprintf("value%d", i),
			Score:       float64(100 - i),
			PartitionID: 1,
		})
	}
	want := make([]string, 0)
	for i := 10; i >= 1; i-- {
		want = append(want, fmt.Sprintf("value%d", i))
	}

	// 初始化调用队列数据
	queueEntity.InitQueue()
	wg.Wait()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got = %v, want %v", got, want)
		return
	}
}

// 指定消息分区
func TestQueueConcurrencyEntity_PartitionQueue(t *testing.T) {
	InitAut()

	wg := &sync.WaitGroup{}
	got := make(map[int64][]string)
	myHandler := func(param *HandlerParam) {
		defer func() {
			wg.Done()
		}()
		log.Println("处理任务", "queueID", param.QueueID, "data", param.Data)
		got[param.QueueID] = append(got[param.QueueID], param.Data)
	}

	concurrencyNum := int64(3)
	queueEntity := NewQueueConcurrencyEntity(
		ConfigLevelNames("TestA"),
		ConfigMaxQueueConcurrencyNum(concurrencyNum),
		//ConfigRedisConn(common.GetRedisClient(common.ToAreaType(rpc.AreaEnv))),
		ConfigHandler(myHandler),
		ConfigBatchSize(5),
	)

	for i := 1; i <= 30; i++ {
		wg.Add(1)
		_ = queueEntity.PushMessage(Message{
			Data:        fmt.Sprintf("value%d", i),
			Score:       float64(i),
			PartitionID: int64(i),
		})
	}
	want := make(map[int64][]string)
	for i := int64(1); i <= 30; i++ {
		qID := i % concurrencyNum
		if qID == 0 {
			qID = concurrencyNum
		}
		want[qID] = append(want[qID], fmt.Sprintf("value%d", i))
	}

	// 初始化调用队列数据
	queueEntity.InitQueue()
	wg.Wait()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got = %v, want %v", got, want)
		return
	}
	time.Sleep(1 * time.Second)
}
