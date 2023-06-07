package task_queue

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func InitClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 密码为空
		DB:       0,  // 使用默认数据库
	})
	return client
}

func TestTaskQueueEntity_Run(t *testing.T) {
	client := InitClient()

	wg := &sync.WaitGroup{}
	lock := &sync.RWMutex{}
	got := make(map[string]bool)
	myHandler := func(param Delivery) {
		defer func() {
			wg.Done()
		}()
		log.Println("处理任务", "data", param.Payload())
		lock.Lock()
		got[param.Payload()] = true
		lock.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	total := 400
	wg.Add(total)

	queueEntity, err := NewTaskQueueEntity(
		ConfigName("TestQueueA"),
		ConfigMaxQueueConcurrencyNum(10),
		ConfigRedisConn(client),
		ConfigConsumerFunc(myHandler),
	)
	if err != nil {
		t.Errorf("NewTaskQueueEntity err: %v", err)
		return
	}

	// 初始化调用队列数据
	err = queueEntity.StartConsuming()
	if err != nil {
		t.Errorf("StartConsuming err: %v", err)
		return
	}

	want := make(map[string]bool)
	go func() {
		// 推送消息
		for i := 0; i < total; i++ {
			data := fmt.Sprintf("{\"id\":%d}", i)
			_ = queueEntity.Publish(data)
			want[data] = true
			time.Sleep(10 * time.Millisecond)
		}
		log.Println("推送消息完成")
	}()

	wg.Wait()
	time.Sleep(2 * time.Second)

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got = %v, want %v", got, want)
		return
	}
	log.Println("finish", "got len", len(got), "want len", len(want))
}

func TestTaskQueueEntity_QueueB_Consumer(t *testing.T) {
	client := InitClient()

	count := int64(0)
	myHandler := func(param Delivery) {
		log.Println("处理任务", "data", param.Payload())
		atomic.AddInt64(&count, 1)
		time.Sleep(10 * time.Millisecond)
	}

	queueEntity, err := NewTaskQueueEntity(
		ConfigName("TestQueueB"),
		ConfigMaxQueueConcurrencyNum(10),
		ConfigRedisConn(client),
		ConfigConsumerFunc(myHandler),
	)
	if err != nil {
		t.Errorf("NewTaskQueueEntity err: %v", err)
		return
	}

	// 初始化调用队列数据
	err = queueEntity.StartConsuming()
	if err != nil {
		t.Errorf("StartConsuming err: %v", err)
		return
	}

	time.Sleep(60 * time.Second)

	log.Println("finish", "count", count)
}

func TestTaskQueueEntity_QueueB_Producer(t *testing.T) {
	client := InitClient()

	queueEntity, err := NewTaskQueueEntity(
		ConfigName("TestQueueB"),
		ConfigRedisConn(client),
	)
	if err != nil {
		t.Errorf("NewTaskQueueEntity err: %v", err)
		return
	}

	total := 100
	// 推送消息
	for i := 0; i < total; i++ {
		data := fmt.Sprintf("{\"id\":%d}", i)
		_ = queueEntity.Publish(data)
		time.Sleep(10 * time.Millisecond)
	}
	log.Println("推送消息完成", "total", total)
}

func TestTaskQueueEntity_QueueB_Panic(t *testing.T) {
	client := InitClient()

	count := int64(0)
	myHandler := func(param Delivery) {
		log.Println("处理任务", "data", param.Payload())
		atomic.AddInt64(&count, 1)
		if strings.Contains(param.Payload(), "99") {
			panic("99panic")
		}
		time.Sleep(10 * time.Millisecond)
	}

	queueEntity, err := NewTaskQueueEntity(
		ConfigName("TestQueueB"),
		ConfigMaxQueueConcurrencyNum(1),
		ConfigRedisConn(client),
		ConfigConsumerFunc(myHandler),
	)
	if err != nil {
		t.Errorf("NewTaskQueueEntity err: %v", err)
		return
	}

	// 初始化调用队列数据
	err = queueEntity.StartConsuming()
	if err != nil {
		t.Errorf("StartConsuming err: %v", err)
		return
	}

	total := 200
	// 推送消息
	for i := 0; i < total; i++ {
		data := fmt.Sprintf("{\"id\":%d}", i)
		_ = queueEntity.Publish(data)
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(10 * time.Second)

	log.Println("finish", "count", count)
}
