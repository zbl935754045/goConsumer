package main

import (
	kafka "eloizhang/goConsumer/kafkautils"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v7"
	"os/signal"
	"runtime"
	"syscall"
)

type request struct {
	RoomId  string `json:"room_id"`
	UserId  string `json:"user_id"`
	Time    float64    `json:"time"`
	Message `json:"msg"`
}

type Message struct {
	Color   string `json:"color"`
	Content string `json:"content"`
	Speed   string `json:"speed"`
}


func main() {
	// 初始化消费者
	kafka.InitConsumer("localhost:9092")

	// 监听
	go func() {
		kafka.LoopConsumer("Test", TopicCallBack)
	}()

	signal.Ignore(syscall.SIGHUP)
	runtime.Goexit()
}

func TopicCallBack(data []byte) {
	//log.Printf("kafka Test:" + string(data))
	var req request
	err := json.Unmarshal([]byte(data), &req)
	//fmt.Println(req)

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})


	err = client.ZAdd(req.RoomId, &redis.Z{
		Score:  req.Time,
		Member: data,
	}).Err()
	if err != nil {
		panic(err)
	}

	vals, err := client.ZRevRange(req.RoomId,0,-1).Result()
	if err != nil {
		panic(err)
	}
	for _, val := range vals {
		fmt.Println(val)
	}
}