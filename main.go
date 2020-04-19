package main

import (
	kafka "eloizhang/goConsumer/kafkautils"
	"log"
	"os/signal"
	"runtime"
	"syscall"
)

type message struct {
	roomId string
	userId string
	time   string
	msg    string
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
	log.Printf("kafka Test:" + string(data))
}