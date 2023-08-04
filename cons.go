package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"os"
)

func SubcribeMessage(topic string) {
	// 订阅主题、消费
	endPoint := []string{"127.0.0.1:9876"}
	// 创建一个consumer实例
	c, err := rocketmq.NewPushConsumer(consumer.WithNameServer(endPoint),
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithGroupName("ConsumerGroupName"),
	)

	// 订阅topic
	//consumer.WithConsumeTimestamp(time.Now().Add(time.Second * (-60)).Format("20060102150405"))

	err = c.Subscribe(topic, consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for i := range msgs {
			//
			//fmt.Println("get message")
			if contains(topic, msgs[i].GetProperty("key")) {
				delete(topic, msgs[i].GetProperty("key"))
				fmt.Printf("receive new message :msgID = %s,key = %v\n", msgs[i].MsgId, msgs[i].GetProperty("key"))
			} else {
				fmt.Printf("message have been used,key = %v\n", msgs[i].GetProperty("key"))
			}

		}
		return consumer.ConsumeSuccess, nil
	})

	if err != nil {
		fmt.Printf("subscribe message error: %s\n", err.Error())
	}

	// 启动consumer
	err = c.Start()

	if err != nil {
		fmt.Printf("consumer start error: %s\n", err.Error())
		os.Exit(-1)
	}

	err = c.Shutdown()
	if err != nil {
		fmt.Printf("shutdown Consumer error: %s\n", err.Error())
	}
}
