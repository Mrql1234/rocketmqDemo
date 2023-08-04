package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

/*
*
topic: examples/admin/topic/main.go
*/
func main() {
	// 1. 创建主题，这一步可以省略，在send的时候如果没有topic，也会进行创建。
	// 2.生产者向主题中发送消息
	initRedis("testTopic01")

	//CreateTopic("testTopic01")

	//DeleteTopic("testTopic01")

	SendSyncMessage("testTopic01", "hello world")

	SubcribeMessage("testTopic01")

	//time.Sleep(5 * time.Second)
}
func DeleteTopic(topicName string) {
	topic := topicName
	//clusterName := "DefaultCluster"
	nameSrvAddr := []string{"127.0.0.1:9876"}
	brokerAddr := "127.0.0.1:10911"

	testAdmin, err := admin.NewAdmin(
		admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)))
	if err != nil {
		fmt.Println(err.Error())
	}

	err = testAdmin.DeleteTopic(
		context.Background(),
		admin.WithTopicDelete(topic),
		admin.WithBrokerAddrDelete(brokerAddr),
		//admin.WithNameSrvAddr(nameSrvAddr),
	)
	if err != nil {
		fmt.Println("Delete topic error:", err.Error())
	} else {
		fmt.Println("Delete topic successful")
	}

}
func CreateTopic(topicName string) {
	endPoint := []string{"127.0.0.1:9876"}
	brokerAddr := "127.0.0.1:10911"
	// 创建主题
	testAdmin, err := admin.NewAdmin(
		admin.WithResolver(primitive.NewPassthroughResolver(endPoint)))
	if err != nil {
		fmt.Printf("connection error: %s\n", err.Error())
	}
	err = testAdmin.CreateTopic(
		context.Background(),
		admin.WithTopicCreate(topicName),
		admin.WithBrokerAddrCreate(brokerAddr),
	)
	if err != nil {
		fmt.Printf("createTopic error: %s\n", err.Error())
	}
}

func SendSyncMessage(topic, message string) {
	// 发送消息
	endPoint := []string{"127.0.0.1:9876"}
	// 创建一个producer实例
	p, _ := rocketmq.NewProducer(
		producer.WithNameServer(endPoint),
		producer.WithRetry(2),
		producer.WithGroupName("ProducerGroupName"),
	)
	// 启动
	err := p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		os.Exit(1)
	}

	// 发送消息
	for i := 0; i < 1000; i++ {
		message := &primitive.Message{
			Topic: topic,
			Body:  []byte(fmt.Sprintf("%s %v", message, i)),
		}
		//timeStamp := time.Now().Unix()
		// 不能用时间 并发量大了 很多重复的
		str := fmt.Sprintf("%v", time.Now().Unix())
		message.WithProperty("key", str)
		result, err := p.SendSync(context.Background(), message)

		if err != nil {
			fmt.Printf("send message error: %s\n", err.Error())
		} else {
			fmt.Printf("send message success: status = %v,msgID = %v,key = %v\n", result.Status, result.MsgID, str) //result.String()
			// 存入redis
			insert(topic, str)
		}

		//time.Sleep(1 * time.Second)
	}
	expire(topic, 1)

}
