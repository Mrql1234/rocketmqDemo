package main

import (
	"fmt"
	"github.com/go-redis/redis"
	"time"
)

func initRedis(topic string) {
	// 创建Redis客户端
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // Redis密码
		DB:       0,  // Redis数据库编号
	})

	// 测试连接
	pong, err := client.Ping().Result()
	fmt.Println(pong, err)

}

func expire(topic string, num int) {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // Redis密码
		DB:       0,  // Redis数据库编号
	})
	//设置过期时间
	err := client.Expire(topic, time.Minute*10).Err()
	if err != nil {
		fmt.Println("set expire err")
		panic(err)
	}
}
func insert(topic, str string) {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // Redis密码
		DB:       0,  // Redis数据库编号
	})

	err := client.SAdd(topic, str).Err()
	if err != nil {
		fmt.Println("insert redis err")
		panic(err)
	}
}
func contains(topic, str string) bool {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // Redis密码
		DB:       0,  // Redis数据库编号
	})
	exists, err := client.SIsMember(topic, str).Result()
	if err != nil {
		panic(err)
	}
	// fmt.Println("a exists in set:", exists)
	return exists
}

func delete(topic, str string) {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "", // Redis密码
		DB:       0,  // Redis数据库编号
	})
	err := client.SRem(topic, str).Err()
	if err != nil {
		fmt.Println("delete err:", err)
	}
}
