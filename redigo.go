package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	// DefaultRedisHost Redis连接地址
	DefaultRedisHost = "127.0.0.1:6379"
	// DefaultRedisDb Redis数据库编号
	DefaultRedisDb = 0
	// DefaultRedisPassword Redis密码
	DefaultRedisPassword = ""
	// DefaultRedisMaxIdle Redis连接池闲置连接数
	DefaultRedisMaxIdle = 3
	// DefaultRedisMaxActive Redis连接池最大激活连接数, 0为不限制
	DefaultRedisMaxActive = 0
	// DefaultRedisConnectTimeout Redis连接超时时间,单位毫秒
	DefaultRedisConnectTimeout = 5 * 1000
	// DefaultRedisReadTimeout Redis读取超时时间, 单位毫秒
	DefaultRedisReadTimeout = 180 * 1000
	// DefaultRedisWriteTimeout Redis写入超时时间, 单位毫秒
	DefaultRedisWriteTimeout = 3 * 1000
)

// RedisConfig Redis配置
type RedisConfig struct {
	Host           string
	Db             uint32
	Password       string
	MaxIdle        int // 连接池最大空闲连接数
	MaxActive      int // 连接池最大激活连接数
	ConnectTimeout int // 连接超时, 单位毫秒
	ReadTimeout    int // 读取超时, 单位毫秒
	WriteTimeout   int // 写入超时, 单位毫秒
}

func NewRedisConfig() *RedisConfig {
	return &RedisConfig{
		MaxIdle:        DefaultRedisMaxIdle,
		MaxActive:      DefaultRedisMaxActive,
		ConnectTimeout: DefaultRedisConnectTimeout,
		ReadTimeout:    DefaultRedisReadTimeout,
		WriteTimeout:   DefaultRedisWriteTimeout,
	}
}

// 初始化MQ连接池
func InitRedisMQPool(cfg *RedisConfig) *redis.Pool {
	log.SetFlags(log.LstdFlags)
	return &redis.Pool{
		MaxIdle:     cfg.MaxIdle,
		MaxActive:   cfg.MaxActive,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial(
				"tcp",
				cfg.Host,
				redis.DialConnectTimeout(time.Duration(cfg.ConnectTimeout)*time.Millisecond),
				//redis.DialReadTimeout(time.Duration(cfg.ReadTimeout)*time.Millisecond),
				//redis.DialWriteTimeout(time.Duration(cfg.WriteTimeout)*time.Millisecond),
			)
			if err != nil {
				log.Println("连接redis失败,", err)
				return nil, err
			}

			if cfg.Password != "" {
				if _, err := conn.Do("AUTH", cfg.Password); err != nil {
					conn.Close()
					log.Println("redis认证失败,", err)
					return nil, err
				}
			}
			_, err = conn.Do("SELECT", cfg.Db)
			if err != nil {
				conn.Close()
				log.Println("redis选择数据库失败,", err)
				return nil, err
			}
			return conn, nil
		},
		TestOnBorrow: redisTestOnBorrow,
		Wait:         true,
	}
}

// 从池中取出连接后，判断连接是否有效
func redisTestOnBorrow(conn redis.Conn, t time.Time) error {
	_, err := conn.Do("PING")
	if err != nil {
		log.Println("从redis连接池取出的连接无效,", err)
	}
	return err
}

// 执行redis命令, 执行完成后连接自动放回连接池
func execRedisCmd(pool *redis.Pool, command string, args ...interface{}) (interface{}, error) {
	conn := pool.Get()
	defer conn.Close()
	return conn.Do(command, args...)
}

// getScanKeys REDIS.SCAN
func getScanKeys(conn redis.Conn, pattern string) ([]string, error) {
	iter := 0
	keys := []string{}
	for {
		arr, err := redis.Values(conn.Do("SCAN", iter, "MATCH", pattern, "COUNT", 10000))
		if err != nil {
			return keys, fmt.Errorf("error retrieving '%s' keys", pattern)
		}
		iter, _ = redis.Int(arr[0], nil)
		k, _ := redis.Strings(arr[1], nil)
		keys = append(keys, k...)
		if iter == 0 {
			break
		}
	}
	return keys, nil
}

// 订阅event
func DoSub(pool *redis.Pool, event string, Hander func(msg []byte) error) {
	conn := pool.Get()
	defer conn.Close()

	channels := redis.Args{event}
	//channels = channels.Add(event)

	psc := redis.PubSubConn{Conn: conn}
	psc.Subscribe(channels...)
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			log.Println("util.sub_msg:", v.Channel, v.Pattern, string(v.Data))
			Hander(v.Data)
		case redis.Subscription:
			log.Println("util.sub:", v.Channel, v.Kind, v.Count)
		case error:
			log.Println("DoSub.err:", v)
			return
		}
	}
}
