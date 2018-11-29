package main

import (
	"github.com/gomodule/redigo/redis"
	"log"
)

func Migrate_Values(src, dst string, mKey []string) {
	cfg1 := NewRedisConfig()
	cfg1.Host = src
	src_redis := InitRedisMQPool(cfg1)
	conn1 := src_redis.Get()
	defer conn1.Close()

	cfg2 := NewRedisConfig()
	cfg2.Host = dst
	dst_redis := InitRedisMQPool(cfg2)
	conn2 := dst_redis.Get()
	defer conn2.Close()

	for _, key := range mKey {
		key_type, err := redis.String(conn1.Do("TYPE", key))
		if err != nil {
			log.Println("conn1.Do[TYPE] error,", err)
			continue
		}
		//log.Println("key_type:", key_type)
		//string, list, set, zset 和 hash
		switch key_type {
		case "hash":
			map_values, _ := redis.StringMap(conn1.Do("HGETALL", key))
			for field_name, val := range map_values {
				conn2.Do("HSET", key, field_name, val)
			}
		case "string":
			reply_val, _ := conn1.Do("GET", key)
			conn2.Do("SET", key, reply_val)
		case "set":
			reply_val, _ := redis.Values(conn1.Do("SMEMBERS", key))
			conn2.Do("SADD", append([]interface{}{key}, reply_val...)...)
		}
	}
}

func main() {
	Migrate_Values("172.18.3.227:6379", "172.18.3.188:6379", []string{"g:goldGame:commonConfig", "g:goldGame:levelConfig", "g:goldGame:levelIncId"})
}
