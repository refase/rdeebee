package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/mux"
)

type Seq struct {
	Sequence uint64 `json:"Sequence"`
}

type redisConn struct {
	pool *redis.Pool
}

func newRedisConn() redisConn {
	pool := newPool()
	return redisConn{pool}
}

func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:   80,
		MaxActive: 1200,
		Dial: func() (redis.Conn, error) {
			// c, err := redis.Dial("tcp", "redis:6379")
			c, err := redis.Dial("tcp", "localhost:6379")
			if err != nil {
				log.Panic(err)
			}
			return c, nil
		},
	}
}

func getConnection(redis_pool redis.Pool) redis.Conn {
	conn := redis_pool.Get()

	if _, err := conn.Do("AUTH", "p@ssw0rd"); err != nil {
		log.Panic(err)
	}
	return conn
}

func (r *redisConn) flush() {
	conn := getConnection(*r.pool)
	_, err := conn.Do("flushall")
	if err != nil {
		log.Panic(err)
	}
}

func (r *redisConn) cas(svc string) uint64 {
	conn := getConnection(*r.pool)
	val, err := redis.Uint64(conn.Do("incr", svc))
	if err != nil {
		log.Panic(err)
	}
	return val
}

func main() {
	redis_conn := newRedisConn()

	redis_conn.flush()

	router := mux.NewRouter()
	router.HandleFunc("/", redis_conn.handleConn)

	log.Fatal(http.ListenAndServe(":8080", router))
}

func (r *redisConn) handleConn(w http.ResponseWriter, _ *http.Request) {
	s := Seq{Sequence: r.cas("rdeebee")}
	json.NewEncoder(w).Encode(s)
}
