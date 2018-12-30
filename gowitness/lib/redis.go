package lib

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"log"
	"time"
)

type RedisStorage struct {
	connstring   string
	password     string
	db           *redis.Pool
}

func (storage *RedisStorage) GetConn() redis.Conn {
	if storage.db == nil {
		return nil
	}
	return storage.db.Get()
}

func (storage *RedisStorage) Connect() {
	storage.db = &redis.Pool{
		MaxIdle:     64,
		MaxActive:   0,
		Wait:        true,
		IdleTimeout: 240 * time.Second,
		Dial: func () (redis.Conn, error) {
		   c, err := redis.Dial("tcp", storage.connstring)
		   if err != nil {
			   return nil, err
		   }
		   if storage.password != "" {
			   if _, err := c.Do("AUTH", storage.password); err != nil {
				   c.Close()
				   return nil, err
			   }
		   }
			if _, err := c.Do("SELECT", 0); err != nil {
			   c.Close()
			   return nil, err
		   }
		   return c, nil
	    },
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func (storage *RedisStorage) Ping() error {
	conn := storage.db.Get()
	defer conn.Close()

	_, err := redis.String(conn.Do("PING"))
	if err != nil {
		return fmt.Errorf("cannot 'PING' db: %v", err)
	}
	return nil
}

func (storage *RedisStorage) Close() error {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return connectionErr
	}

	err := storage.db.Close()
	if err == nil {
		storage.db = nil
	}
	return err
}

func (storage *RedisStorage) checkConnected() error {
	if storage.db == nil {
		return fmt.Errorf("this PostgresStorage already disconnected")
	}
	return nil
}

func MakeRedisStorage(connString, password string) *RedisStorage {
	// Unnecessary abstraction but to be fixed later
	return &RedisStorage{
		connstring: connString,
		password:   password,
		db:         nil,
	}
}

func (storage *RedisStorage) randomKey() *string {
	conn := storage.db.Get()
	defer conn.Close()

	data, err := redis.String(conn.Do("RANDOMKEY"))
	if err != nil {
		log.Printf("error getting RANDOMKEY: %s\n", err.Error())
		return nil
	}
	return &data
}

func (storage *RedisStorage) get(key string) ([]byte, error) {
	conn := storage.db.Get()
	defer conn.Close()

	var data []byte
	data, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return data, fmt.Errorf("error getting key %s: %v", key, err)
	}
	return data, err
}

func (storage *RedisStorage) sadd(set, value string) error {
	conn := storage.db.Get()
	defer conn.Close()

	_, err := conn.Do("SADD", set, value)
	if err != nil {
		v := string(value)
		if len(v) > 15 {
			v = v[0:12] + "..."
		}
		return fmt.Errorf("error adding %s to %s: %s", set, value, err.Error())
	}
	return err
}

func (storage *RedisStorage) set(key string, value []byte) error {
	conn := storage.db.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, value)
	if err != nil {
		v := string(value)
		if len(v) > 15 {
			v = v[0:12] + "..."
		}
		return fmt.Errorf("error setting key %s to %s: %v", key, v, err)
	}
	return err
}

func (storage *RedisStorage) exists(key string) (bool, error) {
	conn := storage.db.Get()
	defer conn.Close()

	ok, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return ok, fmt.Errorf("error checking if key %s exists: %v", key, err)
	}
	return ok, err
}

func (storage *RedisStorage) Delete(key string) error {
	conn := storage.db.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)
	return err
}

func (storage *RedisStorage) getKeys(pattern string) ([]string, error) {
	conn := storage.db.Get()
	defer conn.Close()

	iter := 0
	keys := make([]string, 0)
	for {
		arr, err := redis.Values(conn.Do("SCAN", iter, "MATCH", pattern))
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

func (storage *RedisStorage) GetFullKeys(pattern string) ([]string, error) {
	conn := storage.db.Get()
	defer conn.Close()

	arr, err := redis.Strings(conn.Do("KEYS", pattern))
	if err != nil {
		return nil, fmt.Errorf("error retrieving KEYS %s", pattern)
	}

	return arr, nil
}

func (storage *RedisStorage) getFullSet(set string) ([]string, error) {
	conn := storage.db.Get()
	defer conn.Close()

	arr, err := redis.Strings(conn.Do("SMEMBERS", set))
	if err != nil {
		return nil, fmt.Errorf("error retrieving SMEMBERS %s", set)
	}

	return arr, nil
}

func (storage *RedisStorage) getSetIntersection(s1, s2 string) ([]string, error) {
	conn := storage.db.Get()
	defer conn.Close()

	arr, err := redis.Strings(conn.Do("SINTER", s1, s2))
	if err != nil {
		return nil, fmt.Errorf("error retrieving intersection of %s and %s sets", s1, s2)
	}

	return arr, nil
}

func (storage *RedisStorage) iterSetMembers(setname, pattern string, batchSize int, callback func(string)) error {
	conn := storage.db.Get()
	defer conn.Close()

	iter := 0
	for {
		arr, err := redis.Values(conn.Do("SSCAN", setname, iter, "MATCH", pattern, "COUNT", batchSize))
		if err != nil {
			return fmt.Errorf("error retrieving members of '%s'", setname)
		}

		iter, _ = redis.Int(arr[0], nil)
		keys, _ := redis.Strings(arr[1], nil)
		for _, key := range keys {
			callback(key)
		}
		if iter == 0 {
			break
		}
	}
	return nil
}

func (storage *RedisStorage) iter(pattern string, estimateSetSize int, callback func(string)) error {
	conn := storage.db.Get()
	defer conn.Close()

	iter := 0
	for {
		arr, err := redis.Values(conn.Do("SCAN", iter, "MATCH", pattern, "COUNT", estimateSetSize))
		if err != nil {
			return fmt.Errorf("error iterating on '%s' keys", pattern)
		}

		iter, _ = redis.Int(arr[0], nil)
		keys, _ := redis.Strings(arr[1], nil)
		for _, key := range keys {
			callback(key)
		}
		if iter == 0 {
			break
		}
	}
	return nil
}

func (storage *RedisStorage) increment(counterKey string) (int, error) {
	conn := storage.db.Get()
	defer conn.Close()

	return redis.Int(conn.Do("INCR", counterKey))
}


func (storage *RedisStorage) ReleaseLock(resource string) error {
	return storage.Delete(fmt.Sprintf("lock_%s", resource))
}

func (storage *RedisStorage) AcquireLock(resource string, expiryMillis int, leaseExtension bool) bool {
	conn := storage.GetConn()
	name := fmt.Sprintf("lock_%s", resource)
	if !leaseExtension {
		_, err := redis.Int(conn.Do("GET", name))
		if err == nil {
			return false
		}
	}
	_, err := conn.Do("PSETEX", name, expiryMillis, 1)
	return err == nil
}

func (storage *RedisStorage) RemoveActiveTag(resource string) error {
	return storage.Delete(fmt.Sprintf("active_%s", resource))
}

func (storage *RedisStorage) SetActiveTag(resource string, expirySeconds int) bool {
	conn := storage.GetConn()
	name := fmt.Sprintf("active_%s", resource)
	_, err := conn.Do("SETEX", name, expirySeconds, 1)
	return err == nil
}

func (storage *RedisStorage) CheckActiveTag(resource string) bool {
	conn := storage.GetConn()
	name := fmt.Sprintf("active_%s", resource)
	_, err := redis.Int(conn.Do("GET", name))
	return err == nil
}