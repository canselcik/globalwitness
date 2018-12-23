package lib

import (
	"fmt"
	"github.com/go-redis/redis"
	"net"
	"time"
)

type RedisStorage struct {
	connstring   string
	db           *redis.Client
}


func (storage *RedisStorage) Connect() error {
	if storage.db != nil {
		return fmt.Errorf("this RedisStorage is already created")
	}
	storage.db = redis.NewClient(&redis.Options{
		DB: 0,
		Addr: storage.connstring,
		PoolSize: 32,
	})
	return nil
}

func (storage *RedisStorage) Ping() error {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return connectionErr
	}
	return storage.db.Ping().Err()
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

func MakeRedisStorage(connString string) *RedisStorage {
	// Unnecessary abstraction but to be fixed later
	return &RedisStorage{
		connstring: connString,
		db:         nil,
	}
}



func (storage *RedisStorage) GetNodeByConnString(connString string) *NodeInfo {
	return nil
}

func (storage *RedisStorage) AddNode(addr *net.IP, port uint16, referrerId int64) (*NodeInfo, error) {
	return nil, nil
}

// Takes the record with the ID and updates it with all the other fields
func (storage *RedisStorage) UpdateAllNode(node *NodeInfo) (bool, error) {
	return false, nil
}

func (storage *RedisStorage) GetRandomNode() (*NodeInfo, error) {
	return nil, nil
}

func (storage *RedisStorage) GetNodes() ([]NodeInfo, error) {
	return nil, nil
}



func (storage *RedisStorage) GetUnconfirmedNodes() ([]UnconfirmedNodeInfo, error) {
	return nil, nil
}

func (storage *RedisStorage) GetRandomUnconfirmedNode() (*UnconfirmedNodeInfo, error) {
	return nil, nil
}

func (storage *RedisStorage) CreateUnconfirmedNode(connString string, referrer int64, discovery time.Time) *UnconfirmedNodeInfo {
	return nil
}