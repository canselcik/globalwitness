package lib

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

type Status int
const (
	Stopped Status = iota
	Running
	Paused
)




func (s Status) String() string {
	return [...]string{"Stopped", "Running", "Paused"}[s]
}

type Database struct {
	Address       string
	Port          uint16
	Username      string
	Password      string
	Name          string
}

type RedisConfig struct {
	RedisUrl      string
	Password      string
}

type Coordinator struct {
	CoordinatorName   string
	Peers             []NodeInfo
	MaxPeers 	      int64
	PeerCount         int64
	ExecutionStatus   Status
	Database
	RedisConfig
	DbConn            *PostgresStorage
	RedisConn         *RedisStorage
	Guard             chan struct{}
}

func (cd *Coordinator) initDatabase() *PostgresStorage {
	db := MakePostgresStorage(fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cd.Database.Address, 5432, cd.Database.Username, cd.Database.Password, cd.Database.Name))

	err := db.Connect()
	if err != nil {
		log.Fatalf(err.Error())
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf(err.Error())
	}

	return db
}

func (cd *Coordinator) Wait(summaryInterval time.Duration) {
	for cd.Status() == Running {
		log.Println("Checking on the execution:", cd.String())
		time.Sleep(summaryInterval)
	}
}

// Returns true if and only if a change in ExecutionStatus occurred
func (cd *Coordinator) Start(impl func(cd *Coordinator)) bool {
	if cd.ExecutionStatus == Running {
		return false
	}

	cd.RedisConn = MakeRedisStorage(cd.RedisConfig.RedisUrl, cd.RedisConfig.Password)
	err := cd.RedisConn.Connect()
	if err != nil {
		log.Fatalf("Unable to connect to Redis: %s\n", err.Error())
	}

	cd.DbConn = cd.initDatabase()

	cd.ExecutionStatus = Running
	cd.Guard = make(chan struct{}, cd.MaxPeers)

	go func(cd *Coordinator) {
		for cd.ExecutionStatus == Running {
			cd.Guard <- struct{}{}
			go func(cd *Coordinator) {
				impl(cd)
				<-cd.Guard
			}(cd)
		}
	}(cd)
	return true
}

func (cd *Coordinator) Status() Status {
	return cd.ExecutionStatus
}

func (cd *Coordinator) String() string {
	return fmt.Sprintf("Coordinator[name=%s, status=%s, peerCount=%d, maxPeers=%d]",
		cd.CoordinatorName,
		cd.ExecutionStatus.String(),
		atomic.LoadInt64(&cd.PeerCount),
		atomic.LoadInt64(&cd.MaxPeers))
}

// Returns true if and only if a change in ExecutionStatus occurred
func (cd *Coordinator) Pause() bool {
	if cd.ExecutionStatus == Paused {
		return false
	}

	return true
}

// Returns true if and only if a change in ExecutionStatus occurred
func (cd *Coordinator) Stop() bool {
	if cd.ExecutionStatus == Stopped {
		return false
	}

	return true
}

func MakeCoordinator(name string, maxPeers int64, database Database, redisConfig RedisConfig) *Coordinator {
	return &Coordinator{
		ExecutionStatus: Stopped,
		CoordinatorName: name,
		MaxPeers: maxPeers,
		Peers: make([]NodeInfo, 0),
		Database: database,
		RedisConfig: redisConfig,
		PeerCount: 0,
		DbConn: nil,
		RedisConn: nil,
		Guard: nil,
	}
}