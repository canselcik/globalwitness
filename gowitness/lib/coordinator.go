package lib

import (
	"fmt"
	"github.com/paulbellamy/ratecounter"
	"log"
	"sync/atomic"
	"time"
)

// Keeping enum type as uint8 because simplifies atomic access
const (
   Stopped uint32 = 0
   Paused  uint32 = 1
   Running uint32 = 2
)

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
	ExecutionStatus   uint32
	DbConn            *PostgresStorage
	RedisConn         *RedisStorage
	Guard             chan struct{}
	FailCounter       *ratecounter.RateCounter
	AttemptCounter    *ratecounter.RateCounter
	Database
	RedisConfig
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
func (cd *Coordinator) Run() bool {
	if cd.ExecutionStatus == Running {
		return false
	}

	// Connect to Postgres
	cd.DbConn = cd.initDatabase()

	// Connect to Redis
	cd.RedisConn = MakeRedisStorage(cd.RedisConfig.RedisUrl, cd.RedisConfig.Password)
	cd.RedisConn.Connect()

	cd.ExecutionStatus = Running
	for atomic.LoadUint32(&cd.ExecutionStatus) == Running {
		currentPeerCount := atomic.LoadInt64(&cd.PeerCount)
		if currentPeerCount >= cd.MaxPeers {
			time.Sleep(time.Second * 15)
			continue
		}

		randomNode := cd.DbConn.GetRandomNode()
		if randomNode == nil {
			log.Println("Got nil for random node")
			continue
		}
		if cd.RedisConn.CheckActiveTag(randomNode.ConnString) {
			log.Println("Skipping", randomNode.ConnString, "because it is already a peer of our network")
			continue
		}

		handler := MakeBitcoinHandler(randomNode, cd.DbConn, cd.RedisConn)
		go handler.Run(cd)
	}
	return true
}

func (cd *Coordinator) Status() uint32 {
	return atomic.LoadUint32(&cd.ExecutionStatus)
}

func (cd *Coordinator) String() string {
	return fmt.Sprintf("Coordinator[name=%s, status=%s, peerCount=%d, maxPeers=%d, attemptRate=%s, failRate=%s]",
		cd.CoordinatorName,
		executionStatusAsString(atomic.LoadUint32(&cd.ExecutionStatus)),
		atomic.LoadInt64(&cd.PeerCount),
		atomic.LoadInt64(&cd.MaxPeers),
		cd.AttemptCounter.String(),
		cd.FailCounter.String())
}

type CoordinatorStateSnapshot struct {
	Name           string
	Status         string
	PeerCount      int64
	MaxPeers       int64
	AttemptCounter int64
	FailCounter    int64
}

func executionStatusAsString(status uint32) string {
	switch status {
	case 0:
		return "stopped"
	case 1:
		return "paused"
	case 2:
		return "running"
	default:
		return "unknown"
	}
}

func (cd *Coordinator) Summary() CoordinatorStateSnapshot {
	return CoordinatorStateSnapshot{
		cd.CoordinatorName,
		executionStatusAsString(atomic.LoadUint32(&cd.ExecutionStatus)),
		atomic.LoadInt64(&cd.PeerCount),
		atomic.LoadInt64(&cd.MaxPeers),
		cd.AttemptCounter.Rate(),
		cd.FailCounter.Rate(),
	}
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
		FailCounter: ratecounter.NewRateCounter(time.Minute),
		AttemptCounter: ratecounter.NewRateCounter(time.Minute),
		Guard: nil,
	}
}