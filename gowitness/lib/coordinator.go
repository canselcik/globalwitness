package lib

import (
	"fmt"
	"github.com/paulbellamy/ratecounter"
	"log"
	"sync/atomic"
	"time"
)

// Keeping enum type as uint32 because simplifies atomic access
const (
   Stopped uint32 = 0
   Paused  uint32 = 1
   Running uint32 = 2
)

type DatabaseConfig struct {
	Address       string
	Port          uint16
	Username      string
	Password      string
	Name          string
	MaxOpen       int
	MaxIdle       int
}

type RedisConfig struct {
	RedisUrl      string
	Password      string
	MaxOpen       int
	MaxIdle       int
}

type Coordinator struct {
	CoordinatorName   			 string
	Peers             			 []NodeInfo
	MaxPeers 	      			 int64
	PeerCount         		 	 int64
	ExecutionStatus   			 uint32
	DbConn            		     *PostgresStorage
	RedisConn         			 *RedisStorage
	Guard             			 chan struct{}
	FailCounter                  *ratecounter.RateCounter
	SuccessCounter               *ratecounter.RateCounter
	VoluntaryDisconnectCounter   *ratecounter.RateCounter
	AttemptCounter               *ratecounter.RateCounter
	SkippedDueToInNetworkCounter *ratecounter.RateCounter
	DatabaseConfig
	RedisConfig
}

func (cd *Coordinator) initDatabase() *PostgresStorage {
	db := MakePostgresStorage(fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cd.DatabaseConfig.Address, 5432, cd.DatabaseConfig.Username, cd.DatabaseConfig.Password, cd.DatabaseConfig.Name))

	err := db.Connect(cd.DatabaseConfig.MaxOpen, cd.DatabaseConfig.MaxIdle)
	if err != nil {
		log.Fatalf(err.Error())
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf(err.Error())
	}

	return db
}

// Returns true if and only if a change in ExecutionStatus occurred
func (cd *Coordinator) Run() bool {
	if cd.ExecutionStatus == Running {
		return false
	}

	log.Println("Coordinator started.")

	// Connect to Postgres
	cd.DbConn = cd.initDatabase()
	log.Println("Database connection established.")

	// Connect to Redis
	cd.RedisConn = MakeRedisStorage(cd.RedisConfig.RedisUrl, cd.RedisConfig.Password)
	cd.RedisConn.Connect()
	log.Println("Redis connection established.")

	nextNodes := cd.DbConn.GetRandomNodes(0.1)

	cd.ExecutionStatus = Running

	for atomic.LoadUint32(&cd.ExecutionStatus) == Running {
		currentPeerCount := atomic.LoadInt64(&cd.PeerCount)
		if currentPeerCount >= cd.MaxPeers {
			time.Sleep(time.Second * 15)
			continue
		}

		if nextNodes == nil || len(nextNodes) == 0 {
			nextNodes = cd.DbConn.GetRandomNodes(0.1)
		}

		if len(nextNodes) == 0 {
			continue
		}

		var randomNode *NodeInfo
		randomNode, nextNodes = &nextNodes[0], nextNodes[1:]
		if cd.RedisConn.CheckActiveTag(nil, randomNode.ConnString) {
			cd.SkippedDueToInNetworkCounter.Incr(1)
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

type CoordinatorStateSnapshot struct {
	Name                         string
	Status                       string
	PeerCount                    int64
	MaxPeers                     int64
	AttemptCounter               int64
	FailCounter                  int64
	VoluntaryDisconnectCounter   int64
	SuccessCounter               int64
	SkippedDueToInNetworkCounter int64
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
		cd.VoluntaryDisconnectCounter.Rate(),
		cd.SuccessCounter.Rate(),
		cd.SkippedDueToInNetworkCounter.Rate(),
	}
}

// Returns true if and only if a change in ExecutionStatus occurred
func (cd *Coordinator) Pause() bool {
	if atomic.LoadUint32(&cd.ExecutionStatus) == Paused {
		return false
	}

	atomic.StoreUint32(&cd.ExecutionStatus, Paused)
	return true
}

// Returns true if and only if a change in ExecutionStatus occurred
func (cd *Coordinator) Stop() bool {
	if atomic.LoadUint32(&cd.ExecutionStatus) == Stopped {
		return false
	}

	atomic.StoreUint32(&cd.ExecutionStatus, Stopped)
	return true
}

func MakeCoordinator(name string, maxPeers int64, database DatabaseConfig, redisConfig RedisConfig) *Coordinator {
	return &Coordinator{
		ExecutionStatus: Stopped,
		CoordinatorName: name,
		MaxPeers: maxPeers,
		Peers: make([]NodeInfo, 0),
		DatabaseConfig: database,
		RedisConfig: redisConfig,
		PeerCount: 0,
		DbConn: nil,
		RedisConn: nil,
		FailCounter: ratecounter.NewRateCounter(time.Minute),
		AttemptCounter: ratecounter.NewRateCounter(time.Minute),
		SuccessCounter: ratecounter.NewRateCounter(time.Minute),
		VoluntaryDisconnectCounter: ratecounter.NewRateCounter(time.Minute),
		SkippedDueToInNetworkCounter: ratecounter.NewRateCounter(time.Minute),
		Guard: nil,
	}
}