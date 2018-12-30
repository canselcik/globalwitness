package lib

import (
	"fmt"
	"github.com/paulbellamy/ratecounter"
	"log"
	"sync/atomic"
	"time"
)

type Status int32
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
	FailCounter       *ratecounter.RateCounter
	AttemptCounter    *ratecounter.RateCounter
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
func (cd *Coordinator) Start() bool {
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
	go func(coord *Coordinator) {
		for {
			currentPeerCount := atomic.LoadInt64(&coord.PeerCount)
			if currentPeerCount >= coord.MaxPeers {
				log.Println("Waiting 15 seconds because we are at or above MAX_PEERS.")
				time.Sleep(time.Second * 15)
				continue
			}

			randomNode := coord.DbConn.GetRandomNode()
			if randomNode == nil {
				log.Println("Got nil for random node :(")
				continue
			}
			if coord.RedisConn.CheckActiveTag(randomNode.ConnString) {
				log.Println("Skipping", randomNode.ConnString, "because it is already a peer of our network")
				continue
			}

			log.Println("trying node", randomNode.ConnString)

			handler := MakeBitcoinHandler(randomNode, coord.DbConn, coord.RedisConn)
			coord.AttemptCounter.Incr(1)
			go handler.Run(coord)
		}
	}(cd)
	return true
}

func (cd *Coordinator) Status() Status {
	return cd.ExecutionStatus
}

func (cd *Coordinator) String() string {
	return fmt.Sprintf("Coordinator[name=%s, status=%s, peerCount=%d, maxPeers=%d, attemptRate=%s, failRate=%s]",
		cd.CoordinatorName,
		cd.ExecutionStatus.String(),
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
	AttemptCounter string
	FailCounter    string
}

func (cd *Coordinator) Summary() CoordinatorStateSnapshot {
	return CoordinatorStateSnapshot{
		cd.CoordinatorName,
		cd.ExecutionStatus.String(),
		atomic.LoadInt64(&cd.PeerCount),
		atomic.LoadInt64(&cd.MaxPeers),
		cd.AttemptCounter.String(),
		cd.FailCounter.String(),
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