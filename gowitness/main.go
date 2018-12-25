package main

import (
	"globalwitness/lib"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

func initConfigs() (int64, lib.Database, lib.RedisConfig) {
	rand.Seed(time.Now().UnixNano())
	REDIS_PASS, RPASSOK := os.LookupEnv("REDIS_PASS")
	REDIS_URL, RURLOK := os.LookupEnv("REDIS_URL")
	if !RURLOK || !RPASSOK {
		log.Fatalf("Make sure REDIS_URL and REDIS_PASS is set.")
	}

	PHOST, PHOSTOK := os.LookupEnv("POSTGRES_HOST")
	PPASS, PPASSOK := os.LookupEnv("POSTGRES_PASS")
	PUSER, PUSEROK := os.LookupEnv("POSTGRES_USER")
	MAXPEERS := os.Getenv("MAX_PEERS")
	if MAXPEERS == "" {
		MAXPEERS = "16"
	}

	PDB, PDBOK := os.LookupEnv("POSTGRES_DB")
	if !PHOSTOK || !PPASSOK || !PUSEROK || !PDBOK {
		log.Fatalf("Make sure to all the required environment variables.")
	}

	maxPeers, err := strconv.ParseInt(MAXPEERS, 10, 64)
	if err != nil {
		panic(err)
	}

	dbconfig := lib.Database{
		Address:  PHOST,
		Port:     5432,
		Name:     PDB,
		Username: PUSER,
		Password: PPASS,
	}
	redisconfig := lib.RedisConfig{
		RedisUrl: REDIS_URL,
		Password: REDIS_PASS,
	}
	return maxPeers, dbconfig, redisconfig
}



func main() {
	maxPeers, dbConfig, redisConfig := initConfigs()
	// We ensure we have exactly MAXPEERS of these running at a time
	cd := lib.MakeCoordinator("primaryCoordinator", maxPeers, dbConfig, redisConfig)
	cd.Start(func(cd *lib.Coordinator) {
		resolvedNode, err := cd.DbConn.GetRandomNode()
		if resolvedNode == nil {
			log.Fatalln("Failed to get a random node from storage:", err.Error())
			return
		}

		log.Printf("Random Node from storage: %v\n", resolvedNode.ConnString)

		handler := lib.MakeBitcoinHandler(resolvedNode, cd.DbConn, cd.RedisConn)
		atomic.AddInt64(&cd.PeerCount, 1)
		err = handler.Run()
		if err != nil {
			// TODO: Write to the nodehistory here about this node that it was a failure
			log.Println("Failed connect to", resolvedNode.ConnString, "due to:", err.Error())
		}
		atomic.AddInt64(&cd.PeerCount, -1)
	})

	cd.Wait(time.Second * 30)
}
