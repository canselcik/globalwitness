package main

import (
	"globalwitness/lib"
	"log"
	"os"
	"strconv"
	"time"
	//"github.com/go-redis/redis"
)


func main() {
	//rand.Seed(time.Now().UnixNano())
	//pool := newPool()

	//client := redis.NewClient(&redis.Options{
	//	Addr:     "localhost:6379",
	//	Password: "", // no password set
	//	DB:       0,  // use default DB
	//})
	//client.PoolStats()
	//pong, err := client.Ping().Result()

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

	cd := lib.MakeCoordinator("primaryCoordinator", maxPeers, lib.Database{
		Address:  PHOST,
		Port:     5432,
		Name:     PDB,
		Username: PUSER,
		Password: PPASS,
	})

	cd.Start()
	for cd.Status() == lib.Running {
		log.Println("Checking on the execution:", cd.String())
		time.Sleep(time.Second * 30)
	}
}
