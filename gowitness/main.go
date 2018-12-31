package main

import (
	"github.com/Pallinder/sillyname-go"
	"globalwitness/lib"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func initConfigs() (int64, lib.DatabaseConfig, lib.RedisConfig, lib.APIServerConfig) {
	API_PORT, APORTOK := os.LookupEnv("API_PORT")
	API_BINDING, ABINDINGOK := os.LookupEnv("API_BINDING")
	if !APORTOK {
		log.Println("API_PORT env var is not set. Defaulting to 8080")
		API_PORT = "8080"
	}
	if !ABINDINGOK {
		log.Println("API_BINDING env var is not set. Defaulting to 0.0.0.0")
		API_BINDING = "0.0.0.0"
	}

	REDIS_MAXOPEN, RMAXOPENOK := os.LookupEnv("REDIS_MAXOPEN")
	REDIS_MAXIDLE, RMAXIDLEOK := os.LookupEnv("REDIS_MAXIDLE")
	if !RMAXOPENOK {
		log.Println("REDIS_MAXOPEN env var is not set. Defaulting to 16.")
		REDIS_MAXOPEN = "16"
	}
	if !RMAXIDLEOK {
		log.Println("REDIS_MAXIDLE env var is not set. Defaulting to 8.")
		REDIS_MAXIDLE = "8"
	}

	REDIS_PASS, RPASSOK := os.LookupEnv("REDIS_PASS")
	REDIS_URL, RURLOK := os.LookupEnv("REDIS_URL")
	if !RURLOK || !RPASSOK {
		log.Fatalf("Make sure REDIS_URL and REDIS_PASS is set.")
	}

	POSTGRES_MAXOPEN, PMAXOPENOK := os.LookupEnv("POSTGRES_MAXOPEN")
	POSTGRES_MAXIDLE, PMAXIDLEOK := os.LookupEnv("POSTGRES_MAXIDLE")
	if !PMAXOPENOK {
		log.Println("POSTGRES_MAXOPEN env var is not set. Defaulting to 16.")
		POSTGRES_MAXOPEN = "16"
	}
	if !PMAXIDLEOK {
		log.Println("POSTGRES_MAXIDLE env var is not set. Defaulting to 8.")
		POSTGRES_MAXIDLE = "8"
	}

	PHOST, PHOSTOK := os.LookupEnv("POSTGRES_HOST")
	PPASS, PPASSOK := os.LookupEnv("POSTGRES_PASS")
	PUSER, PUSEROK := os.LookupEnv("POSTGRES_USER")
	MAXPEERS := os.Getenv("MAX_PEERS")
	if MAXPEERS == "" {
		log.Println("MAX_PEERS env var is not set. Defaulting to 16.")
		MAXPEERS = "16"
	}

	PDB, PDBOK := os.LookupEnv("POSTGRES_DB")
	if !PHOSTOK || !PPASSOK || !PUSEROK || !PDBOK {
		log.Fatalf("Make sure to all the required environment variables.")
	}

	maxPeers, err := strconv.ParseInt(MAXPEERS, 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse MAX_PEERS env var into an int.")
	}

	redisMaxOpen, err := strconv.Atoi(REDIS_MAXOPEN)
	if err != nil {
		log.Fatalf("Failed to parse REDIS_MAXOPEN env var into an int.")
	}
	redisMaxIdle, err := strconv.Atoi(REDIS_MAXIDLE)
	if err != nil {
		log.Fatalf("Failed to parse REDIS_MAXIDLE env var into an int.")
	}

	postgresMaxOpen, err := strconv.Atoi(POSTGRES_MAXOPEN)
	if err != nil {
		log.Fatalf("Failed to parse POSTGRES_MAXOPEN env var into an int.")
	}
	postgresMaxIdle, err := strconv.Atoi(POSTGRES_MAXIDLE)
	if err != nil {
		log.Fatalf("Failed to parse POSTGRES_MAXIDLE env var into an int.")
	}

	apiPort, err := strconv.ParseUint(API_PORT, 10, 16)
	if err != nil {
		log.Fatalf("Failed to parse API_PORT env var into a uint16.")
	}

	dbconfig := lib.DatabaseConfig{
		Address:  PHOST,
		Port:     5432,
		Name:     PDB,
		Username: PUSER,
		Password: PPASS,
		MaxIdle:  postgresMaxIdle,
		MaxOpen:  postgresMaxOpen,
	}
	redisconfig := lib.RedisConfig{
		RedisUrl: REDIS_URL,
		Password: REDIS_PASS,
		MaxIdle:  redisMaxIdle,
		MaxOpen:  redisMaxOpen,
	}
	apiconfig := lib.APIServerConfig{
		Port:        uint16(apiPort),
		BindAddress: API_BINDING,
	}
	return maxPeers, dbconfig, redisconfig, apiconfig
}

func main() {
	rand.Seed(time.Now().UnixNano())
	instance_name := sillyname.GenerateStupidName()

	log.Println("#################################")
	log.Println("#  GlobalWitness Discovery")
	log.Println("#  ( instance:", instance_name, ")")
	log.Println("#################################")

	maxPeers, dbConfig, redisConfig := initConfigs()

	// We ensure we have exactly MAXPEERS of these running at a time
	cd := lib.MakeCoordinator(instance_name, maxPeers, dbConfig, redisConfig)

	// Start HTTP server for debugging and inspection
	go runApiServer(cd)
	cd.Run()
}
