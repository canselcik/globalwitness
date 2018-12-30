package main

import (
	"encoding/json"
	"fmt"
	"github.com/Pallinder/sillyname-go"
	"globalwitness/lib"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
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

func runApiServer(coordinator *lib.Coordinator) {
	http.HandleFunc("/globalwitness/status", func (w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if coordinator == nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte("{\"error\":\"coordinator is nil\"}"))
			return
		}
		serialized, err := json.Marshal(coordinator.Summary())
		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(fmt.Sprintf("{\"error\":\"%s\"}", err.Error())))
		}
		w.WriteHeader(200)
		_, _ = w.Write(serialized)
	})

	http.HandleFunc("/globalwitness/peers", func (w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if coordinator == nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte("{\"error\":\"coordinator is nil\"}"))
			return
		}

		active, err := coordinator.RedisConn.GetFullKeys("active_*")
		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(fmt.Sprintf("{\"error\":\"%s\"}", err.Error())))
		}

		ret := struct {
			Info	lib.CoordinatorStateSnapshot
			Peers   []string
		} {
			Info: coordinator.Summary(),
			Peers: make([]string, 0),
		}

		for _, connstring := range active {
			ret.Peers = append(ret.Peers, strings.Replace(connstring, "active_", "", 1))
		}
		serialized, err := json.Marshal(ret)
		w.WriteHeader(200)
		_, _ = w.Write(serialized)
	})


	// Low priority TODO -- make this configurable by env vars
	log.Println("API Server begins listening.")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalln("Failed to bind API Server to port 8080:", err.Error())
	}
}

func main() {
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
