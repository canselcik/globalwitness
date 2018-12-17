package main

import (
	"fmt"
	"globalwitness/common"
	"globalwitness/handlers"
	"globalwitness/storage"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

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

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		PHOST, 5432, PUSER, PPASS, PDB)
	db := storage.MakePostgresStorage(psqlInfo)
	err := db.Connect()

	if err != nil {
		log.Fatalf(err.Error())
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf(err.Error())
	}

	maxPeers, err := strconv.ParseInt(MAXPEERS, 10, 32)
	if err != nil {
		panic(err)
	}

	log.Println("MaxPeers set to", maxPeers)
	guard := make(chan struct{}, maxPeers)
	for {
		nodes, _ := db.GetNodes()
		rand.Shuffle(len(nodes), func(i, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})
		for _, node := range nodes {
			// Block if at maxPeers
			guard <- struct{}{}
			go func(node common.NodeInfo) {
				handler := handlers.MakeBitcoinHandler(&node, db)
				_ = handler.Run()
				//term := "nil"
				//if err != nil {
				//	term = err.Error()
				//}
				//log.Println("BitcoinHandler for", node.ConnString, "ended with:", term)
				<-guard
			}(node)
		}
	}
}
