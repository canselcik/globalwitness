package main

import (
	"fmt"
	"globalwitness/handlers"
	"globalwitness/storage"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	PHOST, PHOSTOK := os.LookupEnv("POSTGRES_HOST")
	PPASS, PPASSOK := os.LookupEnv("POSTGRES_PASS")
	PUSER, PUSEROK := os.LookupEnv("POSTGRES_USER")
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

	nodes, _ := db.GetNodes()
	for {
		var wg sync.WaitGroup
		rand.Shuffle(len(nodes), func(i, j int) {
			nodes[i], nodes[j] = nodes[j], nodes[i]
		})
		for i := 0; i < 1000; i++ {
			handler := handlers.MakeBitcoinHandler(&nodes[i], db)
			_ = handler.Async(&wg)
		}
		wg.Wait()
	}
}
