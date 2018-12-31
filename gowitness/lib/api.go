package lib

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
)

type APIServerConfig struct {
	Port 		  uint16
	BindAddress   string
}

func RunAPIServer(coordinator *Coordinator, config APIServerConfig) {
	http.HandleFunc("/globalwitness/status", func (w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if coordinator == nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte("{\"result\":\"error\",\"error\":\"coordinator is nil\"}"))
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

	http.HandleFunc("/globalwitness/flush", func (w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if coordinator == nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte("{\"result\":\"error\",\"error\":\"coordinator is nil\"}"))
			return
		}

		coordinator.RedisConn.FlushDB()

		w.WriteHeader(200)
		_, _ = w.Write([]byte("{\"result\":\"success\"}"))
	})

	http.HandleFunc("/globalwitness/peers", func (w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if coordinator == nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte("{\"result\":\"error\",\"error\":\"coordinator is nil\"}"))
			return
		}

		active, err := coordinator.RedisConn.GetFullKeys("active_*")
		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte("{\"result\":\"error\",\"error\":\"%s\"}"))
		}

		ret := struct {
			Info	CoordinatorStateSnapshot
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

	binding := fmt.Sprintf("[%s]:%d", config.BindAddress, config.Port)

	log.Println("Binding APIServer to", binding)
	if err := http.ListenAndServe(binding, nil); err != nil {
		log.Fatalf("Failed to bind API Server to %s: %s\n", binding, err.Error())
	}
}
