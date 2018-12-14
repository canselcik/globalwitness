package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/wire"

	"github.com/go-redis/redis"
)

func getRedis() {

	// Output: key value
	// key2 does not exist
}

func main() {
	REDIS_PORT := os.Getenv("REDIS_PORT")
	REDIS_HOST := os.Getenv("REDIS_HOST")
	REDIS_PASS := os.Getenv("REDIS_PASS")
	REDIS_DB := os.Getenv("REDIS_DB")
	if len(REDIS_HOST) == 0 || len(REDIS_DB) == 0 || len(REDIS_PORT) == 0 {
		log.Fatalln("Missing env variables, exiting.")
	}

	db, err := strconv.ParseInt(REDIS_DB, 0, 32)
	if err != nil {
		db = 0
	}

	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", REDIS_HOST, REDIS_PORT),
		Password: REDIS_PASS,
		DB:       int(db),
	})

	err = client.Set("key", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := client.Get("key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val2, err := client.Get("key2").Result()
	if err == redis.Nil {
		fmt.Println("key2 does not exist")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("key2", val2)
	}

	peerCfg := &peer.Config{
		UserAgentName:    "globalwitness",
		UserAgentVersion: "0.1",
		ChainParams:      &chaincfg.MainNetParams,
		Services:         wire.SFNodeXthin | wire.SFNodeWitness | wire.SFNodeGetUTXO | wire.SFNodeCF | wire.SFNodeBloom | wire.SFNodeBit5 | wire.SFNode2X | wire.SFNodeNetwork,
		TrickleInterval:  time.Second * 10,
		Listeners: peer.MessageListeners{
			// OnAddr is invoked when a peer receives an addr bitcoin message.
			OnAddr: func(p *peer.Peer, msg *wire.MsgAddr) {
				log.Println("new peer notification with", len(msg.AddrList), "addresses")
				for _, addr := range msg.AddrList {
					log.Println("  PEER", *addr)
				}
				p.QueueMessage(wire.NewMsgPing(122312312), nil)
			},
			OnTx: func(p *peer.Peer, msg *wire.MsgTx) {
				log.Println("txn:", msg.TxHash())
			},
			OnVersion: func(p *peer.Peer, msg *wire.MsgVersion) *wire.MsgReject {
				log.Println("Peer is a", msg.UserAgent)
				return nil
			},
			OnVerAck: func(p *peer.Peer, msg *wire.MsgVerAck) {
				log.Println("Received version ack:", *msg)
			},
			OnReject: func(p *peer.Peer, msg *wire.MsgReject) {
				log.Println("REJECT MSG", msg.Reason)
			},
			OnPing: func(p *peer.Peer, msg *wire.MsgPing) {
				log.Println("PING CALLED", msg.Nonce)
				p.QueueMessage(wire.NewMsgPong(msg.Nonce), nil)
				p.QueueMessage(wire.NewMsgGetAddr(), nil)
			},
			OnPong: func(p *peer.Peer, msg *wire.MsgPong) {
				log.Println("PONG CALLED", msg.Nonce)
				myaddr := wire.NewMsgAddr()
				_ = myaddr.AddAddress(p.NA())
				p.QueueMessage(myaddr, nil)
			},
			OnMemPool: func(p *peer.Peer, msg *wire.MsgMemPool) {
				log.Println("MEMPOOL MSG", msg.Command())
			},
		},
	}
	p, err := peer.NewOutboundPeer(peerCfg, "47.133.69.42:8333")
	if err != nil {
		fmt.Printf("NewOutboundPeer: error %v\n", err)
		return
	}

	// Establish the connection to the peer address and mark it connected.
	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		fmt.Printf("net.Dial: error %v\n", err)
		return
	}
	p.AssociateConnection(conn)

	p.WaitForDisconnect()
}
