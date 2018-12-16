package handlers

import (
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/wire"
	"globalwitness/common"
	"globalwitness/storage"
	"log"
	"net"
	"sync"
	"time"
)

type BitcoinHandler struct {
	nodeInfo           *common.NodeInfo
	db                 storage.PersistenceStore
	peerCfg            *peer.Config
	incomingPingNonce  uint64
	expectedPongNonce  uint64
}

// OnAddr is invoked when a peer receives an addr bitcoin message.
func (handler *BitcoinHandler) onAddrHandler(p *peer.Peer, msg *wire.MsgAddr) {
	log.Println("new peer notification with", len(msg.AddrList), "addresses")
	for _, addr := range msg.AddrList {
		added, err := handler.db.AddNode(&addr.IP, addr.Port, handler.nodeInfo.Id)
		if err != nil {
			log.Println("  Failed to add node to storage:", err.Error())
		}
		if added {
			log.Printf("  Discovered and stored a brand new node: [%s]:%d", addr.IP, addr.Port)
		}
	}

	// Ping the host after that and expect the expected nonce
	handler.expectedPongNonce = common.GenerateNonce()
	p.QueueMessage(wire.NewMsgPing(handler.expectedPongNonce), nil)
}

func (handler *BitcoinHandler) onPingHandler(p *peer.Peer, msg *wire.MsgPing) {
	handler.incomingPingNonce = msg.Nonce

	// Reply with a pong
	p.QueueMessage(wire.NewMsgPong(msg.Nonce), nil)

	// And ask for more addr
	p.QueueMessage(wire.NewMsgGetAddr(), nil)
}

func (handler *BitcoinHandler) onPongHandler(p *peer.Peer, msg *wire.MsgPong) {
	if msg.Nonce != handler.expectedPongNonce {
		log.Println("Received invalid nonce in pong response from peer")
	}

	// We will just tell them we have no peers
	myaddr := wire.NewMsgAddr()
	_ = myaddr.AddAddress(p.NA())
	p.QueueMessage(myaddr, nil)
}


func (handler *BitcoinHandler) Run() error {
	handler.peerCfg.Listeners.OnAddr = func(p *peer.Peer, msg *wire.MsgAddr) {
		handler.onAddrHandler(p, msg)
	}
	handler.peerCfg.Listeners.OnTx = func(p *peer.Peer, msg *wire.MsgTx) {
		//log.Println("MsgTx:", *msg)
	}
	handler.peerCfg.Listeners.OnVersion = func(p *peer.Peer, msg *wire.MsgVersion) *wire.MsgReject {
		//log.Println("MsgVersion:", *msg)
		return nil
	}
	handler.peerCfg.Listeners.OnVerAck = func(p *peer.Peer, msg *wire.MsgVerAck) {
		//log.Println("MsgVerAck:", *msg)
	}
	handler.peerCfg.Listeners.OnReject = func(p *peer.Peer, msg *wire.MsgReject) {
		//log.Println("MsgReject:", *msg)
	}
	handler.peerCfg.Listeners.OnPing = func(p *peer.Peer, msg *wire.MsgPing) {
		handler.onPingHandler(p, msg)
	}
	handler.peerCfg.Listeners.OnPong = func(p *peer.Peer, msg *wire.MsgPong) {
		handler.onPongHandler(p, msg)
	}
	handler.peerCfg.Listeners.OnMemPool = func(p *peer.Peer, msg *wire.MsgMemPool) {
		//log.Println("MsgMemPool:", msg.Command())
	}

	log.Println("Connecting to", handler.nodeInfo.ConnString)

	// Establish the connection to the peer address and mark it connected.
	conn, err := net.Dial("tcp", handler.nodeInfo.ConnString)
	if err != nil {
		log.Println("Failed to connect to", handler.nodeInfo.ConnString, err.Error())
		return err
	}

	p, err := peer.NewOutboundPeer(handler.peerCfg, handler.nodeInfo.ConnString)
	if err != nil {
		log.Printf("Failed to create OutboundPeer configuration: %s\n", err.Error())
		return err
	}

	log.Println("Connected to", handler.nodeInfo.ConnString)
	p.AssociateConnection(conn)
	p.WaitForDisconnect()
	return nil
}

func (handler *BitcoinHandler) Async(wg *sync.WaitGroup) error {
	go func() {
		wg.Add(1)
		_ = handler.Run()
		wg.Done()
	}()
	return nil
}

func (handler *BitcoinHandler) Status() bool {
	return false
}

func (handler *BitcoinHandler) Stop() error {
	return nil
}

func MakeBitcoinHandler(node *common.NodeInfo, db storage.PersistenceStore) *BitcoinHandler {
	return &BitcoinHandler{
		nodeInfo:          node,
		incomingPingNonce: 0,
		expectedPongNonce: 0,
		db:                db,
		peerCfg:           &peer.Config{
			UserAgentName:    "globalwitness",
			UserAgentVersion: "0.1",
			ChainParams:      &chaincfg.MainNetParams,
			Services:         wire.SFNodeXthin | wire.SFNodeWitness | wire.SFNodeGetUTXO |
				wire.SFNodeCF | wire.SFNodeBloom | wire.SFNodeBit5 |
				wire.SFNode2X | wire.SFNodeNetwork,
			TrickleInterval:  time.Second * 10,
		},
	}
}