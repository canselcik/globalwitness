package lib

import (
	"fmt"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/wire"
	"github.com/jmoiron/sqlx/types"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

// Ping/Pong Nonce and inv trickle handled by the `PeerBase.
type BitcoinHandler struct {
	nodeInfo           *NodeInfo
	db                 *PostgresStorage
	rs                 *RedisStorage
	peerCfg            *peer.Config
	peerInstance       *peer.Peer
	started            time.Time
}

// table nodehistory
type NodeHistoryEntry struct {
	Id                      int64               `db:"id"`
	NodeId                  int64               `db:"nodeId"`
	EventType               string              `db:"eventtype"`
	Timestamp               time.Time           `db:"timestamp"`
	Data                    types.NullJSONText  `db:"data"`
}

// table nodes
type NodeInfo struct {
	Id                      int64               `db:"id"`
	ConnString              string              `db:"connstring"`
	Referrer                int64               `db:"referrer"`
	Version                 string              `db:"version"`
	Discovery               time.Time           `db:"discovery"`
	LastSeen				time.Time           `db:"lastseen"`
	Data                    types.NullJSONText  `db:"data"`
}

func (handler *BitcoinHandler) testNewAdvertisement(addr wire.NetAddress) {
	if addr.Port == 0 {
		addr.Port = 8333
	}

	connstring := fmt.Sprintf("[%s]:%d", addr.IP.String(), addr.Port)
	testCfg := peer.Config{
		UserAgentName:    "Satoshi",
		UserAgentVersion: "0.17.99",
		ChainParams:      &chaincfg.MainNetParams,
		Services:         wire.SFNodeXthin | wire.SFNodeWitness | wire.SFNodeGetUTXO |
			wire.SFNodeCF | wire.SFNodeBloom | wire.SFNodeBit5 |
			wire.SFNode2X | wire.SFNodeNetwork,
		// Trickle slowly on purpose as to not contaminate the data
		TrickleInterval:  time.Minute * 2,
	}
	testCfg.Listeners.OnVersion = func(newPeer *peer.Peer, msg *wire.MsgVersion) *wire.MsgReject {
		defer newPeer.Disconnect()

		now := time.Now()
		node := NodeInfo{
			ConnString: connstring,
			Discovery: now,
			LastSeen: now,
			Version: msg.UserAgent,
		}
		_, err := handler.db.UpdateAllNode(&node)
		if err != nil {
			log.Println("Error while adding the new found and confirmed node:", err.Error())
			return nil
		}
		return nil
	}

	newPeer, err := peer.NewOutboundPeer(handler.peerCfg, connstring)
	if err != nil {
		return
	}

	// Establish the connection to the peer address and mark it connected.
	conn, err := net.DialTimeout("tcp", handler.nodeInfo.ConnString, time.Duration(5) * time.Second)
	newPeer.AssociateConnection(conn)
	newPeer.WaitForDisconnect()
}


// OnAddr is invoked when a peer receives an addr bitcoin message.
func (handler *BitcoinHandler) onAddrHandler(p *peer.Peer, msg *wire.MsgAddr) {
	for _, addr := range msg.AddrList {
		if addr.Port == 0 {
			addr.Port = 8333
		}
		connstring := fmt.Sprintf("[%s]:%d", addr.IP.String(), addr.Port)
		instance := handler.db.GetNodeByConnString(connstring)

		// Check if we already have this node
		if instance != nil {
			continue
		}
		recommendedNode, err := handler.db.AddNode(&addr.IP, addr.Port, handler.nodeInfo.Id)
		if err != nil {
			log.Println("Error while adding unconfirmed node:", err.Error())
			continue
		}
		// Already known node
		if recommendedNode == nil {
			continue
		}

		// TODO: Add to nodehistory about this nodes discovery
		// TODO: Add this node to pending nodes table
		// TODO: Check perhaps here if this node is reachable
		//go handler.testNewAdvertisement(*addr)
		log.Println("Added new unconfirmed node:", recommendedNode.ConnString)
	}

	// Just tell the peer about a few nodes -- hacky for now since it isnt the focus
	randomNode, _ := handler.db.GetRandomNode()
	ip, port, err := net.SplitHostPort(randomNode.ConnString)
	if err != nil {
		log.Println("An error occurred while splitting host-port for sending to peer:", err.Error())
		return
	}
	iport, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		log.Println("An error occurred while splitting parsing the port from split host port for sending to peer:", err.Error())
		return
	}
	myaddr := wire.NewMsgAddr()
	_ = myaddr.AddAddresses(&wire.NetAddress {
		IP: net.ParseIP(ip),
		Port: uint16(iport),
		Services: wire.SFNodeNetwork,
		Timestamp: time.Now(),
	})
	_ = myaddr.AddAddress(p.NA())

	// TODO Have a better mechanism than disconnect after 5 mins w/o configuration
	var doneChan chan struct{}
	if time.Now().Sub(handler.started) > time.Minute * 5 {
		doneChan = make(chan struct{})
	}
	p.QueueMessage(myaddr, doneChan)
	if doneChan != nil {
		go func(doneChan chan struct{}) {
			_ = <-doneChan
			log.Println("Disconnecting from peer", handler.nodeInfo.ConnString, "after a cycle of addr exchange")
			p.Disconnect()
		}(doneChan)
	}
}

func (handler *BitcoinHandler) Run() error {
	handler.started = time.Now()

	handler.peerCfg.Listeners.OnAddr = func(p *peer.Peer, msg *wire.MsgAddr) {
		handler.onAddrHandler(p, msg)
	}
	handler.peerCfg.Listeners.OnTx = func(p *peer.Peer, msg *wire.MsgTx) {
		log.Println("MsgTx:", *msg)
	}
	handler.peerCfg.Listeners.OnBlock = func(p *peer.Peer, msg *wire.MsgBlock, buf []byte) {
		log.Println("MsgBlock: size:", len(buf), "hash:", msg.BlockHash().String(), "timestamp:", msg.Header.Timestamp.String())
	}
	handler.peerCfg.Listeners.OnVersion = func(p *peer.Peer, msg *wire.MsgVersion) *wire.MsgReject {
		handler.nodeInfo.Version = msg.UserAgent
		handler.nodeInfo.LastSeen = time.Now()
		return nil
	}
	handler.peerCfg.Listeners.OnVerAck = func(p *peer.Peer, msg *wire.MsgVerAck) {
		_, err := handler.db.UpdateAllNode(handler.nodeInfo)
		//log.Println("Handshake completed with", handler.nodeInfo.ConnString, "(VerAck)")
		if err != nil {
			log.Println("Failed to update node session time:", err.Error())
		}
	}
	handler.peerCfg.Listeners.OnReject = func(p *peer.Peer, msg *wire.MsgReject) {
		log.Println("MsgReject:", *msg)
	}
	handler.peerCfg.Listeners.OnInv = func(p *peer.Peer, msg *wire.MsgInv) {
		invSize := len(msg.InvList)
		if invSize == 0 {
			return
		}
		for _, inv := range msg.InvList {
			switch t := inv.Type; t {
			case wire.InvTypeTx:
				break
				//log.Println("->Tx", inv.Hash.String())
			case wire.InvTypeBlock:
				log.Println("->Block", inv.Hash.String(), "from", handler.nodeInfo.ConnString)

				req := wire.NewMsgGetData()
				_ = req.AddInvVect(inv)
				p.QueueMessage(req, nil)
			case wire.InvTypeError:
				log.Println("->Error", inv.Hash.String())
			case wire.InvTypeFilteredBlock:
				log.Println("->FilteredBlock", inv.Hash.String())
			case wire.InvTypeWitnessBlock:
				log.Println("->WitnessBlock", inv.Hash.String())
			case wire.InvTypeFilteredWitnessBlock:
				log.Println("->FilteredWitnessBlock", inv.Hash.String())
			case wire.InvTypeWitnessTx:
				log.Println("->WitnessTx", inv.Hash.String())
			default:
				log.Println("->Unknown inventory type", inv.Type, "hash", inv.Hash)
			}
		}
	}
	handler.peerCfg.Listeners.OnMemPool = func(p *peer.Peer, msg *wire.MsgMemPool) {
		log.Println("MsgMemPool:", msg.Command())
	}

	// Establish the connection to the peer address and mark it connected.
	conn, err := net.DialTimeout("tcp", handler.nodeInfo.ConnString, time.Duration(5) * time.Second)
	if err != nil {
		return err
	}

	p, err := peer.NewOutboundPeer(handler.peerCfg, handler.nodeInfo.ConnString)
	if err != nil {
		return err
	}

	handler.peerInstance = p
	p.AssociateConnection(conn)
	p.WaitForDisconnect()
	return nil
}

func (handler *BitcoinHandler) Async(wg *sync.WaitGroup) error {
	wg.Add(1)
	go func() {
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

func MakeBitcoinHandler(node *NodeInfo, db *PostgresStorage, rs *RedisStorage) *BitcoinHandler {
	return &BitcoinHandler{
		peerInstance:      nil,
		nodeInfo:          node,
		db:                db,
		rs:                rs,
		peerCfg:           &peer.Config{
			UserAgentName:    "Satoshi",
			UserAgentVersion: "0.17.99",
			ChainParams:      &chaincfg.MainNetParams,
			Services:         wire.SFNodeXthin | wire.SFNodeWitness | wire.SFNodeGetUTXO |
				wire.SFNodeCF | wire.SFNodeBloom | wire.SFNodeBit5 |
				wire.SFNode2X | wire.SFNodeNetwork,
			// Trickle slowly on purpose as to not contaminate the data
			TrickleInterval:  time.Minute * 2,
		},
	}
}
