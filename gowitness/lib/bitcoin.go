package lib

import (
	"fmt"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/peer"
	"github.com/btcsuite/btcd/wire"
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
	peerCfg            *peer.Config
	peerInstance       *peer.Peer
	started            time.Time
}

// unconfirmednodes table
type UnconfirmedNodeInfo struct {
	ConnString string        `db:"connstring"`
	Referrer   int64         `db:"referrer"`
	Discovery  *time.Time    `db:"discovery"`
}

// nodes table
type NodeInfo struct {
	Id                      int64         `db:"id"`
	ConnString              string        `db:"connstring"`
	Referrer                *int64        `db:"referrer"`
	Discovery               *time.Time    `db:"discovery"`
	LastSession             *time.Time    `db:"lastsession"`
	Version                 *string       `db:"version"`
	SuccessfulSessionCount  uint64        `db:"sessionok"`
	FailedSessionCount 		uint64        `db:"sessionerr"`
}

func (handler *BitcoinHandler) testNewAdvertisement(addr wire.NetAddress) {
	if addr.Port == 0 {
		addr.Port = 8333
	}

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
		node, err := handler.db.AddNode(&addr.IP, addr.Port, handler.nodeInfo.Id)
		if err != nil {
			log.Println("Error while adding the new found and confirmed node:", err.Error())
			return nil
		}
		node.Version = &msg.UserAgent
		added, err := handler.db.UpdateAllNode(node)
		if err != nil {
			log.Println(err)
		}
		if added {
			log.Println("NEW CONFIRMED NODE:", node.ConnString, node.Version)
		}
		return nil
	}

	newPeer, err := peer.NewOutboundPeer(handler.peerCfg, fmt.Sprintf("[%s]:%d", addr.IP.String(), addr.Port))
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
		confirmedInstance := handler.db.GetNodeByConnString(connstring)
		if confirmedInstance == nil {
			unconfirmedInstance := handler.db.CreateUnconfirmedNode(connstring, handler.nodeInfo.Id, time.Now())
			if unconfirmedInstance != nil {
				log.Println("Added new unconfirmed node:", unconfirmedInstance.ConnString)
			}
		}
		//go handler.testNewAdvertisement(*addr)
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
		now := time.Now()
		handler.nodeInfo.Version = &msg.UserAgent
		handler.nodeInfo.LastSession = &now
		handler.nodeInfo.SuccessfulSessionCount++
		return nil
	}
	handler.peerCfg.Listeners.OnVerAck = func(p *peer.Peer, msg *wire.MsgVerAck) {
		updated, err := handler.db.UpdateAllNode(handler.nodeInfo)
		//log.Println("Handshake completed with", handler.nodeInfo.ConnString, "(VerAck)")
		if err != nil {
			log.Println("Failed to update node session time:", err.Error())
		}
		if !updated {
			log.Println("Failed to update node session time despite no errors")
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

func MakeBitcoinHandler(node *NodeInfo, db *PostgresStorage) *BitcoinHandler {
	return &BitcoinHandler{
		peerInstance:      nil,
		nodeInfo:          node,
		db:                db,
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
