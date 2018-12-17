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

// Ping/Pong Nonce and inv trickle handled by the `PeerBase.
type BitcoinHandler struct {
	nodeInfo           *common.NodeInfo
	db                 storage.PersistenceStore
	peerCfg            *peer.Config
	peerInstance       *peer.Peer
}

// OnAddr is invoked when a peer receives an addr bitcoin message.
func (handler *BitcoinHandler) onAddrHandler(p *peer.Peer, msg *wire.MsgAddr) {
	newNodes := 0
	for _, addr := range msg.AddrList {
		added, err := handler.db.AddNode(&addr.IP, addr.Port, handler.nodeInfo.Id)
		if err != nil && added {
			newNodes++
		}
	}
	if newNodes > 0 {
		log.Println("Added", newNodes, "unique nodes to our database out of", len(msg.AddrList))
	}

	// Just tell the peer about a few nodes
	randomNode, _ := handler.db.GetRandomNode()
	ip, port := common.ParseConnString(randomNode.ConnString)
	myaddr := wire.NewMsgAddr()
	_ = myaddr.AddAddresses(&wire.NetAddress {
		IP: net.ParseIP(ip),
		Port: port,
		Services: wire.SFNodeNetwork,
		Timestamp: time.Now(),
	})
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
		now := time.Now()
		handler.nodeInfo.LastSession = &now
		return nil
	}
	handler.peerCfg.Listeners.OnVerAck = func(p *peer.Peer, msg *wire.MsgVerAck) {
		updated, err := handler.db.UpdateAllNode(handler.nodeInfo)
		if err != nil {
			log.Println("Failed to update node session time:", err.Error())
		}
		if !updated {
			log.Println("Failed to update node session time despite no errors")
		}
	}
	handler.peerCfg.Listeners.OnReject = func(p *peer.Peer, msg *wire.MsgReject) {
		//log.Println("MsgReject:", *msg)
	}
	handler.peerCfg.Listeners.OnInv = func(p *peer.Peer, msg *wire.MsgInv) {
		invSize := len(msg.InvList)
		if invSize == 0 {
			return
		}
		log.Println("Received", invSize, "items from", handler.nodeInfo.ConnString)
		for _, inv := range msg.InvList {
			switch t := inv.Type; t {
			case wire.InvTypeTx:
				log.Println("->Tx", inv.Hash.String())
			case wire.InvTypeBlock:
				log.Println("->Block", inv.Hash.String())
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

	//log.Println("Connecting to", handler.nodeInfo.ConnString)

	// Establish the connection to the peer address and mark it connected.
	conn, err := net.DialTimeout("tcp", handler.nodeInfo.ConnString, time.Duration(10) * time.Second)
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
	handler.peerInstance = p

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
			TrickleInterval:  time.Second * 10,
		},
	}
}