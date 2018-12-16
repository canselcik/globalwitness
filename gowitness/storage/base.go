package storage

import (
	"globalwitness/common"
	"net"
)

type PersistenceStore interface {
	Connect() error
	Ping() error
	AddNode(addr *net.IP, port uint16, referrerId uint64) (bool, error)
	UpdateAllNode(node *common.NodeInfo) (bool, error)
	GetRandomNode() (*common.NodeInfo, error)
	GetNodes() ([]common.NodeInfo, error)
	Close() error
}