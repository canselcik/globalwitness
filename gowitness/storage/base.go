package storage

import (
	"globalwitness/common"
	"net"
)

type PersistenceStore interface {
	Ping() error
	AddNode(addr *net.IP, port uint16, referrerId int64) (*common.NodeInfo, error)
	UpdateAllNode(node *common.NodeInfo) (bool, error)
	GetRandomNode() (*common.NodeInfo, error)
	GetNodes() ([]common.NodeInfo, error)
	Close() error
}