package lib

import (
	"fmt"
	"github.com/gocraft/dbr"
	_ "github.com/lib/pq"
	"log"
	"net"
	"strings"
	"time"
)

type PostgresStorage struct {
	connString   string
	db           *dbr.Connection
}

func (storage *PostgresStorage) Connect(maxOpen, maxIdle int) error {
	if storage.db != nil {
		return fmt.Errorf("this PostgresStorage is already connected")
	}
	db, err := dbr.Open("postgres", storage.connString, nil)
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	if err != nil {
		return err
	}
	storage.db = db
	return nil
}

func (storage *PostgresStorage) Ping() error {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return connectionErr
	}
	return storage.db.Ping()
}

func (storage *PostgresStorage) Close() error {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return connectionErr
	}

	err := storage.db.Close()
	if err == nil {
		storage.db = nil
	}
	return err
}

func (storage *PostgresStorage) checkConnected() error {
	if storage.db == nil {
		return fmt.Errorf("this PostgresStorage already disconnected")
	}
	return nil
}

func MakePostgresStorage(connString string) *PostgresStorage {
	// Unnecessary abstraction but to be fixed later
	return &PostgresStorage{
		connString: connString,
		db:         nil,
	}
}



func (storage *PostgresStorage) GetNodeByConnString(connString string) *NodeInfo {
	session := storage.db.NewSession(nil)
	node := NodeInfo{}
	err := session.Select("*").
					From("nodes").
					Where("connstring = ?", connString).
					Limit(1).
					LoadOne(&node)
	if err != nil {
		return nil
	}
	return &node
}

func (storage *PostgresStorage) AddNode(addr *net.IP, port uint16, referrerId int64) (*NodeInfo, bool) {
	now := time.Now()
	node := NodeInfo{
		Id:                     -1,
		Referrer:               referrerId,
		ConnString:             fmt.Sprintf("[%s]:%d", addr.String(), port),
		Discovery:              now,
		Version:                "",
		LastSeen:               now,
	}

	sess := storage.db.NewSession(nil)
	tx, err := sess.Begin()
	if err != nil {
		log.Printf("Error while getting session in AddNode(...): %s\n", err.Error())
		return nil, false
	}
	defer tx.RollbackUnlessCommitted()

	err = tx.InsertInto("nodes").
				Columns("connstring", "referrer", "discovery", "lastseen", "version").
				Record(&node).
				Returning("id").
				Load(&node.Id)
	if err != nil {
		if strings.Index(err.Error(), "pq: duplicate key") == 0 {
			return &node, false
		}
		log.Println("Error while executing the query in AddNode(...):", err.Error())
		return nil, false
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Error while executing committing the query in AddNode(...):", err.Error())
		return nil, false
	}
	return &node, true
}


func (storage *PostgresStorage) AddNodeHistory(node *NodeInfo, eventType string,
	timestamp time.Time, data dbr.NullString) *NodeHistoryEntry {

	entry := NodeHistoryEntry{
		Id: -1,
		Data: data,
		EventType: eventType,
		Timestamp: timestamp,
		NodeId: node.Id,
	}

	sess := storage.db.NewSession(nil)
	tx, err := sess.Begin()
	if err != nil {
		log.Printf("Error while getting session in AddNodeHistory(...): %s\n", err.Error())
		return nil
	}
	defer tx.RollbackUnlessCommitted()

	err = tx.InsertInto("nodehistory").Columns("nodeid", "eventtype", "timestamp", "data").Record(&entry).Returning("id").Load(&entry.Id)
	if err != nil {
		log.Println("Error while executing the query in AddNodeHistory(...):", err.Error())
		return nil
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Error while executing committing the query in AddNodeHistory(...):", err.Error())
		return nil
	}
	return &entry
}

func (storage *PostgresStorage) GetNodeHistoryByNodeId(nodeId int64, limit uint64) []NodeHistoryEntry {
	sess := storage.db.NewSession(nil)
	entries := make([]NodeHistoryEntry, 0)

	_, err := sess.Select("*").
		From("nodehistory").
		Where("nodeid = ?", nodeId).
		OrderDesc("timestamp").
		Limit(limit).
		Load(&entries)
	if err != nil {
		log.Println("Error while executing the query in GetNodeHistoryByNodeId(...):", err.Error())
		return nil
	}

	return entries
}

// Takes the record with the ID and updates it with all the other fields
func (storage *PostgresStorage) UpdateAllNode(node *NodeInfo) bool {
	session := storage.db.NewSession(nil)
	_, err := session.Update("nodes").
				Set("connstring", node.ConnString).
				Set("referrer", node.Referrer).
				Set("discovery", node.Discovery).
				Set("lastseen", node.LastSeen).
				Set("version", node.Version).
				Where("id = ?", node.Id).Exec()
	if err != nil {
		log.Println("Error while executing committing the query in UpdateAllNode(...):", err.Error())
		return false
	}
	return true
}

func (storage *PostgresStorage) GetRandomNode() *NodeInfo {
	session := storage.db.NewSession(nil)
	node := NodeInfo{}
	err := session.SelectBySql("SELECT * FROM nodes TABLESAMPLE BERNOULLI(1) LIMIT 1").LoadOne(&node)

	if err != nil {
		log.Printf("Error while executing SELECT in GetRandomNode(): %s\n", err.Error())
		return nil
	}
	return &node
}

func (storage *PostgresStorage) GetNodes() ([]NodeInfo, error) {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return nil, connectionErr
	}

	rows, err := storage.db.Query("SELECT id, connstring, referrer, discovery, lastseen, version FROM nodes")
	if err != nil {
		return nil, err
	}

	nodes := make([]NodeInfo, 0)
	for rows.Next() {
		node := NodeInfo{}
		err = rows.Scan(&node.Id, &node.ConnString, &node.Referrer, &node.Discovery, &node.LastSeen, &node.Version)
		if err == nil {
			nodes = append(nodes, node)
		}
	}

	_ = rows.Close()
	return nodes, rows.Err()
}