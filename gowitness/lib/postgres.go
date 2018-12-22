package lib

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
	"net"
	"strings"
	"time"
)

type PostgresStorage struct {
	connString   string
	db           *sqlx.DB
}

func (storage *PostgresStorage) Connect() error {
	if storage.db != nil {
		return fmt.Errorf("this PostgresStorage is already connected")
	}
	db, err := sqlx.Open("postgres", storage.connString)
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(8)
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
	node := NodeInfo{}
	err := storage.db.Get(&node, "SELECT * FROM nodes WHERE connstring = $1", connString)
	if err != nil {
		return nil
	}
	return &node
}

func (storage *PostgresStorage) AddNode(addr *net.IP, port uint16, referrerId int64) (*NodeInfo, error) {
	now := time.Now()
	node := NodeInfo{
		Id:                     -1,
		Referrer:               &referrerId,
		ConnString:             fmt.Sprintf("[%s]:%d", addr.String(), port),
		Discovery:              &now,
		Version:                nil,
		SuccessfulSessionCount: 0,
		FailedSessionCount:     0,
		LastSession:            nil,
	}

	res := storage.db.QueryRow(`INSERT INTO nodes (connstring, referrer, discovery, lastsession, version, sessionok, sessionerr) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
		node.ConnString, node.Referrer, node.Discovery, node.LastSession, node.Version, node.SuccessfulSessionCount, node.FailedSessionCount)
	err := res.Scan(&node.Id)
	if err != nil {
		if strings.Index(err.Error(), "pq: duplicate key") == 0 {
			return nil, nil
		}
		return nil, err
	}

	return &node, nil
}

// Takes the record with the ID and updates it with all the other fields
func (storage *PostgresStorage) UpdateAllNode(node *NodeInfo) (bool, error) {
	res, err := storage.db.NamedExec("UPDATE nodes SET connstring = :connstring, referrer = :referrer, discovery = :discovery, lastsession = :lastsession, version=:version, sessionok=:sessionok, sessionerr=:sessionerr WHERE id = :id", node)
	if err != nil {
		return false, err
	}
	rowCount, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rowCount != 0, nil
}

func (storage *PostgresStorage) GetRandomNode() (*NodeInfo, error) {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return nil, connectionErr
	}

	node := NodeInfo{}
	err := storage.db.Get(&node, "SELECT * FROM nodes TABLESAMPLE BERNOULLI(10) LIMIT 1")
	if err != nil {
		return nil, err
	}
	return &node, nil
}

func (storage *PostgresStorage) GetNodes() ([]NodeInfo, error) {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return nil, connectionErr
	}

	rows, err := storage.db.Query("SELECT id, connstring, referrer, discovery, lastsession, version, sessionOk, sessionErr FROM nodes")
	if err != nil {
		return nil, err
	}

	nodes := make([]NodeInfo, 0)
	for rows.Next() {
		node := NodeInfo{}
		err = rows.Scan(&node.Id, &node.ConnString, &node.Referrer, &node.Discovery, &node.LastSession, &node.Version, &node.SuccessfulSessionCount, &node.FailedSessionCount)
		if err == nil {
			nodes = append(nodes, node)
		}
	}

	_ = rows.Close()
	return nodes, rows.Err()
}



func (storage *PostgresStorage) GetUnconfirmedNodes() ([]UnconfirmedNodeInfo, error) {
	nodes := make([]UnconfirmedNodeInfo, 0)
	err := storage.db.Select(&nodes, "SELECT * FROM unconfirmednodes")
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

func (storage *PostgresStorage) GetRandomUnconfirmedNode() (*UnconfirmedNodeInfo, error) {
	node := UnconfirmedNodeInfo{}
	err := storage.db.Get(&node, "SELECT * FROM unconfirmednodes TABLESAMPLE BERNOULLI(10) LIMIT 1")
	if err != nil {
		return nil, err
	}
	return &node, nil
}

func (storage *PostgresStorage) CreateUnconfirmedNode(connString string, referrer int64, discovery time.Time) *UnconfirmedNodeInfo {
	ret := UnconfirmedNodeInfo{
		ConnString: connString,
		Referrer: referrer,
		Discovery: &discovery,
	}
	rows, err := storage.db.NamedQuery(`INSERT INTO unconfirmednodes (connstring, referrer, discovery) VALUES (:connstring, :referrer, :discovery) RETURNING referrer`, &ret)
	if err != nil {
		if strings.Index(err.Error(), "pq: duplicate key") != 0 {
			log.Println("Failed to add unconfirmed node:", err.Error())
		}
		return nil
	}
	if !rows.Next() {
		log.Println("Failed to add unconfirmed node, query didnt return expected value")
		_ = rows.Close()
		return nil
	}
	var ref int64
	err = rows.Scan(&ref)
	_ = rows.Close()

	if err != nil {
		log.Println("Failed to add unconfirmed node:", err.Error())
		return nil
	}
	return &ret
}