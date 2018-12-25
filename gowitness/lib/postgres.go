package lib

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
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
		Referrer:               referrerId,
		ConnString:             fmt.Sprintf("[%s]:%d", addr.String(), port),
		Discovery:              now,
		Version:                "",
		LastSeen:               now,
	}

	res := storage.db.QueryRow(`INSERT INTO nodes (connstring, referrer, discovery, lastseen, version) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		node.ConnString, node.Referrer, node.Discovery, node.LastSeen, node.Version)
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
	res, err := storage.db.NamedExec("UPDATE nodes SET connstring = :connstring, referrer = :referrer, discovery = :discovery, lastseen = :lastseen, version = :version, data = :data WHERE id = :id", node)
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