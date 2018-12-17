package storage

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"globalwitness/common"
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

func (storage *PostgresStorage) AddNode(addr *net.IP, port uint16, referrerId int64) (*common.NodeInfo, error) {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return nil, connectionErr
	}

	now := time.Now()
	node := common.NodeInfo{
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
func (storage *PostgresStorage) UpdateAllNode(node *common.NodeInfo) (bool, error) {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return false, connectionErr
	}
	sqlStatement := "UPDATE nodes SET connstring = $1, referrer = $2, discovery = $3, lastsession = $4, version=$5, sessionOk=$6, sessionErr=$7 WHERE id = $8"
	res, err := storage.db.Exec(sqlStatement, node.ConnString, node.Referrer, node.Discovery, node.LastSession, node.Version, node.SuccessfulSessionCount, node.FailedSessionCount, node.Id)
	if err != nil {
		return false, err
	}
	rowCount, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rowCount != 0, nil
}

func (storage *PostgresStorage) GetRandomNode() (*common.NodeInfo, error) {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return nil, connectionErr
	}

	node := common.NodeInfo{}
	err := storage.db.Get(&node, "SELECT * FROM nodes TABLESAMPLE BERNOULLI(10) LIMIT 1")
	if err != nil {
		return nil, err
	}
	return &node, nil
}

func (storage *PostgresStorage) GetLeastRecentlyUsedNodes(size uint) ([]common.NodeInfo, error) {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return nil, connectionErr
	}

	nodes := []common.NodeInfo{}
	err := storage.db.Select(&nodes, "SELECT id, connstring, referrer, discovery, lastsession, version, sessionOk, sessionErr FROM nodes ORDER BY lastsession LIMIT $1", size)
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

func (storage *PostgresStorage) GetNodes() ([]common.NodeInfo, error) {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return nil, connectionErr
	}

	rows, err := storage.db.Query("SELECT id, connstring, referrer, discovery, lastsession, version, sessionOk, sessionErr FROM nodes")
	if err != nil {
		return nil, err
	}

	nodes := make([]common.NodeInfo, 0)
	for rows.Next() {
		node := common.NodeInfo{}
		err = rows.Scan(&node.Id, &node.ConnString, &node.Referrer, &node.Discovery, &node.LastSession, &node.Version, &node.SuccessfulSessionCount, &node.FailedSessionCount)
		if err == nil {
			nodes = append(nodes, node)
		}
	}

	_ = rows.Close()
	return nodes, rows.Err()
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