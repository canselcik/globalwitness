package storage

import (
	"database/sql"
	"fmt"
	"globalwitness/common"
	"net"

	_ "github.com/lib/pq"
)

type PostgresStorage struct {
	connString   string
	db           *sql.DB
}

func (storage *PostgresStorage) Connect() error {
	if storage.db != nil {
		return fmt.Errorf("this PostgresStorage is already connected")
	}
	db, err := sql.Open("postgres", storage.connString)
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

func (storage *PostgresStorage) AddNode(addr *net.IP, port uint16, referrerId uint64) (bool, error) {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return false, connectionErr
	}

	connString := fmt.Sprintf("[%s]:%d", addr.String(), port)
	sqlStatement := "INSERT INTO nodes (connstring, referrer, discovery, lastsession) VALUES ($1, $2, NOW(), NULL) ON CONFLICT DO NOTHING"
	res, err := storage.db.Exec(sqlStatement, connString, referrerId)
	if err != nil {
		return false, err
	}
	rowCount, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rowCount != 0, nil
}

// Takes the record with the ID and updates it with all the other fields
func (storage *PostgresStorage) UpdateAllNode(node *common.NodeInfo) (bool, error) {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return false, connectionErr
	}
	sqlStatement := "UPDATE nodes SET connstring = $1, referrer = $2, discovery = $3, lastsession = $4 WHERE id = $5"
	res, err := storage.db.Exec(sqlStatement, node.ConnString, node.Referrer, node.Discovery, node.LastSession, node.Id)
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

	rows, err := storage.db.Query("SELECT id, connstring, referrer, discovery, lastsession FROM nodes TABLESAMPLE BERNOULLI(10) LIMIT 1")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	node := common.NodeInfo{}
	if rows.Next() {
		err = rows.Scan(&node.Id, &node.ConnString, &node.Referrer, &node.Discovery, &node.LastSession)
		if err != nil {
			return nil, err
		}
	}
	return &node, nil
}

func (storage *PostgresStorage) GetNodes() ([]common.NodeInfo, error) {
	connectionErr := storage.checkConnected()
	if connectionErr != nil {
		return nil, connectionErr
	}

	rows, err := storage.db.Query("SELECT id, connstring, referrer, discovery, lastsession FROM nodes")
	if err != nil {
		return nil, err
	}

	nodes := make([]common.NodeInfo, 0)
	for rows.Next() {
		node := common.NodeInfo{}
		err = rows.Scan(&node.Id, &node.ConnString, &node.Referrer, &node.Discovery, &node.LastSession)
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
	return &PostgresStorage{
		connString: connString,
		db:         nil,
	}
}