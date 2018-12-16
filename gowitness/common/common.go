package common

import (
	"encoding/binary"
	"math/rand"
	"time"
)

/* CREATE TABLE nodes  (id SERIAL,
						connstring TEXT PRIMARY KEY,
						referrer INTEGER,
						discovery TIMESTAMP,
						lastsession TIMESTAMP);
*/
type NodeInfo struct {
	Id           uint64
	ConnString   string
	Referrer     *uint64
	Discovery    *time.Time
	LastSession  *time.Time
}

func GenerateNonce() uint64 {
	buf := make([]byte, 8)
	rand.Read(buf)
	return binary.LittleEndian.Uint64(buf)
}