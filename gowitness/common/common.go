package common

import (
	"encoding/binary"
	"math/rand"
	"strconv"
	"strings"
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

func ParseConnString(connstring string) (string, uint16) {
	lastColonPos := strings.LastIndex(connstring, ":")
	ip := strings.Replace(connstring[:lastColonPos], "[", "", -1)
	ip = strings.Replace(ip, "]", "", -1)
	port, _ := strconv.ParseUint(connstring[lastColonPos:], 10, 16)
	return ip, uint16(port)
}