package common

import (
	"encoding/binary"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// nodes table
type NodeInfo struct {
	Id                      int64         `db:"id"`
	ConnString              string        `db:"connstring"`
	Referrer                *int64       `db:"referrer"`
	Discovery               *time.Time    `db:"discovery"`
	LastSession             *time.Time    `db:"lastsession"`
	Version                 *string       `db:"version"`
	SuccessfulSessionCount  uint64        `db:"sessionok"`
	FailedSessionCount 		uint64        `db:"sessionerr"`
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
