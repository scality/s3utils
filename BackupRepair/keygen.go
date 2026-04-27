package main

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"strings"
)

type KeyGenerator struct {
	hashFirst  []byte // MD5(backupID)[0:3]
	hashSecond []byte // MD5(backupID)[3:7]
	hashThird  []byte // MD5(backupID)[7:8]
	installID  byte
}

// NewKeyGenerator creates a key generator from a backupID (typically "cluster/raftSessionId")
// and an installID (0-255).
func NewKeyGenerator(backupID string, installID int) *KeyGenerator {
	hash := md5.Sum([]byte(backupID))
	return &KeyGenerator{
		hashFirst:  hash[0:3],
		hashSecond: hash[3:7],
		hashThird:  hash[7:8],
		installID:  byte(installID),
	}
}

// GenerateKey computes the 40-hex-char sproxyd key for a given bseq and copyId.
//
// Key layout (20 bytes):
//   [0:3]   MD5(backupID)[0:3]
//   [3:11]  bseq as uint64 big-endian
//   [11:15] MD5(backupID)[3:7]
//   [15]    0x5A (service ID)
//   [16]    MD5(backupID)[7:8]
//   [17]    installID
//   [18]    copyId
//   [19]    0x20 (COS)
func (kg *KeyGenerator) GenerateKey(bseq int, copyId int) string {
	var buf [20]byte

	copy(buf[0:3], kg.hashFirst)

	binary.BigEndian.PutUint64(buf[3:11], uint64(bseq))

	copy(buf[11:15], kg.hashSecond)

	buf[15] = 0x5A

	copy(buf[16:17], kg.hashThird)

	buf[17] = kg.installID
	buf[18] = byte(copyId)
	buf[19] = 0x20

	return strings.ToUpper(fmt.Sprintf("%x", buf))
}
