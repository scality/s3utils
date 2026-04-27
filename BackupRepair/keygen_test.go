package main

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
)

// referenceKey reimplements the Node.js computeKey logic as a direct translation,
// to cross-check against the KeyGenerator implementation.
func referenceKey(raftSessionID string, installID int, bseq int, copyId int) string {
	hash := md5.Sum([]byte(raftSessionID))
	first := hash[0:3]
	second := hash[3:7]
	third := hash[7:8]

	var buf [20]byte
	copy(buf[0:3], first)
	binary.BigEndian.PutUint64(buf[3:11], uint64(bseq))
	copy(buf[11:15], second)
	buf[15] = 0x5A
	copy(buf[16:17], third)
	buf[17] = byte(installID)
	buf[18] = byte(copyId)
	buf[19] = 0x20

	return strings.ToUpper(fmt.Sprintf("%x", buf))
}

func TestGenerateKeyBasic(t *testing.T) {
	kg := NewKeyGenerator("0", 0)

	tests := []struct {
		bseq   int
		copyId int
	}{
		{1, 0},
		{1, 1},
		{1, 2},
		{42, 0},
		{10000, 0},
		{10000, 1},
		{999999, 2},
	}

	for _, tt := range tests {
		got := kg.GenerateKey(tt.bseq, tt.copyId)
		want := referenceKey("0", 0, tt.bseq, tt.copyId)
		if got != want {
			t.Errorf("GenerateKey(%d, %d) = %s, want %s", tt.bseq, tt.copyId, got, want)
		}
	}
}

func TestGenerateKeyLength(t *testing.T) {
	kg := NewKeyGenerator("test-session", 5)
	key := kg.GenerateKey(1, 0)
	if len(key) != 40 {
		t.Errorf("key length = %d, want 40", len(key))
	}
}

func TestGenerateKeyCopyIdDifference(t *testing.T) {
	kg := NewKeyGenerator("0", 0)

	key0 := kg.GenerateKey(42, 0)
	key1 := kg.GenerateKey(42, 1)
	key2 := kg.GenerateKey(42, 2)

	if key0 == key1 || key0 == key2 || key1 == key2 {
		t.Errorf("keys for different copyIds should differ: %s, %s, %s", key0, key1, key2)
	}

	// Only byte 18 (hex chars 36-37) should differ.
	if key0[:36] != key1[:36] || key0[:36] != key2[:36] {
		t.Errorf("keys should share all bytes except byte 18")
	}
	if key0[38:] != key1[38:] || key0[38:] != key2[38:] {
		t.Errorf("keys should share the trailing COS byte")
	}
}

func TestGenerateKeyDifferentSessions(t *testing.T) {
	kg1 := NewKeyGenerator("0", 0)
	kg2 := NewKeyGenerator("1", 0)

	key1 := kg1.GenerateKey(1, 0)
	key2 := kg2.GenerateKey(1, 0)

	if key1 == key2 {
		t.Errorf("different raft sessions should produce different keys")
	}
}

func TestGenerateKeyDifferentInstallIDs(t *testing.T) {
	kg1 := NewKeyGenerator("0", 0)
	kg2 := NewKeyGenerator("0", 1)

	key1 := kg1.GenerateKey(1, 0)
	key2 := kg2.GenerateKey(1, 0)

	if key1 == key2 {
		t.Errorf("different install IDs should produce different keys")
	}
}

func TestGenerateKeyWithBackupID(t *testing.T) {
	// In production, the key is hashed from "cluster/raftSessionId", not just raftSessionId.
	kg := NewKeyGenerator("bucket/7", 0)

	got := kg.GenerateKey(1, 0)
	want := referenceKey("bucket/7", 0, 1, 0)
	if got != want {
		t.Errorf("GenerateKey(1, 0) = %s, want %s", got, want)
	}
	if got != "E2D7E50000000000000001B39A787D5A0F000020" {
		t.Errorf("key does not match known value from Node.js: %s", got)
	}
}
