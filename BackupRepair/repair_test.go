package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newMockAdminServer(copies map[int][]BackupIndexEntry) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		var copyNum, minBseq, limit, maxBseq int
		fmt.Sscanf(q.Get("copy"), "%d", &copyNum)
		fmt.Sscanf(q.Get("minBseq"), "%d", &minBseq)
		fmt.Sscanf(q.Get("limit"), "%d", &limit)
		fmt.Sscanf(q.Get("maxBseq"), "%d", &maxBseq)
		if limit == 0 {
			limit = indexPageLimit
		}

		all := copies[copyNum]
		var result []BackupIndexEntry
		for _, e := range all {
			if e.Bseq >= minBseq && (maxBseq == 0 || e.Bseq <= maxBseq) && len(result) < limit {
				result = append(result, e)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}))
}

func runFindInconsistent(t *testing.T, copies map[int][]BackupIndexEntry, backupCopies int) []missingPrimary {
	t.Helper()
	return runFindInconsistentRange(t, copies, backupCopies, 1, 0)
}

func runFindInconsistentRange(t *testing.T, copies map[int][]BackupIndexEntry, backupCopies, minBseq, maxBseq int) []missingPrimary {
	t.Helper()
	server := newMockAdminServer(copies)
	defer server.Close()
	admin := NewAdminClient(server.URL, "0")
	missing, err := findInconsistentBseqs(admin, backupCopies, minBseq, maxBseq)
	if err != nil {
		t.Fatal(err)
	}
	return missing
}

func TestFindInconsistent_NoneMissing(t *testing.T) {
	missing := runFindInconsistent(t, map[int][]BackupIndexEntry{
		0: {{Bseq: 1}, {Bseq: 2}, {Bseq: 3}},
		1: {{Bseq: 1}, {Bseq: 2}, {Bseq: 3}},
		2: {{Bseq: 1}, {Bseq: 2}, {Bseq: 3}},
	}, 3)
	if len(missing) != 0 {
		t.Errorf("expected 0 missing, got %d", len(missing))
	}
}

func TestFindInconsistent_GapInMiddle(t *testing.T) {
	missing := runFindInconsistent(t, map[int][]BackupIndexEntry{
		0: {{Bseq: 1}, {Bseq: 2}, {Bseq: 5}},
		1: {{Bseq: 1}, {Bseq: 2}, {Bseq: 3}, {Bseq: 4}, {Bseq: 5}},
		2: {{Bseq: 1}, {Bseq: 2}, {Bseq: 3}, {Bseq: 4}, {Bseq: 5}},
	}, 3)

	if len(missing) != 2 {
		t.Fatalf("expected 2 missing, got %d", len(missing))
	}
	if missing[0].bseq != 3 || missing[0].donorCopy != 1 {
		t.Errorf("missing[0] = %+v, want bseq=3 donorCopy=1", missing[0])
	}
	if missing[1].bseq != 4 || missing[1].donorCopy != 1 {
		t.Errorf("missing[1] = %+v, want bseq=4 donorCopy=1", missing[1])
	}
}

func TestFindInconsistent_PrefersCopy1Over2(t *testing.T) {
	missing := runFindInconsistent(t, map[int][]BackupIndexEntry{
		0: {{Bseq: 2}},
		1: {{Bseq: 1}, {Bseq: 2}},
		2: {{Bseq: 1}, {Bseq: 2}},
	}, 3)

	if len(missing) != 1 {
		t.Fatalf("expected 1 missing, got %d", len(missing))
	}
	if missing[0].donorCopy != 1 {
		t.Errorf("expected donorCopy=1, got %d", missing[0].donorCopy)
	}
}

func TestFindInconsistent_FallsToCopy2(t *testing.T) {
	missing := runFindInconsistent(t, map[int][]BackupIndexEntry{
		0: {{Bseq: 2}},
		1: {{Bseq: 2}},
		2: {{Bseq: 1}, {Bseq: 2}},
	}, 3)

	if len(missing) != 1 {
		t.Fatalf("expected 1 missing, got %d", len(missing))
	}
	if missing[0].donorCopy != 2 {
		t.Errorf("expected donorCopy=2, got %d", missing[0].donorCopy)
	}
}

func TestFindInconsistent_MissingFromAllCopies(t *testing.T) {
	// bseq 2 is missing from all copies — gap detected, but no donor found
	missing := runFindInconsistent(t, map[int][]BackupIndexEntry{
		0: {{Bseq: 1}, {Bseq: 3}},
		1: {{Bseq: 1}, {Bseq: 3}},
		2: {{Bseq: 1}, {Bseq: 3}},
	}, 3)

	if len(missing) != 0 {
		t.Errorf("expected 0 missing (no donor), got %d", len(missing))
	}
}

func TestFindInconsistent_TwoCopiesMode(t *testing.T) {
	missing := runFindInconsistent(t, map[int][]BackupIndexEntry{
		0: {{Bseq: 1}, {Bseq: 3}},
		1: {{Bseq: 1}, {Bseq: 2}, {Bseq: 3}},
	}, 2)

	if len(missing) != 1 {
		t.Fatalf("expected 1 missing, got %d", len(missing))
	}
	if missing[0].bseq != 2 || missing[0].donorCopy != 1 {
		t.Errorf("expected bseq=2 donorCopy=1, got %+v", missing[0])
	}
}

func TestFindInconsistent_MaxBseq(t *testing.T) {
	copies := map[int][]BackupIndexEntry{
		0: {{Bseq: 1}, {Bseq: 4}, {Bseq: 8}},
		1: {{Bseq: 1}, {Bseq: 2}, {Bseq: 3}, {Bseq: 4}, {Bseq: 5}, {Bseq: 6}, {Bseq: 7}, {Bseq: 8}},
	}

	missing := runFindInconsistentRange(t, copies, 2, 1, 5)
	expected := []int{2, 3, 5}
	if len(missing) != len(expected) {
		t.Fatalf("expected %d missing, got %d: %+v", len(expected), len(missing), missing)
	}
	for i, e := range expected {
		if missing[i].bseq != e {
			t.Errorf("missing[%d].bseq = %d, want %d", i, missing[i].bseq, e)
		}
	}
}

func TestFindInconsistent_MinAndMaxBseq(t *testing.T) {
	copies := map[int][]BackupIndexEntry{
		0: {{Bseq: 1}, {Bseq: 4}, {Bseq: 8}},
		1: {{Bseq: 1}, {Bseq: 2}, {Bseq: 3}, {Bseq: 4}, {Bseq: 5}, {Bseq: 6}, {Bseq: 7}, {Bseq: 8}},
	}

	missing := runFindInconsistentRange(t, copies, 2, 3, 6)
	expected := []int{3, 5, 6}
	if len(missing) != len(expected) {
		t.Fatalf("expected %d missing, got %d: %+v", len(expected), len(missing), missing)
	}
	for i, e := range expected {
		if missing[i].bseq != e {
			t.Errorf("missing[%d].bseq = %d, want %d", i, missing[i].bseq, e)
		}
	}
}

func TestFindInconsistent_MaxBseqBeyondPrimary(t *testing.T) {
	// Primary exhausted at bseq 3, but maxBseq is 6 — gaps 4-6 must still be detected.
	copies := map[int][]BackupIndexEntry{
		0: {{Bseq: 1}, {Bseq: 3}},
		1: {{Bseq: 1}, {Bseq: 2}, {Bseq: 3}, {Bseq: 4}, {Bseq: 5}, {Bseq: 6}},
	}

	missing := runFindInconsistentRange(t, copies, 2, 1, 6)
	expected := []int{2, 4, 5, 6}
	if len(missing) != len(expected) {
		t.Fatalf("expected %d missing, got %d: %+v", len(expected), len(missing), missing)
	}
	for i, e := range expected {
		if missing[i].bseq != e {
			t.Errorf("missing[%d].bseq = %d, want %d", i, missing[i].bseq, e)
		}
	}
}

func TestFindInconsistent_MultipleGaps(t *testing.T) {
	missing := runFindInconsistent(t, map[int][]BackupIndexEntry{
		0: {{Bseq: 1}, {Bseq: 4}, {Bseq: 8}},
		1: {{Bseq: 1}, {Bseq: 2}, {Bseq: 3}, {Bseq: 4}, {Bseq: 5}, {Bseq: 6}, {Bseq: 7}, {Bseq: 8}},
	}, 2)

	if len(missing) != 5 {
		t.Fatalf("expected 5 missing, got %d: %+v", len(missing), missing)
	}
	expected := []int{2, 3, 5, 6, 7}
	for i, e := range expected {
		if missing[i].bseq != e {
			t.Errorf("missing[%d].bseq = %d, want %d", i, missing[i].bseq, e)
		}
	}
}
