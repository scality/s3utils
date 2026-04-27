package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type AdminClient struct {
	endpoint string
	client   *http.Client
}

type BackupIndexEntry struct {
	Bseq          int    `json:"bseq"`
	CopyNumber    int    `json:"copyNumber"`
	Size          int    `json:"size"`
	CumSize       int    `json:"cumsize"`
	Time          string `json:"time"`
	FormatVersion int    `json:"formatVersion"`
	Compression   string `json:"compression"`
}

type ReindexStatus struct {
	Status         string `json:"status"`
	ProcessingBseq int    `json:"processingBseq"`
	TargetBseq     int    `json:"targetBseq"`
	Error          string `json:"error"`
}

func NewAdminClient(endpoint string) *AdminClient {
	return &AdminClient{
		endpoint: endpoint,
		client:   &http.Client{Timeout: 30 * time.Second},
	}
}

const indexPageLimit = 1000

// HasBseq checks whether a specific bseq exists for a given copy number.
func (a *AdminClient) HasBseq(copyNumber, bseq int) (bool, error) {
	entries, err := a.getBackupIndexPage(copyNumber, bseq, 1, bseq)
	if err != nil {
		return false, err
	}
	return len(entries) > 0 && entries[0].Bseq == bseq, nil
}

// getBackupIndexPage fetches a single page of backup index entries.
// If maxBseq is 0, no upper bound is applied.
func (a *AdminClient) getBackupIndexPage(copyNumber, minBseq, limit, maxBseq int) ([]BackupIndexEntry, error) {
	url := fmt.Sprintf("%s/_/raft/backups?copy=%d&limit=%d&minBseq=%d",
		a.endpoint, copyNumber, limit, minBseq)
	if maxBseq > 0 {
		url += fmt.Sprintf("&maxBseq=%d", maxBseq)
	}

	resp, err := a.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned %d: %s", url, resp.StatusCode, string(body))
	}

	var entries []BackupIndexEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}
	return entries, nil
}

// indexIterator streams backup index entries page by page for a single copy.
type indexIterator struct {
	admin   *AdminClient
	copyNum int
	buf     []BackupIndexEntry
	pos     int
	nextMin int
	done    bool
	count   int
	maxBseq int
}

func newIndexIterator(admin *AdminClient, copyNum int) *indexIterator {
	return &indexIterator{admin: admin, copyNum: copyNum, nextMin: 1}
}

// next returns the next bseq, or (0, false, nil) when exhausted.
func (it *indexIterator) next() (int, bool, error) {
	if err := it.fill(); err != nil {
		return 0, false, err
	}
	if it.done {
		return 0, false, nil
	}
	bseq := it.buf[it.pos].Bseq
	it.pos++
	it.count++
	if bseq > it.maxBseq {
		it.maxBseq = bseq
	}
	return bseq, true, nil
}

// peek returns the current bseq without consuming it, or (0, false, nil) when exhausted.
func (it *indexIterator) peek() (int, bool, error) {
	if err := it.fill(); err != nil {
		return 0, false, err
	}
	if it.done {
		return 0, false, nil
	}
	return it.buf[it.pos].Bseq, true, nil
}

func (it *indexIterator) fill() error {
	if it.done || it.pos < len(it.buf) {
		return nil
	}
	entries, err := it.admin.getBackupIndexPage(it.copyNum, it.nextMin, indexPageLimit, 0)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		it.done = true
		return nil
	}
	it.buf = entries
	it.pos = 0
	it.nextMin = entries[len(entries)-1].Bseq + 1
	return nil
}

// GetBackupIndex fetches all backup index entries for a given copy number.
// Used by verifyRepairs where we need the full list.
func (a *AdminClient) GetBackupIndex(copyNumber int) ([]BackupIndexEntry, error) {
	var allEntries []BackupIndexEntry
	it := newIndexIterator(a, copyNumber)
	for {
		if err := it.fill(); err != nil {
			return nil, err
		}
		if it.done {
			break
		}
		allEntries = append(allEntries, it.buf[it.pos:]...)
		it.pos = len(it.buf)
	}
	return allEntries, nil
}

func (a *AdminClient) TriggerReindex() error {
	url := a.endpoint + "/_/raft/backups/reindex"
	resp, err := a.client.Post(url, "", nil)
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("POST %s returned %d: %s", url, resp.StatusCode, string(body))
	}
	return nil
}

func (a *AdminClient) GetReindexStatus() (*ReindexStatus, error) {
	url := a.endpoint + "/_/raft/backups/reindex"
	resp, err := a.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned %d: %s", url, resp.StatusCode, string(body))
	}

	var status ReindexStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return nil, fmt.Errorf("parsing reindex status: %w", err)
	}
	return &status, nil
}

const maxStallPolls = 10

func (a *AdminClient) WaitForReindex(pollInterval time.Duration) error {
	lastBseq := -1
	stallCount := 0

	for {
		status, err := a.GetReindexStatus()
		if err != nil {
			return err
		}

		switch status.Status {
		case "success":
			return nil
		case "failed":
			return fmt.Errorf("reindex job failed: %s", status.Error)
		case "running":
			if status.TargetBseq > 0 {
				pct := float64(status.ProcessingBseq) / float64(status.TargetBseq) * 100
				log.Printf("  reindex progress: bseq %d / %d (%.1f%%)",
					status.ProcessingBseq, status.TargetBseq, pct)
			}
			if status.ProcessingBseq == lastBseq {
				stallCount++
				if stallCount >= maxStallPolls {
					return fmt.Errorf("reindex stalled at bseq %d for %d consecutive polls",
						lastBseq, stallCount)
				}
			} else {
				stallCount = 0
				lastBseq = status.ProcessingBseq
			}
		default:
			return fmt.Errorf("reindex returned unknown status: %q", status.Status)
		}

		time.Sleep(pollInterval)
	}
}
