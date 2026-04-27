package main

import (
	"log"
	"time"
)

const (
	maxCopyRetries = 3
	retryBaseWait  = 1 * time.Second
)

// missingPrimary records a bseq whose primary is absent and one available donor copy.
type missingPrimary struct {
	bseq      int
	donorCopy int
}

// findInconsistentBseqs walks copy 0 page by page expecting contiguous bseqs.
// Whenever a gap is found, it queries secondaries one by one (limit=1) to find
// a donor. If no secondary has the bseq either, it logs a warning.
// Trailing bseqs that only exist on secondaries are intentionally ignored:
// this tool targets historical gaps, not an in-progress primary that hasn't
// caught up yet.
func findInconsistentBseqs(admin *AdminClient, backupCopies, minBseq, maxBseq int) ([]missingPrimary, error) {
	primary := newIndexIterator(admin, 0)
	if minBseq > 1 {
		primary.nextMin = minBseq
	}
	var missing []missingPrimary
	expectedBseq := minBseq

	fillGaps := func(from, to int) error {
		for g := from; g < to; g++ {
			donor, err := findDonor(admin, g, backupCopies)
			if err != nil {
				return err
			}
			if donor == -1 {
				log.Printf("  WARNING bseq=%d: missing from all copies", g)
			} else {
				missing = append(missing, missingPrimary{bseq: g, donorCopy: donor})
			}
		}
		return nil
	}

	for {
		bseq, ok, err := primary.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		gapEnd := bseq
		if maxBseq > 0 && gapEnd > maxBseq+1 {
			gapEnd = maxBseq + 1
		}
		if err := fillGaps(expectedBseq, gapEnd); err != nil {
			return nil, err
		}
		if maxBseq > 0 && bseq > maxBseq {
			expectedBseq = maxBseq + 1
			break
		}
		expectedBseq = bseq + 1
	}

	if maxBseq > 0 && expectedBseq <= maxBseq {
		if err := fillGaps(expectedBseq, maxBseq+1); err != nil {
			return nil, err
		}
	}

	log.Printf("  scanned copy 0: %d entries (max bseq %d)", primary.count, primary.maxBseq)

	return missing, nil
}

// findDonor queries secondaries one by one for a specific bseq.
// Returns the copy number of the first secondary that has it, or -1 if none.
func findDonor(admin *AdminClient, bseq, backupCopies int) (int, error) {
	for copyNum := 1; copyNum < backupCopies; copyNum++ {
		has, err := admin.HasBseq(copyNum, bseq)
		if err != nil {
			return -1, err
		}
		if has {
			return copyNum, nil
		}
	}
	return -1, nil
}

// repairMissingPrimaries copies the donor secondary to the primary key for each
// missing bseq. Returns counts of successful, skipped (donor vanished), and failed repairs.
func repairMissingPrimaries(
	sproxyd *SproxydClient,
	keygen *KeyGenerator,
	missing []missingPrimary,
	dryRun bool,
) (repaired, skipped, failed int) {
	for _, m := range missing {
		srcKey := keygen.GenerateKey(m.bseq, m.donorCopy)
		dstKey := keygen.GenerateKey(m.bseq, 0)

		if dryRun {
			log.Printf("  [dry-run] bseq=%d: would copy %s (copy %d) -> %s (copy 0)",
				m.bseq, srcKey, m.donorCopy, dstKey)
			repaired++
			continue
		}

		copied, err := copyKeyWithRetry(sproxyd, srcKey, dstKey, m.bseq, m.donorCopy)
		if err != nil {
			log.Printf("  ERROR bseq=%d: %v", m.bseq, err)
			failed++
			continue
		}
		if !copied {
			skipped++
			continue
		}

		repaired++
		log.Printf("  repaired bseq=%d: copied from copy %d", m.bseq, m.donorCopy)
	}
	return
}

func copyKeyWithRetry(sproxyd *SproxydClient, srcKey, dstKey string, bseq, donorCopy int) (bool, error) {
	var lastErr error
	for attempt := range maxCopyRetries {
		copied, err := copyKey(sproxyd, srcKey, dstKey, bseq, donorCopy)
		if err == nil {
			return copied, nil
		}
		lastErr = err
		if attempt < maxCopyRetries-1 {
			wait := retryBaseWait * time.Duration(attempt+1)
			log.Printf("  retrying bseq=%d after %v (attempt %d/%d): %v",
				bseq, wait, attempt+1, maxCopyRetries, err)
			time.Sleep(wait)
		}
	}
	return false, lastErr
}

func copyKey(sproxyd *SproxydClient, srcKey, dstKey string, bseq, donorCopy int) (bool, error) {
	body, info, err := sproxyd.Get(srcKey)
	if err != nil {
		return false, err
	}
	if body == nil {
		log.Printf("  WARN bseq=%d: donor copy %d vanished (404) — skipping", bseq, donorCopy)
		return false, nil
	}
	defer body.Close()

	return true, sproxyd.Put(dstKey, body, info)
}

// verifyRepairs re-fetches the backup index for copy 0 and checks that all
// repaired bseqs are now present.
func verifyRepairs(admin *AdminClient, missing []missingPrimary) {
	entries, err := admin.GetBackupIndex(0)
	if err != nil {
		log.Printf("verification failed: could not fetch index: %v", err)
		return
	}

	primarySet := make(map[int]bool, len(entries))
	for _, e := range entries {
		primarySet[e.Bseq] = true
	}

	stillMissing := 0
	for _, m := range missing {
		if !primarySet[m.bseq] {
			log.Printf("  STILL MISSING bseq=%d", m.bseq)
			stillMissing++
		}
	}

	if stillMissing == 0 {
		log.Printf("verification passed: all %d repaired primaries confirmed in index", len(missing))
	} else {
		log.Printf("verification: %d / %d primaries still missing after repair", stillMissing, len(missing))
	}
}
