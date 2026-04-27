package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

func formatBseqs(bseqs []int) string {
	const maxDisplay = 50
	if len(bseqs) <= maxDisplay {
		items := make([]string, len(bseqs))
		for i, b := range bseqs {
			items[i] = fmt.Sprintf("%d", b)
		}
		return strings.Join(items, ", ")
	}

	half := maxDisplay / 2
	head := make([]string, half)
	tail := make([]string, half)
	for i := 0; i < half; i++ {
		head[i] = fmt.Sprintf("%d", bseqs[i])
		tail[i] = fmt.Sprintf("%d", bseqs[len(bseqs)-half+i])
	}
	return fmt.Sprintf("%s, ... (%d more) ..., %s",
		strings.Join(head, ", "), len(bseqs)-maxDisplay, strings.Join(tail, ", "))
}

var stdinReader = bufio.NewReader(os.Stdin)

func confirmStep(skip bool) {
	if skip {
		return
	}
	// Discard any buffered input so a stray enter doesn't auto-approve.
	for stdinReader.Buffered() > 0 {
		stdinReader.ReadByte()
	}
	fmt.Fprint(os.Stderr, "press enter to continue (or ctrl-c to abort)... ")
	if _, err := stdinReader.ReadString('\n'); err != nil {
		log.Fatalf("stdin closed unexpectedly (non-interactive environment?): %v — use -y to skip prompts", err)
	}
}

func main() {
	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		flag.Usage()
		os.Exit(1)
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	backupID := cfg.BackupID()
	log.Printf("backup repair starting (backupId=%s installID=%d copies=%d)",
		backupID, cfg.InstallID, cfg.BackupCopies)
	log.Printf("bucketd: %s  sproxyd: %s%s",
		cfg.BucketdEndpoint, cfg.SproxydEndpoint, cfg.SproxydPath)

	admin := NewAdminClient(cfg.BucketdEndpoint, cfg.RaftSessionID)
	sproxyd := NewSproxydClient(cfg.SproxydEndpoint, cfg.SproxydPath)
	keygen := NewKeyGenerator(backupID, cfg.InstallID)

	// Step 1: fetch the backup index and identify inconsistencies
	log.Println("step 1: scanning backup index for missing primary copies...")
	missing, err := findInconsistentBseqs(admin, cfg.BackupCopies, cfg.MinBseq, cfg.MaxBseq)
	if err != nil {
		log.Fatalf("failed to scan backup index: %v", err)
	}
	if len(missing) == 0 {
		log.Println("no inconsistencies found — all primary copies are present")
		return
	}
	log.Printf("found %d bseqs with missing primary copy: %s",
		len(missing), formatBseqs(missingBseqs(missing)))
	confirmStep(cfg.Yes || cfg.DryRun)

	// Step 2: repair
	log.Println("step 2: repairing missing primary copies...")
	repaired, skipped, failed := repairMissingPrimaries(sproxyd, keygen, missing, cfg.DryRun)
	if cfg.DryRun {
		log.Printf("[dry-run] %d would be repaired, %d have no donor", repaired, skipped)
	} else {
		log.Printf("repair complete: %d repaired, %d skipped (donor vanished), %d failed",
			repaired, skipped, failed)
	}

	if cfg.DryRun {
		log.Println("dry-run mode — skipping reindex and verification")
		return
	}

	if repaired == 0 {
		log.Println("no repairs succeeded — skipping reindex")
		return
	}
	confirmStep(cfg.Yes)

	// Step 3: trigger reindex
	log.Println("step 3: triggering backup reindex...")
	if err := admin.TriggerReindex(); err != nil {
		log.Fatalf("failed to trigger reindex: %v", err)
	}

	// Step 4: wait for reindex completion
	log.Println("step 4: waiting for reindex to complete...")
	if err := admin.WaitForReindex(30 * time.Second); err != nil {
		log.Fatalf("reindex failed: %v", err)
	}
	log.Println("reindex completed successfully")
	confirmStep(cfg.Yes)

	// Step 5: verify
	log.Println("step 5: verifying repairs...")
	verifyRepairs(admin, missing)
}
