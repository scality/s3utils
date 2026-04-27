package main

import (
	"flag"
	"fmt"
)

type Config struct {
	AdminEndpoint   string
	SproxydEndpoint string
	SproxydPath     string
	Cluster         string
	RaftSessionID   string
	InstallID       int
	BackupCopies    int
	MinBseq         int
	MaxBseq         int
	DryRun          bool
	Yes             bool
}

// BackupID returns the backup identifier used for key generation: "cluster/raftSessionId".
func (c Config) BackupID() string {
	return c.Cluster + "/" + c.RaftSessionID
}

func parseFlags() Config {
	var cfg Config
	flag.StringVar(&cfg.AdminEndpoint, "admin", "http://localhost:4250",
		"MetaData repd admin endpoint (leader)")
	flag.StringVar(&cfg.SproxydEndpoint, "sproxyd", "http://localhost:8181",
		"Sproxyd endpoint")
	flag.StringVar(&cfg.SproxydPath, "sproxyd-path", "/proxy/chord",
		"Sproxyd URL path prefix")
	flag.StringVar(&cfg.Cluster, "cluster", "",
		"Cluster name from repd config (required)")
	flag.StringVar(&cfg.RaftSessionID, "raft-session-id", "",
		"Raft session ID (required)")
	flag.IntVar(&cfg.InstallID, "install-id", 0,
		"Install ID (0-255)")
	flag.IntVar(&cfg.BackupCopies, "backup-copies", 3,
		"Number of backup copies")
	flag.IntVar(&cfg.MinBseq, "min-bseq", 1,
		"First bseq to consider (skip older backups)")
	flag.IntVar(&cfg.MaxBseq, "max-bseq", 0,
		"Last bseq to consider (0 = no upper bound)")
	flag.BoolVar(&cfg.DryRun, "dry-run", false,
		"Report inconsistencies without repairing")
	flag.BoolVar(&cfg.Yes, "y", false,
		"Skip confirmation prompts between steps")
	flag.Parse()
	return cfg
}

func (c Config) validate() error {
	if c.Cluster == "" {
		return fmt.Errorf("--cluster is required")
	}
	if c.RaftSessionID == "" {
		return fmt.Errorf("--raft-session-id is required")
	}
	if c.InstallID < 0 || c.InstallID > 255 {
		return fmt.Errorf("--install-id must be between 0 and 255")
	}
	if c.MinBseq < 1 {
		return fmt.Errorf("--min-bseq must be at least 1")
	}
	if c.MaxBseq < 0 {
		return fmt.Errorf("--max-bseq must be non-negative")
	}
	if c.MaxBseq > 0 && c.MaxBseq < c.MinBseq {
		return fmt.Errorf("--max-bseq (%d) must be >= --min-bseq (%d)", c.MaxBseq, c.MinBseq)
	}
	if c.BackupCopies < 2 {
		return fmt.Errorf("--backup-copies must be at least 2")
	}
	return nil
}
