// Copyright (c) 2024 The Flokicoin developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/flokiorg/go-flokicoin/database"
	_ "github.com/flokiorg/go-flokicoin/database/ffldb" // Register the ffldb driver.
	"github.com/flokiorg/go-flokicoin/limits"
	flog "github.com/flokiorg/go-flokicoin/log"
)

const (
	// blockDbNamePrefix is the prefix for the flokicoind block database.
	blockDbNamePrefix = "blocks"

	// richDbNamePrefix is the prefix for the rich list index database.
	richDbNamePrefix = "richlist"
)

var (
	cfg *config
	log flog.Logger
)

// loadBlockDB opens the block database and returns a handle to it.
func loadBlockDB() (database.DB, error) {
	dbName := blockDbNamePrefix + "_" + cfg.DbType
	dbPath := filepath.Join(cfg.DataDir, dbName)

	log.Infof("Opening block database from %s", dbPath)
	db, err := database.Open(cfg.DbType, dbPath, activeNetParams.Net)
	if err != nil {
		if dbErr, ok := err.(database.Error); ok && dbErr.ErrorCode == database.ErrDbDoesNotExist {
			return nil, fmt.Errorf("block database %q does not exist (path %s)", dbName, dbPath)
		}
		return nil, err
	}
	log.Infof("Block database opened")
	return db, nil
}

// loadRichListDB opens (and creates if necessary) the rich list index database.
func loadRichListDB() (database.DB, error) {
	dbName := richDbNamePrefix + "_" + cfg.DbType
	dbPath := filepath.Join(cfg.RichDataDir, dbName)

	if cfg.Reindex {
		log.Infof("Reindex requested, removing existing rich list database at %s", dbPath)
		if err := os.RemoveAll(dbPath); err != nil {
			return nil, fmt.Errorf("failed to remove existing rich list db: %w", err)
		}
	}

	if err := os.MkdirAll(cfg.RichDataDir, 0o700); err != nil {
		return nil, fmt.Errorf("unable to create rich list data dir: %w", err)
	}

	log.Infof("Opening rich list database at %s", dbPath)
	db, err := database.Open(cfg.DbType, dbPath, activeNetParams.Net)
	if err != nil {
		// Create the db if it does not exist.
		var dbErr database.Error
		if errors.As(err, &dbErr) && dbErr.ErrorCode == database.ErrDbDoesNotExist {
			db, err = database.Create(cfg.DbType, dbPath, activeNetParams.Net)
		}
		if err != nil {
			return nil, err
		}
		if cfg.Reindex {
			log.Info("Rich list database recreated")
		} else {
			log.Info("Rich list database created")
		}
	} else {
		log.Info("Rich list database opened")
	}
	return db, nil
}

// setupInterruptHandler returns a closure that reports whether an interrupt
// signal has been received.
func setupInterruptHandler() (func() bool, func()) {
	interrupt := make(chan struct{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case <-signals:
			log.Warn("Interrupt received, shutting down...")
			close(interrupt)
		}
	}()

	isInterrupted := func() bool {
		select {
		case <-interrupt:
			return true
		default:
			return false
		}
	}

	cleanup := func() {
		signal.Stop(signals)
		close(signals)
	}

	return isInterrupted, cleanup
}

// realMain is the real main function for the utility. It is necessary to work
// around the fact that deferred functions do not run when os.Exit() is called.
func realMain() error {
	tcfg, _, err := loadConfig()
	if err != nil {
		return err
	}
	cfg = tcfg

	if err := limits.SetLimits(); err != nil {
		return err
	}

	backendLogger := flog.NewBackend(os.Stdout)
	defer os.Stdout.Sync()
	log = backendLogger.Logger("MAIN")
	dbLogger := backendLogger.Logger("DB")
	dbLogger.SetLevel(flog.LevelWarn)
	database.UseLogger(dbLogger)

	sourceDB, err := loadBlockDB()
	if err != nil {
		log.Errorf("Failed to open block database: %v", err)
		return err
	}
	defer sourceDB.Close()

	indexDB, err := loadRichListDB()
	if err != nil {
		log.Errorf("Failed to open rich list database: %v", err)
		return err
	}
	defer indexDB.Close()

	isInterrupted, cleanupInterrupts := setupInterruptHandler()
	defer cleanupInterrupts()

	indexer := newRichListIndexer(sourceDB, indexDB, activeNetParams, cfg, log)

	start := time.Now()
	if err := indexer.Run(isInterrupted); err != nil {
		log.Errorf("Indexing failed: %v", err)
		return err
	}
	log.Infof("Indexing completed in %s", time.Since(start).Truncate(time.Millisecond))

	if err := indexer.RenderSummary(); err != nil {
		log.Errorf("Failed to render summary: %v", err)
		return err
	}

	return nil
}

func main() {
	if err := realMain(); err != nil {
		os.Exit(1)
	}
}
