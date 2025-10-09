// Copyright (c) 2024 The Flokicoin developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/flokiorg/go-flokicoin/chaincfg"
	"github.com/flokiorg/go-flokicoin/chainutil"
	"github.com/flokiorg/go-flokicoin/database"
	_ "github.com/flokiorg/go-flokicoin/database/ffldb" // Register the ffldb driver.
	"github.com/flokiorg/go-flokicoin/wire"
)

const (
	defaultDbType      = "ffldb"
	defaultTopLimit    = 100
	defaultProgressSec = 10
)

var (
	flokicoindHomeDir = chainutil.AppDataDir("flokicoind", false)
	defaultDataDir    = filepath.Join(flokicoindHomeDir, "data")
	knownDbTypes      = database.SupportedDrivers()
	activeNetParams   = &chaincfg.MainNetParams
)

// config defines the configuration options for the richlist indexer command.
type config struct {
	DataDir        string `long:"datadir" description:"Location of the flokicoind data directory"`
	RichDataDir    string `long:"richdatadir" description:"Location for the rich list index data (defaults to <datadir>/<network>/richlist)"`
	DbType         string `long:"dbtype" description:"Database backend to use for both block and rich list data"`
	Limit          int    `long:"limit" description:"Number of top addresses to display after indexing"`
	Progress       int    `long:"progress" description:"Show a progress message each time this number of seconds have passed -- Use 0 to disable progress announcements"`
	DuneOutput     string `long:"dune-output" description:"Optional CSV output path for top addresses (address,balance_flc,last_seen_utc) to power Dune dashboards"`
	Reindex        bool   `long:"reindex" description:"Drop existing rich list data and rebuild from scratch"`
	TestNet3       bool   `long:"testnet" description:"Use the test network"`
	RegressionTest bool   `long:"regtest" description:"Use the regression test network"`
	SimNet         bool   `long:"simnet" description:"Use the simulation test network"`
}

// fileExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// validDbType returns whether or not dbType is a supported database type.
func validDbType(dbType string) bool {
	for _, knownType := range knownDbTypes {
		if dbType == knownType {
			return true
		}
	}
	return false
}

// netName returns the name used when referring to a flokicoin network. This
// mirrors the logic used by flokicoind for filesystem layout.
func netName(chainParams *chaincfg.Params) string {
	switch chainParams.Net {
	case wire.TestNet3:
		return "testnet"
	default:
		return chainParams.Name
	}
}

// loadConfig initializes and parses the config using command line options.
func loadConfig() (*config, []string, error) {
	// Default config.
	cfg := config{
		DataDir:  defaultDataDir,
		DbType:   defaultDbType,
		Limit:    defaultTopLimit,
		Progress: defaultProgressSec,
	}

	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.StringVar(&cfg.DataDir, "datadir", cfg.DataDir, "Location of the flokicoind data directory")
	fs.StringVar(&cfg.RichDataDir, "richdatadir", cfg.RichDataDir, "Location for the rich list index data (defaults to <datadir>/<network>/richlist)")
	fs.StringVar(&cfg.DbType, "dbtype", cfg.DbType, "Database backend to use for both block and rich list data")
	fs.IntVar(&cfg.Limit, "limit", cfg.Limit, "Number of top addresses to display after indexing")
	fs.IntVar(&cfg.Progress, "progress", cfg.Progress, "Show a progress message each time this number of seconds have passed -- Use 0 to disable progress announcements")
	fs.StringVar(&cfg.DuneOutput, "dune-output", cfg.DuneOutput, "Optional CSV output path for top addresses (address,balance_flc,last_seen_utc) to power Dune dashboards")
	fs.BoolVar(&cfg.Reindex, "reindex", cfg.Reindex, "Drop existing rich list data and rebuild from scratch")
	fs.BoolVar(&cfg.TestNet3, "testnet", cfg.TestNet3, "Use the test network")
	fs.BoolVar(&cfg.RegressionTest, "regtest", cfg.RegressionTest, "Use the regression test network")
	fs.BoolVar(&cfg.SimNet, "simnet", cfg.SimNet, "Use the simulation test network")

	err := fs.Parse(os.Args[1:])
	if err != nil {
		return nil, nil, err
	}
	remainingArgs := fs.Args()

	// Multiple networks can't be selected simultaneously.
	numNets := 0
	if cfg.TestNet3 {
		numNets++
		activeNetParams = &chaincfg.TestNet3Params
	}
	if cfg.RegressionTest {
		numNets++
		activeNetParams = &chaincfg.RegressionNetParams
	}
	if cfg.SimNet {
		numNets++
		activeNetParams = &chaincfg.SimNetParams
	}
	if numNets > 1 {
		err := fmt.Errorf("loadConfig: the testnet, regtest, and simnet params can't be used together -- choose one")
		return nil, nil, err
	}

	// Validate database type.
	if !validDbType(cfg.DbType) {
		err := fmt.Errorf("loadConfig: the specified database type [%v] is invalid -- supported types %v", cfg.DbType, knownDbTypes)
		return nil, nil, err
	}

	// Validate top limit.
	if cfg.Limit <= 0 {
		err := fmt.Errorf("loadConfig: limit must be greater than zero")
		return nil, nil, err
	}

	if cfg.Progress < 0 {
		err := fmt.Errorf("loadConfig: progress interval must be zero or positive")
		return nil, nil, err
	}

	// Append the network type to the data directory so it is "namespaced"
	// per network.
	cfg.DataDir = filepath.Join(cfg.DataDir, netName(activeNetParams))

	// Default rich data dir if none supplied.
	if cfg.RichDataDir == "" {
		cfg.RichDataDir = filepath.Join(cfg.DataDir, "richlist")
	} else {
		// If a custom path is supplied, namespace it by network as well.
		if !filepath.IsAbs(cfg.RichDataDir) {
			cfg.RichDataDir, err = filepath.Abs(cfg.RichDataDir)
			if err != nil {
				return nil, nil, err
			}
		}
		cfg.RichDataDir = filepath.Join(cfg.RichDataDir, netName(activeNetParams))
	}

	// Ensure the block database directory exists.
	if !fileExists(cfg.DataDir) {
		err := fmt.Errorf("loadConfig: the specified data directory [%v] does not exist", cfg.DataDir)
		return nil, nil, err
	}

	if cfg.DuneOutput != "" {
		if !filepath.IsAbs(cfg.DuneOutput) {
			cfg.DuneOutput, err = filepath.Abs(cfg.DuneOutput)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	return &cfg, remainingArgs, nil
}
