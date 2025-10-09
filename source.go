// Copyright (c) 2024 The Flokicoin developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"encoding/binary"
	"fmt"

	"github.com/flokiorg/go-flokicoin/chaincfg/chainhash"
	"github.com/flokiorg/go-flokicoin/chainutil"
	"github.com/flokiorg/go-flokicoin/database"
)

var (
	heightIndexBucketName = []byte("heightidx")
	chainStateKeyName     = []byte("chainstate")
)

type chainBestState struct {
	hash   chainhash.Hash
	height int32
}

func fetchBestChainState(db database.DB) (*chainBestState, error) {
	var state chainBestState
	err := db.View(func(dbTx database.Tx) error {
		serialized := dbTx.Metadata().Get(chainStateKeyName)
		if serialized == nil {
			return fmt.Errorf("chain state not found")
		}
		if len(serialized) < chainhash.HashSize+4 {
			return fmt.Errorf("chain state corrupted")
		}
		copy(state.hash[:], serialized[:chainhash.HashSize])
		state.height = int32(binary.LittleEndian.Uint32(serialized[chainhash.HashSize:]))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func fetchBlockHashByHeight(db database.DB, height int32) (*chainhash.Hash, error) {
	var hash chainhash.Hash
	err := db.View(func(dbTx database.Tx) error {
		bucket := dbTx.Metadata().Bucket(heightIndexBucketName)
		if bucket == nil {
			return fmt.Errorf("height index bucket not found")
		}
		var key [4]byte
		binary.LittleEndian.PutUint32(key[:], uint32(height))
		hashBytes := bucket.Get(key[:])
		if hashBytes == nil {
			return fmt.Errorf("no block at height %d", height)
		}
		copy(hash[:], hashBytes)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &hash, nil
}

func fetchBlockByHeight(db database.DB, height int32) (*chainutil.Block, error) {
	var blockBytes []byte
	err := db.View(func(dbTx database.Tx) error {
		bucket := dbTx.Metadata().Bucket(heightIndexBucketName)
		if bucket == nil {
			return fmt.Errorf("height index bucket not found")
		}
		var key [4]byte
		binary.LittleEndian.PutUint32(key[:], uint32(height))
		hashBytes := bucket.Get(key[:])
		if hashBytes == nil {
			return fmt.Errorf("no block at height %d", height)
		}
		var hash chainhash.Hash
		copy(hash[:], hashBytes)
		raw, err := dbTx.FetchBlock(&hash)
		if err != nil {
			return err
		}
		blockBytes = append([]byte(nil), raw...)
		return nil
	})
	if err != nil {
		return nil, err
	}

	block, err := chainutil.NewBlockFromBytes(blockBytes)
	if err != nil {
		return nil, err
	}
	block.SetHeight(height)
	return block, nil
}
