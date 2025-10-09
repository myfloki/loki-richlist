// Copyright (c) 2024 The Flokicoin developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/flokiorg/go-flokicoin/chaincfg/chainhash"
	"github.com/flokiorg/go-flokicoin/database"
	"github.com/flokiorg/go-flokicoin/wire"
)

const (
	richListMetaVersion  = 1
	addressRecordVersion = 1

	activityTypeCredit uint8 = 0
	activityTypeDebit  uint8 = 1

	addressRecordSerializedSize = 116
)

var (
	richMetadataBucket = []byte("richmeta")
	richAddressBucket  = []byte("richaddr")
	richUtxoBucket     = []byte("richutxo")
)

var (
	metaVersionKey         = []byte("version")
	metaTipHeightKey       = []byte("tipheight")
	metaTipHashKey         = []byte("tiphash")
	metaTipTimestampKey    = []byte("tiptimestamp")
	metaTrackedBalanceKey  = []byte("trackedbalance")
	metaAddressCountKey    = []byte("addresscount")
	metaBlocksProcessedKey = []byte("blocksprocessed")
	metaTxProcessedKey     = []byte("txprocessed")
	metaOutputsTrackedKey  = []byte("outputstracked")
	metaOutputsSkippedKey  = []byte("outputsskipped")
	metaInputsTrackedKey   = []byte("inputstracked")
	metaInputsSkippedKey   = []byte("inputsskipped")
	metaCreatedAtKey       = []byte("createdat")
	metaLastUpdatedKey     = []byte("lastupdated")
)

type addressRecord struct {
	Version          uint16
	ScriptType       uint8
	LastActivityType uint8
	Balance          int64
	TotalReceived    uint64
	TotalSent        uint64
	ReceivedOutputs  uint64
	SentOutputs      uint64
	UnspentOutputs   uint64
	FirstSeenHeight  uint32
	LastSeenHeight   uint32
	LastSeenTime     int64
	LargestReceive   int64
	LargestSend      int64
	LastActivityTx   chainhash.Hash
}

type utxoEntry struct {
	Amount     int64
	ScriptType uint8
	Height     uint32
	Address    []byte
}

func serializeAddressRecord(record *addressRecord) []byte {
	buf := make([]byte, addressRecordSerializedSize)
	binary.LittleEndian.PutUint16(buf[0:], record.Version)
	buf[2] = record.ScriptType
	buf[3] = record.LastActivityType

	offset := 4
	binary.LittleEndian.PutUint64(buf[offset:], uint64(record.Balance))
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], record.TotalReceived)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], record.TotalSent)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], record.ReceivedOutputs)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], record.SentOutputs)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], record.UnspentOutputs)
	offset += 8
	binary.LittleEndian.PutUint32(buf[offset:], record.FirstSeenHeight)
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], record.LastSeenHeight)
	offset += 4
	binary.LittleEndian.PutUint64(buf[offset:], uint64(record.LastSeenTime))
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], uint64(record.LargestReceive))
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], uint64(record.LargestSend))
	offset += 8
	copy(buf[offset:], record.LastActivityTx[:])
	return buf
}

func deserializeAddressRecord(data []byte) (*addressRecord, error) {
	if len(data) != addressRecordSerializedSize {
		return nil, fmt.Errorf("address record has unexpected size %d", len(data))
	}

	record := &addressRecord{}
	record.Version = binary.LittleEndian.Uint16(data[0:])
	record.ScriptType = data[2]
	record.LastActivityType = data[3]
	if record.Version > addressRecordVersion {
		return nil, fmt.Errorf("address record version %d exceeds supported version %d", record.Version, addressRecordVersion)
	}

	offset := 4
	record.Balance = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	record.TotalReceived = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	record.TotalSent = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	record.ReceivedOutputs = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	record.SentOutputs = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	record.UnspentOutputs = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	record.FirstSeenHeight = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	record.LastSeenHeight = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	record.LastSeenTime = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	record.LargestReceive = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	record.LargestSend = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	copy(record.LastActivityTx[:], data[offset:])
	return record, nil
}

func loadAddressRecord(bucket database.Bucket, addrKey []byte, scriptType uint8, height int32, timestamp int64) (*addressRecord, bool, error) {
	raw := bucket.Get(addrKey)
	if raw == nil {
		record := &addressRecord{
			Version:          addressRecordVersion,
			ScriptType:       scriptType,
			LastActivityType: activityTypeCredit,
			Balance:          0,
			FirstSeenHeight:  uint32(height),
			LastSeenHeight:   uint32(height),
			LastSeenTime:     timestamp,
		}
		return record, true, nil
	}

	record, err := deserializeAddressRecord(raw)
	if err != nil {
		return nil, false, err
	}
	record.ScriptType = scriptType
	return record, false, nil
}

func saveAddressRecord(bucket database.Bucket, addrKey []byte, record *addressRecord) error {
	record.Version = addressRecordVersion
	return bucket.Put(addrKey, serializeAddressRecord(record))
}

func (r *addressRecord) applyCredit(amount int64, height int32, timestamp int64, txHash chainhash.Hash, scriptType uint8) {
	r.Balance += amount
	r.TotalReceived += uint64(amount)
	r.ReceivedOutputs++
	r.UnspentOutputs++
	r.LastSeenHeight = uint32(height)
	r.LastSeenTime = timestamp
	r.LastActivityType = activityTypeCredit
	if amount > r.LargestReceive {
		r.LargestReceive = amount
	}
	r.LastActivityTx = txHash
	r.ScriptType = scriptType
}

func (r *addressRecord) applyDebit(amount int64, height int32, timestamp int64, txHash chainhash.Hash) {
	r.Balance -= amount
	r.TotalSent += uint64(amount)
	r.SentOutputs++
	if r.UnspentOutputs > 0 {
		r.UnspentOutputs--
	}
	r.LastSeenHeight = uint32(height)
	r.LastSeenTime = timestamp
	r.LastActivityType = activityTypeDebit
	if amount > r.LargestSend {
		r.LargestSend = amount
	}
	r.LastActivityTx = txHash
}

func serializeUTXOEntry(entry *utxoEntry) []byte {
	addrLen := len(entry.Address)
	buf := make([]byte, 8+1+4+2+addrLen)
	offset := 0
	binary.LittleEndian.PutUint64(buf[offset:], uint64(entry.Amount))
	offset += 8
	buf[offset] = entry.ScriptType
	offset++
	binary.LittleEndian.PutUint32(buf[offset:], entry.Height)
	offset += 4
	binary.LittleEndian.PutUint16(buf[offset:], uint16(addrLen))
	offset += 2
	copy(buf[offset:], entry.Address)
	return buf
}

func deserializeUTXOEntry(data []byte) (*utxoEntry, error) {
	if len(data) < 8+1+4+2 {
		return nil, errors.New("utxo entry too short")
	}

	entry := &utxoEntry{}
	offset := 0
	entry.Amount = int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	entry.ScriptType = data[offset]
	offset++
	entry.Height = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	addrLen := int(binary.LittleEndian.Uint16(data[offset:]))
	offset += 2
	if len(data[offset:]) != addrLen {
		return nil, fmt.Errorf("utxo entry address length mismatch: want %d, got %d", addrLen, len(data[offset:]))
	}
	if addrLen > 0 {
		entry.Address = make([]byte, addrLen)
		copy(entry.Address, data[offset:])
	}
	return entry, nil
}

func encodeOutpointKey(op *wire.OutPoint) [36]byte {
	var key [36]byte
	copy(key[:], op.Hash[:])
	binary.LittleEndian.PutUint32(key[32:], op.Index)
	return key
}

func putUint32(bucket database.Bucket, key []byte, value uint32) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	return bucket.Put(key, buf[:])
}

func putInt32(bucket database.Bucket, key []byte, value int32) error {
	return putUint32(bucket, key, uint32(value))
}

func putUint64(bucket database.Bucket, key []byte, value uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	return bucket.Put(key, buf[:])
}

func putInt64(bucket database.Bucket, key []byte, value int64) error {
	return putUint64(bucket, key, uint64(value))
}

func fetchUint32(bucket database.Bucket, key []byte, def uint32) (uint32, error) {
	raw := bucket.Get(key)
	if raw == nil {
		return def, nil
	}
	if len(raw) != 4 {
		return 0, fmt.Errorf("invalid uint32 for key %q", key)
	}
	return binary.LittleEndian.Uint32(raw), nil
}

func fetchInt32(bucket database.Bucket, key []byte, def int32) (int32, error) {
	val, err := fetchUint32(bucket, key, uint32(def))
	if err != nil {
		return 0, err
	}
	return int32(val), nil
}

func fetchUint64(bucket database.Bucket, key []byte, def uint64) (uint64, error) {
	raw := bucket.Get(key)
	if raw == nil {
		return def, nil
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid uint64 for key %q", key)
	}
	return binary.LittleEndian.Uint64(raw), nil
}

func fetchInt64(bucket database.Bucket, key []byte, def int64) (int64, error) {
	val, err := fetchUint64(bucket, key, uint64(def))
	if err != nil {
		return 0, err
	}
	return int64(val), nil
}

func addUint64(bucket database.Bucket, key []byte, delta uint64) error {
	value, err := fetchUint64(bucket, key, 0)
	if err != nil {
		return err
	}
	return putUint64(bucket, key, value+delta)
}

func addInt64(bucket database.Bucket, key []byte, delta int64) error {
	value, err := fetchInt64(bucket, key, 0)
	if err != nil {
		return err
	}
	return putInt64(bucket, key, value+delta)
}

func updateMetaAfterBlock(bucket database.Bucket, height int32, hash chainhash.Hash, timestamp int64, stats blockStats) error {
	if err := putInt32(bucket, metaTipHeightKey, height); err != nil {
		return err
	}
	hashBytes := hash.CloneBytes()
	if err := bucket.Put(metaTipHashKey, hashBytes); err != nil {
		return err
	}
	if err := putInt64(bucket, metaTipTimestampKey, timestamp); err != nil {
		return err
	}
	if stats.trackedDelta != 0 {
		if err := addInt64(bucket, metaTrackedBalanceKey, stats.trackedDelta); err != nil {
			return err
		}
	}
	if stats.newAddresses > 0 {
		if err := addUint64(bucket, metaAddressCountKey, stats.newAddresses); err != nil {
			return err
		}
	}
	if err := addUint64(bucket, metaBlocksProcessedKey, 1); err != nil {
		return err
	}
	if stats.txCount > 0 {
		if err := addUint64(bucket, metaTxProcessedKey, stats.txCount); err != nil {
			return err
		}
	}
	if stats.outputsTracked > 0 {
		if err := addUint64(bucket, metaOutputsTrackedKey, stats.outputsTracked); err != nil {
			return err
		}
	}
	if stats.outputsSkipped > 0 {
		if err := addUint64(bucket, metaOutputsSkippedKey, stats.outputsSkipped); err != nil {
			return err
		}
	}
	if stats.inputsTracked > 0 {
		if err := addUint64(bucket, metaInputsTrackedKey, stats.inputsTracked); err != nil {
			return err
		}
	}
	if stats.inputsSkipped > 0 {
		if err := addUint64(bucket, metaInputsSkippedKey, stats.inputsSkipped); err != nil {
			return err
		}
	}
	return putInt64(bucket, metaLastUpdatedKey, time.Now().Unix())
}
