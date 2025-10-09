// Copyright (c) 2024 The Flokicoin developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package main

import (
	"container/heap"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/flokiorg/go-flokicoin/chaincfg"
	"github.com/flokiorg/go-flokicoin/chaincfg/chainhash"
	"github.com/flokiorg/go-flokicoin/chainutil"
	"github.com/flokiorg/go-flokicoin/database"
	flog "github.com/flokiorg/go-flokicoin/log"
	"github.com/flokiorg/go-flokicoin/txscript"
	"github.com/flokiorg/go-flokicoin/wire"
)

// blockStats summarises the results of processing a single block.
type blockStats struct {
	trackedDelta   int64
	newAddresses   uint64
	outputsTracked uint64
	outputsSkipped uint64
	inputsTracked  uint64
	inputsSkipped  uint64
	txCount        uint64
}

// metaSnapshot holds a cached copy of the rich list metadata state.
type metaSnapshot struct {
	Version         uint32
	TipHeight       int32
	TipHash         chainhash.Hash
	TipTimestamp    int64
	TrackedBalance  int64
	AddressCount    uint64
	BlocksProcessed uint64
	TxProcessed     uint64
	OutputsTracked  uint64
	OutputsSkipped  uint64
	InputsTracked   uint64
	InputsSkipped   uint64
	CreatedAt       int64
	LastUpdated     int64
}

func (s *metaSnapshot) applyBlock(height int32, hash chainhash.Hash, timestamp int64, stats blockStats) {
	s.TipHeight = height
	s.TipHash = hash
	s.TipTimestamp = timestamp
	s.TrackedBalance += stats.trackedDelta
	s.AddressCount += stats.newAddresses
	s.BlocksProcessed++
	s.TxProcessed += stats.txCount
	s.OutputsTracked += stats.outputsTracked
	s.OutputsSkipped += stats.outputsSkipped
	s.InputsTracked += stats.inputsTracked
	s.InputsSkipped += stats.inputsSkipped
	s.LastUpdated = time.Now().Unix()
}

type richItem struct {
	address string
	record  *addressRecord
}

type richMinHeap []*richItem

func (h richMinHeap) Len() int { return len(h) }

func (h richMinHeap) Less(i, j int) bool {
	return h[i].record.Balance < h[j].record.Balance
}

func (h richMinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *richMinHeap) Push(x interface{}) {
	item := x.(*richItem)
	*h = append(*h, item)
}

func (h *richMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (h richMinHeap) Items() []richItem {
	out := make([]richItem, len(h))
	for i, item := range h {
		out[i] = *item
	}
	return out
}

// richListIndexer drives the rich list indexing process.
type richListIndexer struct {
	sourceDB database.DB
	indexDB  database.DB
	params   *chaincfg.Params
	cfg      *config
	log      flog.Logger
	state    *metaSnapshot
}

func newRichListIndexer(sourceDB, indexDB database.DB, params *chaincfg.Params, cfg *config, log flog.Logger) *richListIndexer {
	return &richListIndexer{
		sourceDB: sourceDB,
		indexDB:  indexDB,
		params:   params,
		cfg:      cfg,
		log:      log,
	}
}

// Run performs the indexing process.
func (r *richListIndexer) Run(isInterrupted func() bool) error {
	if err := r.ensureBuckets(); err != nil {
		return err
	}

	state, err := r.loadMetaState()
	if err != nil {
		return err
	}
	r.state = state

	for {
		bestState, err := fetchBestChainState(r.sourceDB)
		if err != nil {
			return err
		}

		if err := r.validateResume(bestState); err != nil {
			return err
		}

		nextHeight := r.state.TipHeight + 1
		if nextHeight > bestState.height {
			// Already caught up. Refresh if more work appears later.
			if r.cfg.Progress > 0 {
				r.log.Infof("No new blocks to index (tip height %d)", r.state.TipHeight)
			}
			return nil
		}

		progressDeadline := time.Now().Add(time.Duration(r.cfg.Progress) * time.Second)

		for height := nextHeight; height <= bestState.height; height++ {
			if isInterrupted() {
				return fmt.Errorf("interrupted")
			}

			block, err := fetchBlockByHeight(r.sourceDB, height)
			if err != nil {
				return err
			}

			stats, err := r.processBlock(height, block)
			if err != nil {
				return err
			}

			blockTime := block.MsgBlock().Header.Timestamp.Unix()
			blockHash := *block.Hash()
			r.state.applyBlock(height, blockHash, blockTime, stats)

			if r.cfg.Progress > 0 && time.Now().After(progressDeadline) {
				remaining := bestState.height - height
				total := bestState.height
				percent := float64(height) / float64(total) * 100
				r.log.Infof("Indexed height %d/%d (%.2f%%), remaining %d blocks, tracked addresses %d, tracked balance %s",
					height, total, percent, remaining, r.state.AddressCount, chainutil.Amount(r.state.TrackedBalance).Format(chainutil.AmountFLC))
				progressDeadline = time.Now().Add(time.Duration(r.cfg.Progress) * time.Second)
			}
		}

		// Loop again to check for newly added blocks.
	}
}

func (r *richListIndexer) validateResume(best *chainBestState) error {
	if r.state.TipHeight < 0 {
		return nil
	}

	sourceHash, err := fetchBlockHashByHeight(r.sourceDB, r.state.TipHeight)
	if err != nil {
		return err
	}

	if !sourceHash.IsEqual(&r.state.TipHash) {
		return fmt.Errorf("rich list tip %s at height %d is not on the current best chain (%s) - reindex required",
			r.state.TipHash, r.state.TipHeight, sourceHash)
	}

	if r.state.TipHeight > best.height {
		return fmt.Errorf("rich list tip height %d is ahead of block database best height %d", r.state.TipHeight, best.height)
	}

	return nil
}

func (r *richListIndexer) processBlock(height int32, block *chainutil.Block) (blockStats, error) {
	stats := blockStats{}
	blockHash := *block.Hash()
	blockTime := block.MsgBlock().Header.Timestamp.Unix()

	err := r.indexDB.Update(func(dbTx database.Tx) error {
		metaBucket := dbTx.Metadata().Bucket(richMetadataBucket)
		addrBucket := dbTx.Metadata().Bucket(richAddressBucket)
		utxoBucket := dbTx.Metadata().Bucket(richUtxoBucket)
		if metaBucket == nil || addrBucket == nil || utxoBucket == nil {
			return fmt.Errorf("rich list buckets missing (meta=%v, addr=%v, utxo=%v)",
				metaBucket != nil, addrBucket != nil, utxoBucket != nil)
		}

		for txIndex, wrappedTx := range block.Transactions() {
			msgTx := wrappedTx.MsgTx()
			txHash := msgTx.TxHash()
			stats.txCount++

			if txIndex != 0 {
				for _, txIn := range msgTx.TxIn {
					key := encodeOutpointKey(&txIn.PreviousOutPoint)
					raw := utxoBucket.Get(key[:])
					if raw == nil {
						stats.inputsSkipped++
						continue
					}

					entry, err := deserializeUTXOEntry(raw)
					if err != nil {
						return err
					}
					if err := utxoBucket.Delete(key[:]); err != nil {
						return err
					}

					if len(entry.Address) == 0 {
						stats.inputsSkipped++
						continue
					}

					record, _, err := loadAddressRecord(addrBucket, entry.Address, entry.ScriptType, height, blockTime)
					if err != nil {
						return err
					}

					record.applyDebit(entry.Amount, height, blockTime, txHash)
					if err := saveAddressRecord(addrBucket, entry.Address, record); err != nil {
						return err
					}

					stats.inputsTracked++
					stats.trackedDelta -= entry.Amount
				}
			}

			for outIdx, txOut := range msgTx.TxOut {
				if txOut.Value <= 0 {
					stats.outputsSkipped++
					continue
				}

				outpoint := wire.OutPoint{
					Hash:  txHash,
					Index: uint32(outIdx),
				}

				key := encodeOutpointKey(&outpoint)
				scriptClass, addrs, _, err := txscript.ExtractPkScriptAddrs(txOut.PkScript, r.params)
				if err != nil || len(addrs) != 1 {
					entry := &utxoEntry{
						Amount:     txOut.Value,
						ScriptType: uint8(scriptClass),
						Height:     uint32(height),
						Address:    nil,
					}
					if err := utxoBucket.Put(key[:], serializeUTXOEntry(entry)); err != nil {
						return err
					}
					stats.outputsSkipped++
					continue
				}

				addrKey := []byte(addrs[0].EncodeAddress())
				entry := &utxoEntry{
					Amount:     txOut.Value,
					ScriptType: uint8(scriptClass),
					Height:     uint32(height),
					Address:    addrKey,
				}
				if err := utxoBucket.Put(key[:], serializeUTXOEntry(entry)); err != nil {
					return err
				}

				record, created, err := loadAddressRecord(addrBucket, addrKey, uint8(scriptClass), height, blockTime)
				if err != nil {
					return err
				}
				if created {
					stats.newAddresses++
				}

				record.applyCredit(txOut.Value, height, blockTime, txHash, uint8(scriptClass))
				if err := saveAddressRecord(addrBucket, addrKey, record); err != nil {
					return err
				}

				stats.outputsTracked++
				stats.trackedDelta += txOut.Value
			}
		}

		if err := updateMetaAfterBlock(metaBucket, height, blockHash, blockTime, stats); err != nil {
			return err
		}

		return nil
	})

	return stats, err
}

func (r *richListIndexer) ensureBuckets() error {
	return r.indexDB.Update(func(dbTx database.Tx) error {
		meta := dbTx.Metadata()
		metaBucket, err := meta.CreateBucketIfNotExists(richMetadataBucket)
		if err != nil {
			return err
		}
		if _, err := meta.CreateBucketIfNotExists(richAddressBucket); err != nil {
			return err
		}
		if _, err := meta.CreateBucketIfNotExists(richUtxoBucket); err != nil {
			return err
		}

		if metaBucket.Get(metaVersionKey) != nil {
			return nil
		}

		// Fresh initialization.
		if err := putUint32(metaBucket, metaVersionKey, richListMetaVersion); err != nil {
			return err
		}
		if err := putInt32(metaBucket, metaTipHeightKey, -1); err != nil {
			return err
		}
		var zero chainhash.Hash
		if err := metaBucket.Put(metaTipHashKey, zero.CloneBytes()); err != nil {
			return err
		}
		now := time.Now().Unix()
		initials := []func(database.Bucket) error{
			func(b database.Bucket) error { return putInt64(b, metaTipTimestampKey, 0) },
			func(b database.Bucket) error { return putInt64(b, metaTrackedBalanceKey, 0) },
			func(b database.Bucket) error { return putUint64(b, metaAddressCountKey, 0) },
			func(b database.Bucket) error { return putUint64(b, metaBlocksProcessedKey, 0) },
			func(b database.Bucket) error { return putUint64(b, metaTxProcessedKey, 0) },
			func(b database.Bucket) error { return putUint64(b, metaOutputsTrackedKey, 0) },
			func(b database.Bucket) error { return putUint64(b, metaOutputsSkippedKey, 0) },
			func(b database.Bucket) error { return putUint64(b, metaInputsTrackedKey, 0) },
			func(b database.Bucket) error { return putUint64(b, metaInputsSkippedKey, 0) },
			func(b database.Bucket) error { return putInt64(b, metaCreatedAtKey, now) },
			func(b database.Bucket) error { return putInt64(b, metaLastUpdatedKey, now) },
		}
		for _, set := range initials {
			if err := set(metaBucket); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *richListIndexer) loadMetaState() (*metaSnapshot, error) {
	var snapshot metaSnapshot
	err := r.indexDB.View(func(dbTx database.Tx) error {
		metaBucket := dbTx.Metadata().Bucket(richMetadataBucket)
		if metaBucket == nil {
			return fmt.Errorf("metadata bucket missing")
		}

		var err error
		snapshot.Version, err = fetchUint32(metaBucket, metaVersionKey, richListMetaVersion)
		if err != nil {
			return err
		}
		if snapshot.Version > richListMetaVersion {
			return fmt.Errorf("rich list metadata version %d exceeds supported version %d", snapshot.Version, richListMetaVersion)
		}
		snapshot.TipHeight, err = fetchInt32(metaBucket, metaTipHeightKey, -1)
		if err != nil {
			return err
		}
		copy(snapshot.TipHash[:], metaBucket.Get(metaTipHashKey))
		snapshot.TipTimestamp, err = fetchInt64(metaBucket, metaTipTimestampKey, 0)
		if err != nil {
			return err
		}
		snapshot.TrackedBalance, err = fetchInt64(metaBucket, metaTrackedBalanceKey, 0)
		if err != nil {
			return err
		}
		snapshot.AddressCount, err = fetchUint64(metaBucket, metaAddressCountKey, 0)
		if err != nil {
			return err
		}
		snapshot.BlocksProcessed, err = fetchUint64(metaBucket, metaBlocksProcessedKey, 0)
		if err != nil {
			return err
		}
		snapshot.TxProcessed, err = fetchUint64(metaBucket, metaTxProcessedKey, 0)
		if err != nil {
			return err
		}
		snapshot.OutputsTracked, err = fetchUint64(metaBucket, metaOutputsTrackedKey, 0)
		if err != nil {
			return err
		}
		snapshot.OutputsSkipped, err = fetchUint64(metaBucket, metaOutputsSkippedKey, 0)
		if err != nil {
			return err
		}
		snapshot.InputsTracked, err = fetchUint64(metaBucket, metaInputsTrackedKey, 0)
		if err != nil {
			return err
		}
		snapshot.InputsSkipped, err = fetchUint64(metaBucket, metaInputsSkippedKey, 0)
		if err != nil {
			return err
		}
		snapshot.CreatedAt, err = fetchInt64(metaBucket, metaCreatedAtKey, time.Now().Unix())
		if err != nil {
			return err
		}
		snapshot.LastUpdated, err = fetchInt64(metaBucket, metaLastUpdatedKey, snapshot.CreatedAt)
		if err != nil {
			return err
		}
		return nil
	})
	return &snapshot, err
}

func (r *richListIndexer) collectTopAddresses(limit int) ([]richItem, int64, error) {
	if limit <= 0 {
		return nil, 0, nil
	}

	h := make(richMinHeap, 0, limit)
	heap.Init(&h)

	var positiveBalanceSum int64
	err := r.indexDB.View(func(dbTx database.Tx) error {
		addrBucket := dbTx.Metadata().Bucket(richAddressBucket)
		if addrBucket == nil {
			return fmt.Errorf("address bucket missing")
		}

		cursor := addrBucket.Cursor()
		for ok := cursor.First(); ok; ok = cursor.Next() {
			key := cursor.Key()
			value := cursor.Value()
			if len(value) == 0 {
				continue
			}
			record, err := deserializeAddressRecord(value)
			if err != nil {
				return err
			}
			if record.Balance <= 0 {
				continue
			}
			positiveBalanceSum += record.Balance
			item := &richItem{
				address: string(key),
				record:  record,
			}
			if h.Len() < limit {
				heap.Push(&h, item)
				continue
			}
			if record.Balance > h[0].record.Balance {
				heap.Pop(&h)
				heap.Push(&h, item)
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	entries := h.Items()
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].record.Balance == entries[j].record.Balance {
			return entries[i].address < entries[j].address
		}
		return entries[i].record.Balance > entries[j].record.Balance
	})
	return entries, positiveBalanceSum, nil
}

func (r *richListIndexer) RenderSummary() error {
	summary, err := r.loadMetaState()
	if err != nil {
		return err
	}

	entries, positiveBalance, err := r.collectTopAddresses(r.cfg.Limit)
	if err != nil {
		return err
	}

	totalBalance := summary.TrackedBalance
	if totalBalance <= 0 {
		totalBalance = positiveBalance
	}

	fmt.Println()
	fmt.Printf("Rich List Summary (%s network)\n", r.params.Name)
	fmt.Println(strings.Repeat("-", 96))
	if summary.TipHeight >= 0 {
		fmt.Printf("Tip height:        %d\n", summary.TipHeight)
		fmt.Printf("Tip hash:          %s\n", summary.TipHash.String())
	} else {
		fmt.Printf("Tip height:        (not indexed)\n")
		fmt.Printf("Tip hash:          n/a\n")
	}
	fmt.Printf("Tracked balance:   %s\n", formatAmount(summary.TrackedBalance))
	fmt.Printf("Positive balances: %s\n", formatAmount(positiveBalance))
	fmt.Printf("Tracked addresses: %d\n", summary.AddressCount)
	fmt.Printf("Blocks processed:  %d   Transactions: %d\n", summary.BlocksProcessed, summary.TxProcessed)
	fmt.Printf("Outputs tracked:   %d   skipped: %d\n", summary.OutputsTracked, summary.OutputsSkipped)
	fmt.Printf("Inputs tracked:    %d   skipped: %d\n", summary.InputsTracked, summary.InputsSkipped)
	fmt.Printf("Last updated:      %s\n", formatDetailedTime(summary.LastUpdated))
	fmt.Println()

	if len(entries) == 0 {
		fmt.Println("No positive-balance addresses found.")
		return nil
	}

	fmt.Printf("Top %d Addresses (by tracked balance)\n", len(entries))
	fmt.Printf("%-4s %-44s %-6s %18s %7s %7s %18s %18s %7s %7s %6s %10s %11s %7s\n",
		"Rank", "Address", "Type", "Balance", "%", "Cum%", "Received", "Sent", "RecvTx", "SentTx", "UTXO", "LastHeight", "LastSeen", "LastAct")

	var cumulative int64
	for i, entry := range entries {
		record := entry.record
		cumulative += record.Balance
		share := percentage(record.Balance, totalBalance)
		cumulativeShare := percentage(cumulative, totalBalance)
		lastHeight := int64(record.LastSeenHeight)
		lastSeen := formatTimestamp(record.LastSeenTime)
		fmt.Printf("%4d %-44s %-6s %18s %6.2f%% %6.2f%% %18s %18s %7d %7d %6d %10d %11s %7s\n",
			i+1,
			entry.address,
			txscript.ScriptClass(record.ScriptType).String(),
			formatAmount(record.Balance),
			share,
			cumulativeShare,
			formatAmountFromUint(record.TotalReceived),
			formatAmountFromUint(record.TotalSent),
			record.ReceivedOutputs,
			record.SentOutputs,
			record.UnspentOutputs,
			lastHeight,
			lastSeen,
			activityLabel(record.LastActivityType),
		)
	}

	fmt.Println()
	fmt.Println("Note: tracked balances exclude script types that cannot be mapped to a single address.")

	if r.cfg.DuneOutput != "" {
		if err := r.exportDune(entries); err != nil {
			return err
		}
		fmt.Printf("Dune CSV written to %s\n", r.cfg.DuneOutput)
	}

	return nil
}

func (r *richListIndexer) exportDune(entries []richItem) error {
	if len(entries) == 0 {
		return nil
	}

	outPath := r.cfg.DuneOutput
	if outPath == "" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return fmt.Errorf("failed to create dune output directory: %w", err)
	}

	file, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("failed to create dune output file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"rank", "address", "balance_flc", "last_seen_utc"}); err != nil {
		return err
	}

	for i, entry := range entries {
		record := entry.record
		lastSeen := ""
		if record.LastSeenTime > 0 {
			lastSeen = time.Unix(record.LastSeenTime, 0).UTC().Format(time.RFC3339)
		}
		row := []string{
			fmt.Sprintf("%d", i+1),
			entry.address,
			fmt.Sprintf("%.8f", chainutil.Amount(record.Balance).ToFLC()),
			lastSeen,
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	writer.Flush()
	return writer.Error()
}

func formatAmount(value int64) string {
	return chainutil.Amount(value).Format(chainutil.AmountFLC)
}

func formatAmountFromUint(value uint64) string {
	if value > math.MaxInt64 {
		return fmt.Sprintf("%d Loki", value)
	}
	return formatAmount(int64(value))
}

func percentage(value int64, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return (float64(value) / float64(total)) * 100
}

func formatTimestamp(timestamp int64) string {
	if timestamp <= 0 {
		return "n/a"
	}
	return time.Unix(timestamp, 0).UTC().Format("2006-01-02")
}

func formatDetailedTime(timestamp int64) string {
	if timestamp <= 0 {
		return "n/a"
	}
	return time.Unix(timestamp, 0).UTC().Format(time.RFC3339)
}

func activityLabel(activity uint8) string {
	switch activity {
	case activityTypeCredit:
		return "recv"
	case activityTypeDebit:
		return "send"
	default:
		return "n/a"
	}
}
