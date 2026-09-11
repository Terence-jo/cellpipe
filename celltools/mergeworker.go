package celltools

import (
	"fmt"

	"github.com/Terence-jo/s2-tools/geotiff"

	"github.com/sirupsen/logrus"
)

type cellMergeBundle struct {
	batch          cellBatch
	expectedBlocks []geotiff.BlockCoord
}

type cellAccumulator struct {
	cellID             uint64
	values             []float64
	numBlocksRemaining int // set of blocks still expected to contribute to the cell
}

type mergeWorker struct {
	in              chan []cellMergeBundle
	out             chan []IndexedCellData
	sentinelValue   uint64
	aggFunc         AggFunc
	accumulators    map[uint64]*cellAccumulator
	reverseIndex    map[geotiff.BlockCoord][]*cellAccumulator
	processedBlocks *doneBlockRing
}

func newMergePool(numXBlocks int, aggFunc AggFunc, opts Config, sentinelValue uint64) []mergeWorker {
	mergeWorkers := make([]mergeWorker, opts.NumMergeWorkers)
	for i := range opts.NumMergeWorkers {
		mergeWorkers[i] = mergeWorker{
			in:              make(chan []cellMergeBundle, cellChanBufferSize),
			out:             make(chan []IndexedCellData, cellChanBufferSize),
			sentinelValue:   sentinelValue,
			aggFunc:         aggFunc,
			accumulators:    make(map[uint64]*cellAccumulator),
			reverseIndex:    make(map[geotiff.BlockCoord][]*cellAccumulator),
			processedBlocks: newBlockRing(numXBlocks, opts.NumMergeWorkers),
		}
	}
	return mergeWorkers
}

func (mw *mergeWorker) run() {
	defer close(mw.out)
	for pack := range mw.in {
		mw.accumulate(pack)
	}
	// for mw.in != nil || mw.blockDone != nil {
	// 	select {
	// 	case pack, more := <-mw.in:
	// 		if !more {
	// 			mw.in = nil
	// 			continue
	// 		}
	// 		mw.accumulate(pack)
	// 	case block, more := <-mw.blockDone:
	// 		if !more {
	// 			mw.blockDone = nil
	// 			continue
	// 		}
	// 		mw.onBlockDone(block)
	// 	}
	// }
	// Flush orphans
	if len(mw.accumulators) > 0 {
		logrus.Warn(fmt.Sprintf("orphans detected in merge worker accumulators, flushing %d orphan accumulators", len(mw.accumulators)))
	}
	flushGroup := make([]*cellAccumulator, 0, len(mw.accumulators))
	for _, acc := range mw.accumulators {
		flushGroup = append(flushGroup, acc)
	}
	mw.flush(flushGroup)
}

func (mw *mergeWorker) newAcc(cell uint64, expectedBlocks []geotiff.BlockCoord) {
	acc := &cellAccumulator{
		cellID:             cell,
		numBlocksRemaining: 0,
	}
	for _, block := range expectedBlocks {
		if mw.processedBlocks.hasBlock(block) {
			continue
		}
		acc.numBlocksRemaining++
		mw.reverseIndex[block] = append(mw.reverseIndex[block], acc)
	}
	mw.accumulators[acc.cellID] = acc
}

func (mw *mergeWorker) accumulate(pack []cellMergeBundle) {
	// some accumulators in the pack may be ready to flush; gather them
	flushGroup := make([]*cellAccumulator, 0, len(pack))
	for _, bundle := range pack {
		if bundle.batch.id == mw.sentinelValue {
			mw.onBlockDone(bundle.batch.block)
			continue
		}
		acc, ok := mw.accumulators[bundle.batch.id]
		if !ok {
			mw.newAcc(bundle.batch.id, bundle.expectedBlocks)
			acc = mw.accumulators[bundle.batch.id]
		}
		acc.values = append(acc.values, bundle.batch.values...)
		if acc.numBlocksRemaining == 0 {
			flushGroup = append(flushGroup, acc)
		}
	}
	if len(flushGroup) > 0 {
		mw.flush(flushGroup)
	}
}

func (mw *mergeWorker) onBlockDone(block geotiff.BlockCoord) {
	mw.processedBlocks.addBlock(block)
	flushGroup := make([]*cellAccumulator, 0, chanSendPackSize)
	for _, acc := range mw.reverseIndex[block] {
		acc.numBlocksRemaining--
		// keep this as strict equality and monitor for increased orphan rates
		if acc.numBlocksRemaining == 0 {
			flushGroup = append(flushGroup, acc)
		}
		if len(flushGroup) >= chanSendPackSize {
			mw.flush(flushGroup)
			flushGroup = flushGroup[:0]
		}
	}
	if len(flushGroup) > 0 {
		mw.flush(flushGroup)
	}
	delete(mw.reverseIndex, block)
}

func (mw *mergeWorker) flush(accs []*cellAccumulator) {
	pack := make([]IndexedCellData, 0, len(accs))
	for _, acc := range accs {
		finalValue := mw.aggFunc.Apply(acc.values...)
		// mergeWorker doesn't know how to create WKB for a cell, so it defers
		pack = append(pack, IndexedCellData{acc.cellID, finalValue, []byte{}})
		delete(mw.accumulators, acc.cellID)
	}
	mw.out <- pack
}

// Linear index-ring for tracking done blocks and the high watermark
type doneBlockRing struct {
	numXBlocks   int
	watermark    int
	activeWindow int
	ringSize     int
	blocks       []bool
}

func newBlockRing(numXBlocks int, numWorkers int) *doneBlockRing {
	// +2 to account for a top-left corner cell overlapping block IJ - numXBlocks - 1: the diagonally adjacent cell in the previous row.
	overlapRange := numXBlocks + 2
	// frontierGaps is an assumption about how contiguous the frontier of blocks in active processing will be
	frontierGaps := numWorkers / 2
	// slots outside the active window will be cleared. If too many slow blocks are observed coming in late and not triggering flushes, tune frontierGaps
	// to catch more before clearing. Increasing frontierGaps is a trade-off between increasing known memory usage, and mitigating unanticipated
	// memory usage from orphan cellBatches.
	activeWindow := overlapRange + numWorkers + frontierGaps
	ringSize := activeWindow * 2
	return &doneBlockRing{
		numXBlocks,
		-1,
		activeWindow,
		ringSize,
		// initlialise full ring with zero-value (false)
		make([]bool, ringSize),
	}
}

func (dbr *doneBlockRing) addBlock(block geotiff.BlockCoord) {
	linearPos := block.J*dbr.numXBlocks + block.I
	// if the block is behind the active window, do not add it
	if linearPos <= dbr.watermark-dbr.activeWindow {
		return
	}
	if linearPos > dbr.watermark {
		dbr.watermark = linearPos

		start := max((dbr.watermark-dbr.ringSize)+1, 0)
		for toClear := start; toClear <= (dbr.watermark - dbr.activeWindow); toClear++ {
			dbr.blocks[toClear%dbr.ringSize] = false
		}
	}
	dbr.blocks[linearPos%dbr.ringSize] = true
}

func (dbr *doneBlockRing) hasBlock(block geotiff.BlockCoord) bool {
	linearPos := block.J*dbr.numXBlocks + block.I
	return dbr.blocks[linearPos%len(dbr.blocks)]
}

func cellWorkerIndex(cellID uint64, n int) int {
	// FNV-1a 64-bit
	const (
		offset64 uint64 = 14695981039346656037
		prime64  uint64 = 1099511628211
	)
	h := offset64
	v := cellID
	for range 8 {
		h ^= v & 0xff
		h *= prime64
		v >>= 8
	}
	return int(h % uint64(n))
}
