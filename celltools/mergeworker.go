package celltools

import (
	"fmt"

	"github.com/golang/geo/s2"
	"github.com/sirupsen/logrus"
)

type cellAccumulator struct {
	cellID    s2.CellID
	values    []float64
	remaining map[BlockCoord]struct{} // set of blocks still expected by the cell
}

type mergeWorker struct {
	band            *BandContainer
	in              chan cellBatch
	out             chan S2CellData
	blockDone       chan BlockCoord
	aggFunc         AggFunc
	accumulators    map[s2.CellID]*cellAccumulator
	reverseIndex    map[BlockCoord][]*cellAccumulator
	processedBlocks *doneBlockRing
}

func newMergePool(band *BandContainer, numMergeWorkers int, opts ConfigOpts) []mergeWorker {
	numXBlocks, _ := band.Structure().BlockCount()
	mergeWorkers := make([]mergeWorker, numMergeWorkers)
	for i := range numMergeWorkers {
		mergeWorkers[i] = mergeWorker{
			band:            band,
			in:              make(chan cellBatch, cellChanBufferSize),
			out:             make(chan S2CellData, cellChanBufferSize),
			blockDone:       make(chan BlockCoord, numMergeWorkers),
			aggFunc:         opts.AggFunc,
			accumulators:    make(map[s2.CellID]*cellAccumulator),
			reverseIndex:    make(map[BlockCoord][]*cellAccumulator),
			processedBlocks: newBlockRing(numXBlocks, numMergeWorkers),
		}
	}
	return mergeWorkers
}

func (mw *mergeWorker) run() {
	defer close(mw.out)
	for mw.in != nil || mw.blockDone != nil {
		select {
		case batch, more := <-mw.in:
			if !more {
				mw.in = nil
				continue
			}
			mw.accumulate(batch)
		case block, more := <-mw.blockDone:
			if !more {
				mw.blockDone = nil
				continue
			}
			mw.onBlockDone(block)
		}
	}
	// Flush orphans
	if len(mw.accumulators) > 0 {
		logrus.Warn(fmt.Sprintf("orphans detected in merge worker accumulators, flushing %d orphan accumulators", len(mw.accumulators)))
	}
	for _, acc := range mw.accumulators {
		mw.flush(acc)
	}
}

func (mw *mergeWorker) newAcc(cell s2.CellID) {
	expectedBlocks := expectedBlocksForCell(cell, mw.band)
	acc := &cellAccumulator{
		cellID:    cell,
		remaining: make(map[BlockCoord]struct{}, len(expectedBlocks)),
	}
	for _, block := range expectedBlocks {
		if mw.processedBlocks.hasBlock(block) {
			continue
		}
		acc.remaining[block] = struct{}{}
		mw.reverseIndex[block] = append(mw.reverseIndex[block], acc)
	}
	mw.accumulators[acc.cellID] = acc
}

func (mw *mergeWorker) accumulate(batch cellBatch) {
	acc, ok := mw.accumulators[batch.id]
	if !ok {
		mw.newAcc(batch.id)
		acc = mw.accumulators[batch.id]
	}
	acc.values = append(acc.values, batch.values...)
	batch.ack.Done()
	if len(acc.remaining) == 0 {
		mw.flush(acc)
	}
}

func (mw *mergeWorker) onBlockDone(block BlockCoord) {
	mw.processedBlocks.addBlock(block)
	for _, acc := range mw.reverseIndex[block] {
		delete(acc.remaining, block)
		if len(acc.remaining) == 0 {
			mw.flush(acc)
		}
	}
	delete(mw.reverseIndex, block)
}

func (mw *mergeWorker) flush(acc *cellAccumulator) {
	finalValue := mw.aggFunc.apply(acc.values...)
	mw.out <- S2CellData{acc.cellID, finalValue, CellToWKB(s2.CellFromCellID(acc.cellID))}
	delete(mw.accumulators, acc.cellID)
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

func (dbr *doneBlockRing) addBlock(block BlockCoord) {
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

func (dbr *doneBlockRing) hasBlock(block BlockCoord) bool {
	linearPos := block.J*dbr.numXBlocks + block.I
	return dbr.blocks[linearPos%len(dbr.blocks)]
}
