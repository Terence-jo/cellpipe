package celltools

import (
	"slices"
	"testing"

	"github.com/Terence-jo/s2-tools/dggs"
	"github.com/Terence-jo/s2-tools/sources"
	"github.com/Terence-jo/s2-tools/types"

	"github.com/golang/geo/s2"
)

func TestNewAcc(t *testing.T) {
	ds := setUpRaster(t, TILED)
	band, err := sources.NewBand(ds, 0)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name       string
		cell       s2.CellID
		blocks     []types.BlockCoord
		doneBlocks []types.BlockCoord
	}{
		{
			"one block",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-1, 1)),
			[]types.BlockCoord{{I: 0, J: 0}},
			[]types.BlockCoord{},
		},
		{
			"two blocks",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-16, 1)),
			[]types.BlockCoord{{I: 0, J: 0}, {I: 0, J: 1}},
			[]types.BlockCoord{},
		},
		{
			"four blocks",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-16, 16)),
			[]types.BlockCoord{{I: 0, J: 0}, {I: 0, J: 1}, {I: 1, J: 0}, {I: 1, J: 1}},
			[]types.BlockCoord{},
		},
		{
			"no blocks",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-100, 100)),
			[]types.BlockCoord{},
			[]types.BlockCoord{},
		},
		{
			"four blocks, one done",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-16, 16)),
			[]types.BlockCoord{{I: 0, J: 1}, {I: 1, J: 0}, {I: 1, J: 1}},
			[]types.BlockCoord{{I: 0, J: 0}},
		},
		{
			"four blocks, two done",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-16, 16)),
			[]types.BlockCoord{{I: 0, J: 1}, {I: 1, J: 0}},
			[]types.BlockCoord{{I: 0, J: 0}, {I: 1, J: 1}},
		},
		{
			"four blocks, all done",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-16, 16)),
			[]types.BlockCoord{},
			[]types.BlockCoord{{I: 0, J: 0}, {I: 0, J: 1}, {I: 1, J: 0}, {I: 1, J: 1}},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			numXBlocks, _ := band.Structure.BlockCount()
			worker := newMergePool(numXBlocks, Mean, Config{NumReadWorkers: 1, NumMergeWorkers: 1, Verbose: false}, dggs.S2Indexer{}.SentinelCell())[0]
			// add doneBlocks to the processed index ring
			for _, block := range tt.doneBlocks {
				worker.processedBlocks.addBlock(block)
			}

			cellBBox, err := dggs.S2Indexer{}.CellBBox(uint64(tt.cell))
			if err != nil {
				t.Fatal(err)
			}
			expectedBlocks := band.GetBlocksIntersectingBBox(cellBBox)
			// create the accumulator
			worker.newAcc(uint64(tt.cell), expectedBlocks)
			acc, ok := worker.accumulators[uint64(tt.cell)]
			if !ok {
				t.Error("couldn't find accumulator for the cell")
			}
			if acc.numBlocksRemaining != len(tt.blocks) {
				t.Errorf("got %d blocks, expected %d", acc.numBlocksRemaining, len(expectedBlocks))
			}

			gotBlocks := make([]types.BlockCoord, 0)
			for block := range worker.reverseIndex {
				for _, workerAcc := range worker.reverseIndex[block] {
					if workerAcc.cellID == acc.cellID {
						gotBlocks = append(gotBlocks, block)
					}
				}
			}
			blockCmp := func(a, b types.BlockCoord) int {
				rowDiff := a.J - b.J
				if rowDiff != 0 {
					return rowDiff
				}
				return a.I - b.I
			}
			slices.SortFunc(gotBlocks, blockCmp)
			slices.SortFunc(tt.blocks, blockCmp)
			if !slices.Equal(gotBlocks, tt.blocks) {
				t.Errorf("got %+v, wanted %+v", gotBlocks, tt.blocks)
			}

			// make sure that each block in remaining has a reverse index entry
			for _, block := range gotBlocks {
				idxAccs, ok := worker.reverseIndex[block]
				if !ok {
					t.Errorf("couldn't find block %+v in the reverse index", block)
				}
				if idxAccs[0].cellID != uint64(tt.cell) {
					t.Error("found non-matching accumulator in the index")
				}
			}
		})
	}
}

func TestAccumulate(t *testing.T) {
	ds := setUpRaster(t, TILED)
	band, err := sources.NewBand(ds, 0)
	if err != nil {
		t.Fatal(err)
	}

	// modify table to configure multiple passes at accumulation. test no flush, immediate flush, flush after clearing blocks and a second pass
	accTests := []struct {
		name    string
		cell    s2.CellID
		flushes bool
	}{
		{
			"no flush",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-1, 1)),
			false,
		},
		{
			"immediate flush",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-100, 1)),
			true,
		},
	}
	for _, tt := range accTests {
		t.Run(tt.name, func(t *testing.T) {
			numXBlocks, _ := band.Structure.BlockCount()
			worker := newMergePool(numXBlocks, Mean, Config{NumReadWorkers: 1, NumMergeWorkers: 1, Verbose: false}, dggs.S2Indexer{}.SentinelCell())[0]
			batch := cellBatch{uint64(tt.cell), []float64{1, 2, 3, 4}, types.BlockCoord{I: 0, J: 0}}
			cellBBox, err := dggs.S2Indexer{}.CellBBox(uint64(tt.cell))
			if err != nil {
				t.Fatal(err)
			}
			expectedBlocks := band.GetBlocksIntersectingBBox(cellBBox)
			mergeBundle := cellMergeBundle{batch, expectedBlocks}

			worker.accumulate([]cellMergeBundle{mergeBundle})
			want := []float64{1, 2, 3, 4}
			if !tt.flushes && !slices.Equal(worker.accumulators[batch.id].values, want) {
				t.Errorf("got %+v, wanted %+v", batch.values, want)
			}

			if tt.flushes {
				// flush should be done after the wait above
				select {
				case aggVal := <-worker.out:
					want := 2.5
					if aggVal[0].Data != want {
						t.Errorf("got %1.f, wanted %1.f", aggVal[0].Data, want)
					}
				default:
					t.Fatal("accumulate did not flush")
				}
			}
		})
	}
}

func TestFlush(t *testing.T) {
	// must produce a single value in the out channel for an accumulator flushed
	// must delete accumulator from the mergeworker

	// what behaviour do we want if the accumulator isn't in the mergeWorker? error or flush anyway? start with error, relax only if necessary
}

// test onBlockDone

func TestDoneBlockRing(t *testing.T) {
	// This will initialise a ring with an overlap range of 6 blocks, active window of 8 blocks, frontierGaps of 2, leading to a ringSize of 20
	dbr := newBlockRing(4, 2)

	// When a block has not been added, hasBlock should return false, when a block had been added, hasBlock should return true
	wantBlock := types.BlockCoord{I: 0, J: 0}
	if dbr.hasBlock(wantBlock) {
		t.Error("Block ring should initialise with all-false slots")
	}
	dbr.addBlock(wantBlock)
	if !dbr.hasBlock(types.BlockCoord{I: 0, J: 0}) {
		t.Error("Once added, a block should remain registered in the ring")
	}

	advanceRing := func(ring *doneBlockRing, n int) {
		start := ring.watermark
		for j := range n {
			lpos := (start + j + 1)
			block := types.BlockCoord{I: lpos % ring.numXBlocks, J: lpos / ring.numXBlocks}
			ring.addBlock(block)
		}
	}

	// Advance by one whole ring, check that we have the correct watermark and correct range of false slots
	// The range satisfying (watermark - ringSize) < LPos <= (watermark - overlapRange) should be false
	dbr = newBlockRing(4, 2)
	dbr.addBlock(types.BlockCoord{I: 0, J: 0})
	oldWatermark := dbr.watermark
	wantWatermark := oldWatermark + dbr.ringSize

	advanceRing(dbr, dbr.ringSize)
	if dbr.watermark != wantWatermark {
		t.Fatalf("Watermark didn't advance properly: got %d, wanted %d", dbr.watermark, wantWatermark)
	}
	// After a full cycle of the ring, all slots have been true, but those ahead of or behind the active window should have been cleared when advancing the watermark.
	// Blocks behind the active window return false, blocks in the active window return true, and blocks ahead of the active window return false after a full cycle.
	start := (dbr.watermark - dbr.ringSize) + 1
	startLiveWindow := (dbr.watermark - dbr.activeWindow) + 1
	startFalseAbove := dbr.watermark + 1
	end := dbr.watermark + (dbr.ringSize - dbr.activeWindow)
	for lpos := start; lpos <= end; lpos++ {
		block := types.BlockCoord{I: lpos % dbr.numXBlocks, J: lpos / dbr.numXBlocks}
		ringHasBlock := dbr.hasBlock(block)
		if lpos < startLiveWindow && ringHasBlock {
			t.Errorf("Blocks behind window should return false. With watermark %d block %+v at lpos %d was still registered", dbr.watermark, block, lpos)
		}
		if startLiveWindow <= lpos && lpos < startFalseAbove && !ringHasBlock {
			t.Errorf("Blocks in active window should return true. With watermark %d block %+v at lpos %d was not registered", dbr.watermark, block, lpos)
		}
		if startFalseAbove <= lpos && lpos <= end && ringHasBlock {
			t.Errorf("Blocks behind active window should return false. With watermark %d block %+v at lpos %d was still registered", dbr.watermark, block, lpos)
		}
	}

	// Test out-of-order advancement
	dbr = newBlockRing(4, 2)
	lpos0 := types.BlockCoord{I: 0, J: 0}
	lpos4 := types.BlockCoord{I: 1, J: 1}
	lpos9 := types.BlockCoord{I: 2, J: 2}
	lpos14 := types.BlockCoord{I: 3, J: 3}
	dbr.addBlock(lpos0)
	dbr.addBlock(lpos4)
	dbr.addBlock(lpos9)
	dbr.addBlock(lpos14)

	// reserve blocks that should remain false
	lpos6 := types.BlockCoord{I: 3, J: 1}
	lpos8 := types.BlockCoord{I: 1, J: 2}
	lpos10 := types.BlockCoord{I: 3, J: 2}
	lpos13 := types.BlockCoord{I: 2, J: 3}

	wantWatermark = lpos14.J*4 + lpos14.I
	if dbr.watermark != wantWatermark {
		t.Errorf("Watermark advancement failed, got %d, wanted %d", dbr.watermark, wantWatermark)
	}
	// The active window will be ((numXBlocks + 2) + numWorkers + frontierGaps) -> 4 + 2 + 2 + 2 -> 10, so the start of the window will be LPos 5
	if dbr.hasBlock(lpos0) {
		t.Error("The start of the active window should be LPos 11, but LPos 0 was still registered")
	}
	if dbr.hasBlock(lpos4) {
		t.Error("The start of the active window should be LPos 11, but LPos 5 was still registered")
	}
	if !dbr.hasBlock(lpos9) {
		t.Error("The start of the active window should be LPos 11, but LPos 11 was not registered")
	}
	if !dbr.hasBlock(lpos14) {
		t.Error("The end of the active window should be LPos 20, but LPos 20 was not registered")
	}
	if dbr.hasBlock(lpos6) || dbr.hasBlock(lpos8) || dbr.hasBlock(lpos10) || dbr.hasBlock(lpos13) {
		t.Error("Blocks that were never added should not be registered")
	}

	// adding a block in the active window, but behind the watermark, should not affect the watermark
	dbr.addBlock(lpos13)
	if !dbr.hasBlock(lpos13) {
		t.Error("Adding a block behind the watermark should still register the block in the ring")
	}
	if dbr.watermark != wantWatermark {
		t.Error("Adding a block begind the watermark should not affect the watermark")
	}

	// if a block is behind the active window, it should not affect the watermark and it should not be added to the ring
	dbr.addBlock(lpos4)
	if dbr.watermark != wantWatermark {
		t.Error("Adding a block behind the active window should not affect the watermark")
	}
	if dbr.hasBlock(lpos4) {
		t.Error("Adding a block behind the active window should be a no-op")
	}
}
