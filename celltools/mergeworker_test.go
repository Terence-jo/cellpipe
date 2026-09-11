package celltools

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/golang/geo/s2"
)

func TestNewAcc(t *testing.T) {
	ds := setUpRaster(t, TILED)
	band, err := NewBandContainer(ds, 0)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name       string
		cell       s2.CellID
		blocks     []BlockCoord
		doneBlocks []BlockCoord
	}{
		{
			"one block",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-1, 1)),
			[]BlockCoord{{0, 0}},
			[]BlockCoord{},
		},
		{
			"two blocks",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-16, 1)),
			[]BlockCoord{{0, 0}, {0, 1}},
			[]BlockCoord{},
		},
		{
			"four blocks",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-16, 16)),
			[]BlockCoord{{0, 0}, {0, 1}, {1, 0}, {1, 1}},
			[]BlockCoord{},
		},
		{
			"no blocks",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-100, 100)),
			[]BlockCoord{},
			[]BlockCoord{},
		},
		{
			"four blocks, one done",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-16, 16)),
			[]BlockCoord{{0, 1}, {1, 0}, {1, 1}},
			[]BlockCoord{{0, 0}},
		},
		{
			"four blocks, two done",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-16, 16)),
			[]BlockCoord{{0, 1}, {1, 0}},
			[]BlockCoord{{0, 0}, {1, 1}},
		},
		{
			"four blocks, all done",
			s2.CellIDFromLatLng(s2.LatLngFromDegrees(-16, 16)),
			[]BlockCoord{},
			[]BlockCoord{{0, 0}, {0, 1}, {1, 0}, {1, 1}},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			worker := newMergePool(band, 1, ConfigOpts{1, 1, 30, Mean, 4, false, false})[0]
			// add doneBlocks to the processed index ring
			for _, block := range tt.doneBlocks {
				worker.processedBlocks.addBlock(block)
			}

			// create the accumulator
			worker.newAcc(tt.cell)
			acc, ok := worker.accumulators[tt.cell]
			if !ok {
				t.Error("couldn't find accumulator for the cell")
			}
			gotBlocks := make([]BlockCoord, 0)
			for block := range acc.remaining {
				gotBlocks = append(gotBlocks, block)
			}
			blockCmp := func(a, b BlockCoord) int {
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
				if idxAccs[0].cellID != tt.cell {
					t.Error("found non-matching accumulator in the index")
				}
			}
		})
	}
}

func TestAccumulate(t *testing.T) {
	ds := setUpRaster(t, TILED)
	band, err := NewBandContainer(ds, 0)
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
			worker := newMergePool(band, 1, ConfigOpts{1, 1, 30, Mean, 4, false, false})[0]
			batch := CellBatch{tt.cell, []float64{1, 2, 3, 4}, BlockCoord{0, 0}, &sync.WaitGroup{}}

			batch.ack.Add(1)
			accumulateDone := make(chan struct{})
			go func() {
				batch.ack.Wait()
				close(accumulateDone)
			}()
			go worker.accumulate(batch)
			// Give the goroutine a brief moment to spin up and call batch.ack.Wait()
			time.Sleep(50 * time.Millisecond)
			select {
			case <-time.After(100 * time.Millisecond):
				batch.ack.Done()
				t.Fatal("accumulate did not acknowledge the batch")
			case <-accumulateDone:
			}
			want := []float64{1, 2, 3, 4}
			// flush will delete the accumulator until out is read
			if !tt.flushes && !slices.Equal(worker.accumulators[batch.ID].values, want) {
				t.Errorf("got %+v, wanted %+v", batch.Values, want)
			}

			if tt.flushes {
				// flush should be done after the wait above
				select {
				case aggVal := <-worker.out:
					want := 2.5
					if aggVal.Data != want {
						t.Errorf("got %1.f, wanted %1.f", aggVal.Data, want)
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
	wantBlock := BlockCoord{I: 0, J: 0}
	if dbr.hasBlock(wantBlock) {
		t.Error("Block ring should initialise with all-false slots")
	}
	dbr.addBlock(wantBlock)
	if !dbr.hasBlock(BlockCoord{I: 0, J: 0}) {
		t.Error("Once added, a block should remain registered in the ring")
	}

	advanceRing := func(ring *doneBlockRing, n int) {
		start := ring.watermark
		for j := range n {
			lpos := (start + j + 1)
			block := BlockCoord{I: lpos % ring.numXBlocks, J: lpos / ring.numXBlocks}
			ring.addBlock(block)
		}
	}

	// Advance by one whole ring, check that we have the correct watermark and correct range of false slots
	// The range satisfying (watermark - ringSize) < LPos <= (watermark - overlapRange) should be false
	dbr = newBlockRing(4, 2)
	dbr.addBlock(BlockCoord{0, 0})
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
		block := BlockCoord{I: lpos % dbr.numXBlocks, J: lpos / dbr.numXBlocks}
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
	lpos0 := BlockCoord{I: 0, J: 0}
	lpos4 := BlockCoord{I: 1, J: 1}
	lpos9 := BlockCoord{I: 2, J: 2}
	lpos14 := BlockCoord{I: 3, J: 3}
	dbr.addBlock(lpos0)
	dbr.addBlock(lpos4)
	dbr.addBlock(lpos9)
	dbr.addBlock(lpos14)

	// reserve blocks that should remain false
	lpos6 := BlockCoord{I: 3, J: 1}
	lpos8 := BlockCoord{I: 1, J: 2}
	lpos10 := BlockCoord{I: 3, J: 2}
	lpos13 := BlockCoord{I: 2, J: 3}

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
