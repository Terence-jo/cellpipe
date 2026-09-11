package celltools

import (
	"os"
	"slices"
	"testing"

	"github.com/airbusgeo/godal"
	"github.com/golang/geo/s2"
)

type rasterConfig string
const (
	SIMPLE rasterConfig = "simple"
	TILED rasterConfig = "tiled"
)

func TestPointToS2(t *testing.T) {
	// Create a point
	latLng := s2.LatLngFromDegrees(1.0, 2.0)

	// Create a S2 point
	s2Cell := s2.CellIDFromLatLng(latLng).Parent(11)

	// Compare the two
	desiredCell := s2.CellID(1154732675135700992)
	if s2Cell != desiredCell {
		t.Errorf("S2 cells are not equal, got %v, want %v", s2Cell, desiredCell)
	}
}

func TestRasterBlockToS2(t *testing.T) {
	ds := setUpRaster(t, SIMPLE)
	defer func() {
		if err := ds.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	band, err := NewBandContainer(ds, 0)
	if err != nil {
		t.Fatal(err)
	}

	opts := ConfigOpts{
		NumWorkers: 1,
		S2Lvl:      11,
		AggFunc:    Mean,
		MemLimit:   4,
	}
	dataCh := make(chan S2CellData)
	go func() {
		defer close(dataCh)
		cellsMap, err := rasterBlockToS2(band, band.Band.Structure().FirstBlock(), opts)
		if err != nil {
			return
		}
		aggCellResults(cellsMap, opts.AggFunc, dataCh)
	}()
	var s2Data []S2CellData
	for data := range dataCh {
		s2Data = append(s2Data, data)
	}

	// With GeoTransform [0, 1, 0, 0, 0, -1] and 2x2 pixels:
	// Pixel centers (using row+0.5, col+0.5):
	// row=0,col=0: lat=-0.5, lng=0.5 -> Cell: 1921624993079230464 (value 1)
	// row=0,col=1: lat=-0.5, lng=1.5 -> Cell: 1921234666451369984 (value 2)
	// row=1,col=0: lat=-1.5, lng=0.5 -> Cell: 1922065897241968640 (value 3)
	// row=1,col=1: lat=-1.5, lng=1.5 -> Cell: 1922684922288406528 (value 4)
	cells := []s2.CellID{
		s2.CellID(1921624993079230464),
		s2.CellID(1921234666451369984),
		s2.CellID(1922065897241968640),
		s2.CellID(1922684922288406528),
	}
	var want []S2CellData
	for i, cell := range cells {
		want = append(want, S2CellData{
			Cell:       cell,
			Data:       float64(i + 1),
			GeomString: cellToWKT(s2.CellFromCellID(cell)),
		})
	}

	// Compare the two
	cmpFunc := func(c1, c2 S2CellData) int {
		// do not simply use subtraction as these are uint64 values
		if c1.Cell > c2.Cell {
			return 1
		}
		if c1.Cell == c2.Cell {
			return 0
		}
		return -1
	}
	slices.SortFunc(want, cmpFunc)
	slices.SortFunc(s2Data, cmpFunc)
	if !slices.Equal(s2Data, want) {
		t.Errorf("got %v, \nwant %v", s2Data, want)
	}
}

func TestExpectedBlocksForCell(t *testing.T) {
	// invariants:
	// 1. For a cell wholly within a block, that block is always in the expected set.
	// 2. Never under-includes blocks. For a given cell and set of blocks, once the exact extent of
	// the cell is calculated it will not overlap any blocks that are not in the expected set.
	raster := setUpRaster(t, TILED)
	band, err := NewBandContainer(raster, 0)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct{
		name string
		cell s2.CellID
		expectedBlocks []BlockCoord
	}{
		{
			"middle of first block",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-8.0, 8.0)).ID(),
			[]BlockCoord{{0, 0}},
		},
		{
			"top-left corner",
			s2.CellFromLatLng(s2.LatLngFromDegrees(0.0, 0.0)).ID(),
			[]BlockCoord{{0, 0}},
		},
		{
			"bottom-right corner",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-32.0, 32.0)).ID(),
			[]BlockCoord{{1, 1}},
		},
		{
			"outside raster",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-31.0, 50.0)).ID(),
			[]BlockCoord{},
		},
		{
			"off-diagonal",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-32.1, 32.1)).ID(),
			[]BlockCoord{},
		},
		{
			"four-way diagonal hit",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-16.0, 16.0)).ID(),
			[]BlockCoord{{0, 0}, {0, 1}, {1, 0}, {1, 1}},
		},
		{
			"two-block horizontal hit",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-10.0, 16.0)).ID(),
			[]BlockCoord{{0, 0}, {1, 0}},
		},
		{
			"two-block vertical hit",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-16.0, 10.0)).ID(),
			[]BlockCoord{{0, 0}, {0, 1}},
		},

	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := expectedBlocksForCell(tt.cell, band)
			want := tt.expectedBlocks
			missing := make([]BlockCoord, 0, len(want))
			for _, block := range want {
				blockFound := false
				for _, foundBlock := range got {
					if foundBlock == block {
						blockFound = true
					}
				}
				if !blockFound {
					missing = append(missing, block)
				}
			}
			if len(missing) > 0 {
				t.Errorf("missing: %+v, wanted at least: %+v", missing, want)
			}
		})
	}
}

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
			lpos := (start + j+1)
			block := BlockCoord{ I: lpos % ring.numXBlocks, J: lpos / ring.numXBlocks }
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
		block := BlockCoord{ I: lpos % dbr.numXBlocks, J: lpos / dbr.numXBlocks }
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

	wantWatermark = lpos14.J * 4 + lpos14.I
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

func setUpRaster(t testing.TB, config rasterConfig) *godal.Dataset {
	godal.RegisterAll()
	t.Helper()

	tmpFile, _ := os.CreateTemp("", "")
	if err := tmpFile.Close(); err != nil {
		t.Fatal(err)
	}
	dsFile := tmpFile.Name()
	defer func() {
		err := os.Remove(dsFile)
		if err != nil {
			t.Fatal(err)
		}
	}()

	var rasterSize int
	switch config {
	case SIMPLE:
		rasterSize = 2
	case TILED:
		rasterSize = 32
	default:
		t.Fatal("invalid raster config")
	}

	// Create a raster
	ds, _ := godal.Create(
		godal.GTiff,
		dsFile,
		1,
		godal.Byte,
		rasterSize,
		rasterSize,
		godal.CreationOption("TILED=YES", "BLOCKXSIZE=16", "BLOCKYSIZE=16"),
	)
	if err := ds.SetGeoTransform([6]float64{0.0, 1.0, 0.0, 0.0, 0.0, -1.0}); err != nil {
		t.Fatal(err)
	}

	// fill band with random data
	buf := []byte{1, 2, 3, 4}
	bands := ds.Bands()

	if err := bands[0].Write(0, 0, buf, 2, 2); err != nil {
		t.Fatal(err)
	}
	return ds
}
