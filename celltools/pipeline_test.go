package celltools

import (
	"os"
	"slices"
	"testing"

	"github.com/Terence-jo/s2-tools/dggs"
	"github.com/Terence-jo/s2-tools/sources"
	"github.com/Terence-jo/s2-tools/types"

	"github.com/airbusgeo/godal"
	"github.com/golang/geo/s2"
)

type rasterConfig string

const (
	SIMPLE rasterConfig = "simple"
	TILED  rasterConfig = "tiled"
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
	band, err := sources.NewBand(ds, 0)
	if err != nil {
		t.Fatal(err)
	}

	indexer, err := dggs.NewS2Indexer(11)
	if err != nil {
		t.Fatal(err)
	}
	opts := Config{
		NumReadWorkers: 1,
		Verbose:        false,
	}
	pipeline := RasterIndexingPipeline{
		band,
		indexer,
		func(_ <-chan []IndexedCellData) error { return nil },
		Mean,
		opts,
	}
	dataCh := make(chan IndexedCellData)
	go func() {
		defer close(dataCh)
		cellsMap, err := pipeline.indexBlock(types.BlockCoord{I: 0, J: 0}, make(map[uint64]*cellBatch))
		if err != nil {
			return
		}
		for cell := range cellsMap {
			batch := cellsMap[cell]
			wkb, err := pipeline.Indexer.CellIDToWKB(cell)
			if err != nil {
				t.Error(err)
			}
			dataCh <- IndexedCellData{cell, pipeline.AggFunc.Apply(batch.values...), wkb}
		}
	}()
	var s2Data []IndexedCellData
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
	var want []IndexedCellData
	for i, cell := range cells {
		wkb, err := pipeline.Indexer.CellIDToWKB(uint64(cell))
		if err != nil {
			t.Error(err)
		}
		want = append(want, IndexedCellData{
			ID:   uint64(cell),
			Data: float64(i + 1),
			WKB:  wkb,
		})
	}

	// Compare the two
	cmpFunc := func(c1, c2 IndexedCellData) int {
		// do not simply use subtraction as these are uint64 values
		if c1.ID > c2.ID {
			return 1
		}
		if c1.ID == c2.ID {
			return 0
		}
		return -1
	}
	eqFunc := func(c1, c2 IndexedCellData) bool {
		// do not simply use subtraction as these are uint64 values
		if c1.ID != c2.ID {
			return false
		}
		if c1.Data != c2.Data {
			return false
		}
		return slices.Equal(c1.WKB, c2.WKB)
	}
	slices.SortFunc(want, cmpFunc)
	slices.SortFunc(s2Data, cmpFunc)
	if !slices.EqualFunc(s2Data, want, eqFunc) {
		t.Errorf("got %v, \nwant %v", s2Data, want)
	}
}

func TestExpectedBlocksForCell(t *testing.T) {
	// invariants:
	// 1. For a cell wholly within a block, that block is always in the expected set.
	// 2. Never under-includes blocks. For a given cell and set of blocks, once the exact extent of
	// the cell is calculated it will not overlap any blocks that are not in the expected set.
	raster := setUpRaster(t, TILED)
	band, err := sources.NewBand(raster, 0)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name           string
		cell           s2.CellID
		expectedBlocks []types.BlockCoord
	}{
		{
			"middle of first block",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-8.0, 8.0)).ID(),
			[]types.BlockCoord{{I: 0, J: 0}},
		},
		{
			"top-left corner",
			s2.CellFromLatLng(s2.LatLngFromDegrees(0.0, 0.0)).ID(),
			[]types.BlockCoord{{I: 0, J: 0}},
		},
		{
			"bottom-right corner",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-32.0, 32.0)).ID(),
			[]types.BlockCoord{{I: 1, J: 1}},
		},
		{
			"outside raster",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-31.0, 50.0)).ID(),
			[]types.BlockCoord{},
		},
		{
			"off-diagonal",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-32.1, 32.1)).ID(),
			[]types.BlockCoord{},
		},
		{
			"four-way diagonal hit",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-16.0, 16.0)).ID(),
			[]types.BlockCoord{{I: 0, J: 0}, {I: 0, J: 1}, {I: 1, J: 0}, {I: 1, J: 1}},
		},
		{
			"two-block horizontal hit",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-10.0, 16.0)).ID(),
			[]types.BlockCoord{{I: 0, J: 0}, {I: 1, J: 0}},
		},
		{
			"two-block vertical hit",
			s2.CellFromLatLng(s2.LatLngFromDegrees(-16.0, 10.0)).ID(),
			[]types.BlockCoord{{I: 0, J: 0}, {I: 0, J: 1}},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			bbox, err := dggs.S2Indexer{}.CellBBox(uint64(tt.cell))
			if err != nil {
				t.Fatal(err)
			}
			got := band.GetBlocksIntersectingBBox(bbox)
			want := tt.expectedBlocks
			missing := make([]types.BlockCoord, 0, len(want))
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
