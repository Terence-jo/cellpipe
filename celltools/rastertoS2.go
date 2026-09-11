package celltools

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/airbusgeo/godal"
	"github.com/golang/geo/s2"
	"github.com/sirupsen/logrus"
)

const (
	earthRadius  float64 = 6371000
	cellWKBSize  int     = 1 + 4 + 4 + 4 + 5*16
	cellDataSize int     = cellWKBSize + 16
	cellChanBufferSize int = 100 // testing found ~100 allowed saturation of workers
)

type ConfigOpts struct {
	NumReadWorkers  int
	NumMergeWorkers int
	S2Lvl           int
	AggFunc         AggFunc
	Verbose         bool
}

// BandContainer is a thin wrapper over a godal.Band, including a mutex for concurrent readers, and the GeoTransform, which is
// otherwise only available at the scope of the godal.Dataset. It exposes some convenience functions for handling the transform.
type BandContainer struct {
	*sync.Mutex
	godal.Band
	geoTransform [6]float64
}

// Origin retrieves the origin of the raster from the GeoTransform, usually the top-left
func (b *BandContainer) Origin() Point {
	return Point{X: b.geoTransform[0], Y: b.geoTransform[3]}
}

// Resolution returns an xRes, yRes tuple derived from the GeoTransform. yRes is commonly negative.
func (b *BandContainer) Resolution() (float64, float64) {
	xRes := b.geoTransform[1]
	yRes := b.geoTransform[5]
	return xRes, yRes
}

func NewBandContainer(ds *godal.Dataset, bandIdx int) (*BandContainer, error) {
	gt, err := ds.GeoTransform()
	if err != nil {
		return nil, err
	}
	band := ds.Bands()[bandIdx]
	return &BandContainer{&sync.Mutex{}, band, gt}, nil

}

type Point struct {
	X float64
	Y float64
}

type BlockCoord struct{ I, J int }

// cellBatch is an internal implementation detail of the ProcessBlocks step in the pipeline. It is exclusively for transport of cell values from
// rasterBlockToS2() to a mergeWorker
type cellBatch struct {
	id     s2.CellID
	values []float64
	block  BlockCoord
	ack    *sync.WaitGroup
}

// S2CellData is the output type of this pipeline. It describes a single S2 cell with a single value. The WKB included allows writing of valid
// GeoParquet directly from pipeline outputs.
type S2CellData struct {
	Cell s2.CellID
	Data float64
	WKB  []byte
}

func (c S2CellData) String() string {
	return fmt.Sprintf("%v;%v;%s", int64(c.Cell), c.Data, c.WKB)
}

// RasterToS2 contains the whole pipeline, from a raster path input, to a sink callable ready to consume the output channel. path should be a relative or
// absolute path to a _TILED_ GeoTiff file.
func RasterToS2(path string, opts ConfigOpts, sink func(<-chan S2CellData) error) error {
	godal.RegisterAll()

	ds, err := godal.Open(path)
	if err != nil {
		logrus.Error(err)
		return err
	}
	defer func() {
		err = errors.Join(err, ds.Close())
	}()

	band, err := NewBandContainer(ds, 0)
	if err != nil {
		return err
	}

	startTime := time.Now()
	s2Data, err := indexBand(band, opts)
	if err != nil {
		logrus.Error(err)
		return err
	}

	err = sink(s2Data)
	if err != nil {
		return err
	}
	logrus.Info("\nIndexing took: ", time.Since(startTime))
	return nil
}

// indexBand simply reads block metadata from the supplied raster band, populating an input channel for the ProcessBlocks step
func indexBand(bandWithInfo *BandContainer, opts ConfigOpts) (<-chan S2CellData, error) {
	// Asynchronous generation of blocks to be consumed.
	blocks := genBlocks(bandWithInfo, opts)
	// Parallel processing of each block produced above.
	resCh := ProcessBlocks(bandWithInfo, blocks, opts)

	return resCh, nil
}

func genBlocks(band *BandContainer, opts ConfigOpts) <-chan godal.Block {
	logrus.Debug("Entered genBlocks")

	blocks := make(chan godal.Block)
	struc := band.Structure()
	firstBlock := struc.FirstBlock()
	numBlocks := (struc.SizeX * struc.SizeY) / (struc.BlockSizeX * struc.BlockSizeY)
	var i int
	go func() {
		defer close(blocks)
		for block, ok := firstBlock, true; ok; block, ok = block.Next() {
			blocks <- block
			i++
			if !opts.Verbose {
				// If verbose is set, we'll be getting a flood of output from the workers.
				fmt.Printf("\rProcessing block %d of %d", i, numBlocks)
			}
		}
		fmt.Println()
	}()
	logrus.Debug("Exited genBlocks")
	return blocks
}

// ProcessBlocks is the heart of the pipeline. It reads godal.Block definitions from an input channel across a pool of
// ConfigOpts.NumReadWorkers workers, which handle reading data from the underlying raster, indexing pixel values,
// and consolidating values into cellBatches in a per-cell map. The map is consumed and batches handed off to mergeWorkers
// based on a hash of the batch's cell ID. The merge workers ensure a single output value per cell, and their output channels
// get drained into a single return channel.
func ProcessBlocks(band *BandContainer, blocks <-chan godal.Block, opts ConfigOpts) <-chan S2CellData {
	logrus.Debug("Entered ProcessBlocks")
	defer logrus.Debug("Exited ProcessBlocks")
	// TODO: configurable buffer on result channel
	resCh := make(chan S2CellData, cellChanBufferSize)
	readWg := sync.WaitGroup{}
	mergeWorkers := newMergePool(band, opts.NumMergeWorkers, opts)

	// start merge workers listening here
	for _, w := range mergeWorkers {
		go func() {
			w.run()
		}()
	}

	for i := 0; i < opts.NumReadWorkers; i++ {
		readWg.Go(func() {
			logrus.Debug("Entered indexing goroutine")
			defer logrus.Debug("Exited indexing goroutine")
			for block := range blocks {
				logrus.Infof("Processing block at [%v, %v]", block.X0, block.Y0)
				// read the block and generate cells
				cellsMap, blockCoord, err := rasterBlockToS2(band, block, opts)
				if err != nil {
					logrus.Error(err)
					continue
				}

				// pass cell-batches to merge workers to ensure a single record for each cell
				mergeWG := sync.WaitGroup{}
				mergeWG.Add(len(cellsMap))
				for cell, batch := range cellsMap {
					batch.ack = &mergeWG
					workerID := cellWorkerIndex(cell, opts.NumMergeWorkers)
					mergeWorkers[workerID].in <- batch
				}
				// wait for acks from each batch once consumed by worker
				mergeWG.Wait()
				for _, worker := range mergeWorkers {
					worker.blockDone <- blockCoord
				}
			}
		})
	}

	// wait for all block reads to finish, then close in channels
	go func() {
		readWg.Wait()
		for _, w := range mergeWorkers {
			close(w.in)
			close(w.blockDone)
		}
	}()
	// drain all out channels to the single sink
	fanInWg := sync.WaitGroup{}
	for i := range mergeWorkers {
		worker := mergeWorkers[i]
		fanInWg.Go(func() {
			for cell := range worker.out {
				resCh <- cell
			}
		})
	}
	// wait until out channels are drained and close the return channel
	go func() {
		fanInWg.Wait()
		close(resCh)
	}()

	return resCh
}

func rasterBlockToS2(band *BandContainer, block godal.Block, opts ConfigOpts) (map[s2.CellID]cellBatch, BlockCoord, error) {
	results, err := ReadBlockToCells(block, band, opts)
	if err != nil {
		return nil, BlockCoord{}, err
	}

	blockCoord := BlockCoord{block.X0 / band.Structure().BlockSizeX, block.Y0 / band.Structure().BlockSizeY}
	batchesMap := make(map[s2.CellID]cellBatch)
	for _, cellData := range results {
		batch, ok := batchesMap[cellData.Cell]
		if !ok {
			batchesMap[cellData.Cell] = cellBatch{
				cellData.Cell,
				[]float64{cellData.Data},
				blockCoord,
				nil, // to be filled by the block worker's wg
			}
			continue
		}
		batchesMap[cellData.Cell] = cellBatch{
			batch.id,
			append(batch.values, cellData.Data),
			batch.block,
			batch.ack,
		}
	}

	return batchesMap, blockCoord, nil
}

// ReadBlockToCells performs the basic operation of reading a single block of data from the GeoTiff on disk (this required a TILED GeoTiff to
// reasonably bound memory usage), iterating over each pixel and assigning it an S2 cell ID. The return is an unaggregated slice of S2CellData
// with WKB set to an empty byte array. The caller is responsible for aggregating to a single record per cell and generating valid WKB (see
// CellToWKB) if desired.
func ReadBlockToCells(block godal.Block, band *BandContainer, opts ConfigOpts) ([]S2CellData, error) {
	xRes, yRes := band.Resolution()
	blockOrigin, err := blockOrigin(block, []float64{xRes, yRes}, band.Origin())
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	// Read band into blockBuf
	blockBuf := make([]float64, block.H*block.W)

	if err := lockedBlockRead(band, block, blockBuf); err != nil {
		return nil, err
	}

	noData, ok := band.Band.NoData()
	if !ok {
		logrus.Warn("NoData not set")
	}

	var results []S2CellData
	for pix := 0; pix < block.W*block.H; pix++ {
		value := blockBuf[pix]
		if value == noData {
			continue
		}

		// GDAL is row-major
		row := pix / block.W
		col := pix % block.W

		lat := blockOrigin.Y + (float64(row)+0.5)*yRes
		lng := blockOrigin.X + (float64(col)+0.5)*xRes

		pixArea := pixelArea(lat, xRes)

		latLng := s2.LatLngFromDegrees(lat, lng)
		s2Cell := s2.CellIDFromLatLng(latLng).Parent(opts.S2Lvl)

		// S2 areas are in steradians, so we need to convert to square meters.
		cellArea := s2.CellFromCellID(s2Cell).ApproxArea() * earthRadius * earthRadius
		if (cellArea < pixArea) && opts.AggFunc.isExtensive {
			value = value * (cellArea / pixArea)
		}

		// geom string will be created once cells are aggregated
		cellData := S2CellData{s2Cell, value, []byte{}}
		results = append(results, cellData)
	}
	return results, nil
}

func lockedBlockRead(band *BandContainer, block godal.Block, blockBuf []float64) error {
	band.Lock()
	defer band.Unlock()
	if err := band.Band.Read(block.X0, block.Y0, blockBuf, block.W, block.H); err != nil {
		return err
	}
	return nil
}

func blockOrigin(rasterBlock godal.Block, resolution []float64, origin Point) (Point, error) {
	originLng := float64(rasterBlock.X0)*resolution[0] + origin.X
	originLat := float64(rasterBlock.Y0)*resolution[1] + origin.Y
	return Point{X: originLng, Y: originLat}, nil
}

func pixelArea(latitude float64, resolution float64) float64 {
	pixWidth := haversinePixelWidth(latitude, resolution)
	pixHeight := (math.Pi / 180) * resolution * earthRadius
	return pixWidth * pixHeight
}

func haversinePixelWidth(latitude float64, resolution float64) float64 {
	latRad := latitude * math.Pi / 180
	resRad := resolution * math.Pi / 180
	a := math.Pow(math.Cos(latRad), 2) * math.Pow(math.Sin(resRad/2), 2)
	return 2 * earthRadius * math.Asin(math.Sqrt(a))
}

func expectedBlocksForCell(cellID s2.CellID, band *BandContainer) []BlockCoord {
	// need to get the cell rectangle first, and safely convert from radians. two key cases to account for:
	// 1. Anti-meridian - spanning -180/180deg longitude may lead to a cell wrapping back around to the other side of the raster
	// 2. Pole - A cell overlapping 90 or -90 deg latitude will potentially span many blocks on the top or bottom rows
	// defer pole handling, it is **extremely** rare for a raster to overlap the pole. document known issue.
	cellRect := s2.CellFromCellID(cellID).RectBound()
	bbox := [4]float64{
		cellRect.Lo().Lng.Degrees(),
		cellRect.Lo().Lat.Degrees(),
		cellRect.Hi().Lng.Degrees(),
		cellRect.Hi().Lat.Degrees(),
	}
	blockRect := toBlockRectangle(bbox, band)

	expectedBlocks := make([]BlockCoord, 0)
	for i := blockRect[0]; i <= blockRect[2]; i++ {
		for j := blockRect[1]; j <= blockRect[3]; j++ {
			expectedBlocks = append(expectedBlocks, BlockCoord{I: i, J: j})
		}
	}
	return expectedBlocks
}

// toBlockRectangle takes a rectangle described by an array of [minX, minY, maxX, maxY] and calculates the horizontal and vertical block ranges
// it overlaps in the supplied raster. The return is another[minX, minY, maxX, maxY], described in integer block coordinates rather than the
// input raster's coordinate reference system.
func toBlockRectangle(rect [4]float64, band *BandContainer) [4]int {
	xRes, yRes := band.Resolution()
	pixMinCol := math.Floor((rect[0] - band.Origin().X) / xRes)
	pixMaxCol := math.Ceil((rect[2] - band.Origin().X) / xRes)
	// Convention with rasters is for Y resolution to be negative, with the top-left corner as the origin. This makes the minimum Y value the
	// maximum number of rows down, and the maximum Y value (less negative) the minimum number of rows down.
	pixMinRow := math.Floor((rect[3] - band.Origin().Y) / yRes)
	pixMaxRow := math.Ceil((rect[1] - band.Origin().Y) / yRes)

	numXBlocks, numYBlocks := band.Structure().BlockCount()
	return [4]int{
		max(0, int(pixMinCol)/band.Structure().BlockSizeX),
		max(0, int(pixMinRow)/band.Structure().BlockSizeY),
		min(numXBlocks-1, int(pixMaxCol)/band.Structure().BlockSizeX),
		min(numYBlocks-1, int(pixMaxRow)/band.Structure().BlockSizeY),
	}
}

func cellWorkerIndex(cellID s2.CellID, n int) int {
	// FNV-1a 64-bit
	const (
		offset64 uint64 = 14695981039346656037
		prime64  uint64 = 1099511628211
	)
	h := offset64
	v := uint64(cellID)
	for range 8 {
		h ^= v & 0xff
		h *= prime64
		v >>= 8
	}
	return int(h % uint64(n))
}
