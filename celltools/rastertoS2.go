package celltools

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/airbusgeo/godal"
	"github.com/golang/geo/s2"
	"github.com/sirupsen/logrus"
)

const (
	EarthRadius  float64 = 6371000
	CellWKTSize  int     = 19*5 + 11
	CellDataSize int     = CellWKTSize + 16
	BytesInGB    int     = 1024 * 1024 * 1024
)

type ConfigOpts struct {
	NumReadWorkers  int
	NumMergeWorkers  int
	S2Lvl       int
	AggFunc     AggFunc
	MemLimit    int
	IsExtensive bool
	Verbose     bool
}

type Point struct {
	X float64
	Y float64
}

type BandContainer struct {
	*sync.Mutex
	godal.Band
	GeoTransform [6]float64
}

func (b *BandContainer) Origin() Point {
	return Point{ X: b.GeoTransform[0], Y: b.GeoTransform[3]}
}

func (b *BandContainer) Resolution() (float64, float64) {
	xRes := b.GeoTransform[1]
	yRes := b.GeoTransform[5]
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


type S2CellData struct {
	Cell       s2.CellID
	Data       float64
	GeomString string
}

type S2CellGeom struct {
	cell s2.CellID
	geom string
}

func (c S2CellData) String() string {
	return fmt.Sprintf("%v;%v;%s", int64(c.Cell), c.Data, c.GeomString)
}

type AggFunc func(...float64) float64

func (f AggFunc) IsExtensive() bool {
	smallVals := []float64{
		f(1, 2, 3),
		f(1, 1, 1),
		f(-1, -1, -1),
		f(0, 0, 0),
		f(-1, -2, -3),
	}
	largeVals := []float64{
		f(1, 1, 2, 2, 3, 3),
		f(1, 1, 1, 1, 1, 1),
		f(-1, -1, -1, -1, -1, -1),
		f(0, 0, 0, 0, 0, 0),
		f(-1, -1, -2, -2, -3, -3),
	}
	return !reflect.DeepEqual(smallVals, largeVals)
}

func RasterToS2(path string, opts ConfigOpts, sink func(chan S2CellData) error) error {
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
	fmt.Printf("\nIndexing took %v", time.Since(startTime))
	return nil
}

func indexBand(bandWithInfo *BandContainer, opts ConfigOpts) (chan S2CellData, error) {
	// Asynchronous generation of blocks to be consumed.
	blocks := genBlocks(bandWithInfo, opts)
	// Parallel processing of each block produced above.
	resCh := processBlocks(bandWithInfo, blocks, opts)

	return resCh, nil
}

// Produce blocks from a raster band, putting them in a channel to be consumed
// downstream.
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
	}()
	logrus.Debug("Exited genBlocks")
	return blocks
}

func processBlocks(band *BandContainer, blocks <-chan godal.Block, opts ConfigOpts) chan S2CellData {
	logrus.Debug("Entered processBlocks")
	resCh := make(chan S2CellData)
	wg := sync.WaitGroup{}

	for i := 0; i < opts.NumReadWorkers; i++ {
		wg.Add(1)
		go func() {
			logrus.Debug("Entered indexing goroutine")
			defer wg.Done()
			for block := range blocks {
				logrus.Infof("Processing block at [%v, %v]", block.X0, block.Y0)
				cellsMap, err := rasterBlockToS2(band, block, opts)
				if err != nil {
					logrus.Error(err)
					continue
				}
				aggCellResults(cellsMap, opts.AggFunc, resCh)
			}
			logrus.Debug("Exited indexing goroutine")
		}()
	}

	go func() {
		wg.Wait()
		close(resCh)
	}()

	logrus.Debug("Exited blockProcessor")
	return resCh
}

func rasterBlockToS2(band *BandContainer, block godal.Block, opts ConfigOpts) (map[S2CellGeom][]float64, error) {
	results, err := readBlockToCells(block, band, opts)
	if err != nil {
		return nil, err
	}
	groupedResults := groupByCell(results)

	return groupedResults, nil
}

func readBlockToCells(block godal.Block, band *BandContainer, opts ConfigOpts) ([]S2CellData, error) {
	xRes, yRes := band.Resolution()
	blockOrigin, err := blockOrigin(block, []float64{xRes, yRes}, band.Origin())
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	// Read band into blockBuf
	blockBuf := make([]float64, block.H*block.W)

	if err := lockedRead(band, block, blockBuf); err != nil {
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
		cellArea := s2.CellFromCellID(s2Cell).ApproxArea() * EarthRadius * EarthRadius
		if (cellArea < pixArea) && opts.IsExtensive {
			value = value * cellArea / pixArea
		}

		// geom string will be created once cells are aggregated
		cellData := S2CellData{s2Cell, value, ""}
		results = append(results, cellData)
	}
	return results, nil
}

// Locking is required to read from compressed rasters.
func lockedRead(band *BandContainer, block godal.Block, blockBuf []float64) error {
	band.Lock()
	defer band.Unlock()
	if err := band.Band.Read(block.X0, block.Y0, blockBuf, block.W, block.H); err != nil {
		return err
	}
	return nil
}

func aggCellResults(resMap map[S2CellGeom][]float64, aggFunc AggFunc, resCh chan S2CellData) {
	logrus.Debug("Entered aggCellResults")

	for cellGeom := range resMap {
		values := resMap[cellGeom]
		geomString := cellToWKT(s2.CellFromCellID(cellGeom.cell))
		resCh <- S2CellData{cellGeom.cell, aggFunc(values...), geomString}
		// free memory occupied by cell's data as soon as it's not needed
		delete(resMap, cellGeom)
	}
	logrus.Debug("Exited aggCellResults")
}

func groupByCell(results []S2CellData) map[S2CellGeom][]float64 {
	logrus.Debug("Entered groupByCell")

	outMap := make(map[S2CellGeom][]float64)
	for _, cellData := range results {
		cellGeom := S2CellGeom{cellData.Cell, ""}

		outMap[cellGeom] = append(outMap[cellGeom], cellData.Data)

	}
	logrus.Debug("Exited groupByCell")
	return outMap
}

func blockOrigin(rasterBlock godal.Block, resolution []float64, origin Point) (Point, error) {
	originLng := float64(rasterBlock.X0)*resolution[0] + origin.X
	originLat := float64(rasterBlock.Y0)*resolution[1] + origin.Y
	return Point{X: originLng, Y: originLat}, nil
}

func pixelArea(latitude float64, resolution float64) float64 {
	pixWidth := haversinePixelWidth(latitude, resolution)
	pixHeight := (math.Pi / 180) * resolution * EarthRadius
	return pixWidth * pixHeight
}

func haversinePixelWidth(latitude float64, resolution float64) float64 {
	latRad := latitude * math.Pi / 180
	resRad := resolution * math.Pi / 180
	a := math.Pow(math.Cos(latRad), 2) * math.Pow(math.Sin(resRad/2), 2)
	return 2 * EarthRadius * math.Asin(math.Sqrt(a))
}

// SCRATCH
// staging the implementation of a multi-step fan-in to avoid duplication on block edges.
// first I want to sketch alternate processBlocks() that uses multiple worker structs, routed to by 
// hash(cellID) % numWorkers. The workers will accumulate, ack receipt of a block, and flush cells to
// sink when they know all contributing blocks have added values to a cell's accumulator.

// BlockCoord is the X and Y position of a block within the tiling scheme of a raster band
type BlockCoord struct{ I, J int}
// and these will be associated with a CellBatch:
type CellBatch struct{
	ID s2.CellID
	Values []float64
	Block BlockCoord
	ack *sync.WaitGroup
}

// When a worker receives a batch, it accumulates to the cell's slice of values and acknowledges receipt of the block's
// data. These acks will be tracked with a WaitGroup: rasterBlockToS2 emits []CellBatch + BlockCoord, the block worker
// goroutine will create a WaitGroup and add len(batches) to it, then when dispatching batches to mergeWorker in channels
// the batches' ack members can be set to that WG. Then, after accumulating to the cellAcc, the mergeWorker can call
// batch.ack.Done(), and the block worker can wait on that WG to signal blockDone.
//
// When a blockDone signal is received by a worker, it should add the block to processedBlocks and iterate over the cell
// accumulators in reverseIndex under that BlockCoord, removing the block from remaining for each, and flushing accumulators
// that are left with an empty remaining set.
//
// On creation of an accumulator for a cell, check processedBlocks for the block that has created the cell's batch, and if
// found, do not put the block into `remaining` for the batch. Flush if remaining is empty. This is handled in a newAcc 
// method, which is also where the expected blocks are calculated to fill `remaining`.
//
// Traffic to accumulation or blockDone/flush paths will be done by the worker selecting over in and blockDone in its main
// loop. When in closes, drain block.Done for any remaining signals there.

// processedBlocks:
// This exists primarily for orphaned blockDone signals. The case is this: a cell batch is created for a cell that creates
// a rectangle overlapping a previously processed block by a sliver. It expects this block, but that block hasn't create any
// batches for it, so this cell's accumulator will sit with that block in `remaining` until `in` closes and the safety-net
// flush triggers. processedBlocks lets us check if that block was processed before the accumulator was created, and flush
// it when its real expected blocks are done.
//
// We cannot prune processedBlocks too aggressively, and it may be worth leaving it un-pruned since the number of blocks in
// a raster will be negligible next to the number of pixels and/or cells. It's a second-order memory concern. If we were to
// prune it, we must consider that the blocks are emitted in row-major order, so when the row has advanced far enough, we no
// longer need that block in `processedBlocks`. 
// 
// Assuming that a cell is smaller than a block of the raster, a new cell may still overlap a block as long as that block is later than the
// the cell's block x position in the previous row. So, a ring buffer of length NumBlocksX + 1 would hold all relevant blocks.
// In this scenario, blocks would be added to the ring buffer at (j*NumBlocksX + i) % len(buf). The most natural way to use 
// this would end up just being scanning the buffer, which is not the cheapest access. 

func newRasterBlockToS2(band *BandContainer, block godal.Block, opts ConfigOpts) (map[s2.CellID]CellBatch, BlockCoord, error) {
	results, err := readBlockToCells(block, band, opts)
	if err != nil {
		return nil, BlockCoord{}, err
	}

	blockCoord := BlockCoord{block.X0 / band.Structure().BlockSizeX, block.Y0 / band.Structure().BlockSizeY }
	batchesMap := make(map[s2.CellID]CellBatch)
	for _, cellData := range results {
		batch, ok := batchesMap[cellData.Cell]
		if !ok {
			batchesMap[cellData.Cell] = CellBatch{
				cellData.Cell,
				[]float64{cellData.Data},
				blockCoord,
				nil, // to be filled by the block worker's wg
			}
			continue
		}
		batchesMap[cellData.Cell] = CellBatch{
			batch.ID,
			append(batch.Values, cellData.Data),
			batch.Block,
			batch.ack,
		}
	}

	return batchesMap, blockCoord, nil
}


func newProcessBlocks(band *BandContainer, blocks <-chan godal.Block, opts ConfigOpts) chan S2CellData {
	logrus.Debug("Entered processBlocks")
	resCh := make(chan S2CellData)
	readWg := sync.WaitGroup{}
	numMergeWorkers := max(2, opts.NumReadWorkers / 2)
	mergeWorkers := newMergePool(band, numMergeWorkers, opts)

	// start merge workers listening here
	for _, w := range mergeWorkers {
		go func() {
			w.run()
		}()
	}

	for i := 0; i < opts.NumReadWorkers; i++ {
		readWg.Go(func() {
			logrus.Debug("Entered indexing goroutine")
			defer readWg.Done()
			for block := range blocks {
				logrus.Infof("Processing block at [%v, %v]", block.X0, block.Y0)
				// read the block and generate cells
				cellsMap, blockCoords, err := newRasterBlockToS2(band, block, opts)
				if err != nil {
					logrus.Error(err)
					continue
				}

				// pass cell-batches to merge workers to ensure a single record for each cell
				mergeWG := sync.WaitGroup{}
				mergeWG.Add(len(cellsMap))
				for cell, batch := range cellsMap {
					batch.ack = &mergeWG
					// need a hash to use here for worker ID
					workerID := cellWorkerIndex(cell, numMergeWorkers)
					mergeWorkers[workerID].in <- batch
				}
				// wait for acks from each batch once consumed by worker
				mergeWG.Wait()
				for _, worker := range(mergeWorkers) {
					// broadcast block done signal
					worker.blockDone <- blockCoords
				}
			}
			logrus.Debug("Exited indexing goroutine")
		})
	}
	// order of signals:
	// 1. per-block mergeWg signals done, proceed to next block
	// 2. per-read worker readWg signals done once all input blocks have been drained
	// 3. per-merge worker fanInWg signals done once merge results drained

	// wait for all block reads to finish, then close in channels
	go func() {
		readWg.Wait()
		for _, w := range mergeWorkers {
			close(w.in)
			close(w.blockDone)
		}
	}()

	// need to close out channels

	// drain all out channels to the single sink
	fanInWg := sync.WaitGroup{}
	for i := range mergeWorkers {
		worker := mergeWorkers[i]
		fanInWg.Go(func() {
			defer fanInWg.Done()
			for cell := range worker.out {
				resCh <- cell
			}
		})
	}
	// wait until out channels are drained
	go func() {
		fanInWg.Wait()
		close(resCh)
	}()

	logrus.Debug("Exited processBlocks")
	return resCh
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
		max(0, int(pixMinCol) / band.Structure().BlockSizeX),
		max(0, int(pixMinRow) / band.Structure().BlockSizeY),
		min(numXBlocks-1, int(pixMaxCol) / band.Structure().BlockSizeX),
		min(numYBlocks-1, int(pixMaxRow) / band.Structure().BlockSizeY),
	}
}

func cellWorkerIndex(cellID s2.CellID, n int) int {
    // FNV-1a 64-bit
    const (
        offset64 uint64 = 14695981039346656037
        prime64 uint64  = 1099511628211
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