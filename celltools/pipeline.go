package celltools

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Terence-jo/cellpipe/sources"
	"github.com/Terence-jo/cellpipe/types"

	"github.com/airbusgeo/godal"
)

const (
	cellChanBufferSize int = 5000 // testing found ~100 allowed saturation of workers
	chanSendPackSize   int = 1024
)

var (
	logger Logger
)

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type Config struct {
	NumReadWorkers  int
	NumMergeWorkers int
	Logger          Logger
	Verbose         bool
}

// cellBatch is an internal implementation detail of the ProcessBlocks step in the pipeline. It is exclusively for transport of cell values from
// rasterBlockToS2() to a mergeWorker
type cellBatch struct {
	id     uint64
	values []float64
	block  types.BlockCoord
}

// IndexedCellData is the output type of this pipeline. It describes a single S2 cell with a single value. The WKB included allows writing of valid
// GeoParquet directly from pipeline outputs.
type IndexedCellData struct {
	ID   uint64
	Data float64
	WKB  []byte
}

func (c IndexedCellData) String() string {
	return fmt.Sprintf("%v;%v;%s", int64(c.ID), c.Data, c.WKB)
}

// A DataSource would commonly be a types, but could be satisfied by any array-based dataset as long as it's end state is a two-dimensional
// Lng/Lat grid. Implement this interface to include custom transformations before reading and indexing.
type DataSource interface {
	NumBlocks() (int, int)
	BlockSize(block types.BlockCoord) (int, int)
	ReadBlock(block types.BlockCoord, buf []float64) (types.LngLat, error)
	GenerateBlocks() <-chan types.BlockCoord
	GetBlocksIntersectingBBox(bbox [4]float64) []types.BlockCoord
	NoData() float64
	Resolution() (float64, float64)
}

// An Indexer turns coordinates into cell IDs from a Discrete Global Grid System (DGGS)
type Indexer interface {
	Name() string
	PointToCellID(point types.LngLat) (uint64, error)
	CellIDToPoint(cell uint64) (types.LngLat, error)
	CellIDToWKB(cell uint64) ([]byte, error)
	CellArea(cell uint64) (float64, error)
	CellBBox(cell uint64) ([4]float64, error)
	SentinelCell() uint64
}

type RasterIndexingPipeline struct {
	Source  DataSource
	Indexer Indexer
	Sink    func(<-chan []IndexedCellData) error
	AggFunc AggFunc
	Config  Config
}

// Run simply reads block metadata from the supplied raster band, populating an input channel for the ProcessBlocks step
func (r *RasterIndexingPipeline) Run() (<-chan []IndexedCellData, error) {
	// Asynchronous generation of blocks to be consumed.
	blocks := r.Source.GenerateBlocks()
	// Parallel processing of each block produced above.
	resCh := r.ProcessBlocks(blocks)

	return resCh, nil
}

// ProcessBlocks is the heart of the pipeline. It reads block definitions from an input channel across a pool of
// ConfigOpts.NumReadWorkers workers, which handle reading data from the underlying raster, indexing pixel values,
// and consolidating values into cellBatches in a per-cell map. The map is consumed and batches handed off to mergeWorkers
// based on a hash of the batch's cell ID. The merge workers ensure a single output value per cell, and their output channels
// get drained into a single return channel.
func (r *RasterIndexingPipeline) ProcessBlocks(blocks <-chan types.BlockCoord) <-chan []IndexedCellData {
	resCh := make(chan []IndexedCellData, cellChanBufferSize)
	readWg := sync.WaitGroup{}
	numXBlocks, _ := r.Source.NumBlocks()
	mergeWorkers := newMergePool(numXBlocks, r.AggFunc, r.Config, r.Indexer.SentinelCell())

	// start merge workers listening here
	for _, w := range mergeWorkers {
		go func() {
			w.run()
		}()
	}

	blockSizeX, blockSizeY := r.Source.BlockSize(types.BlockCoord{})
	for i := 0; i < r.Config.NumReadWorkers; i++ {
		readWg.Go(func() {
			logger.Debug("Entered indexing goroutine")
			defer logger.Debug("Exited indexing goroutine")
			blockPixels := blockSizeX * blockSizeY
			// pre-generate map with conservatively high map allocation depending on scale factor between pixels and cells to ease allocation pressure
			batchesMap := make(map[uint64]*cellBatch, blockPixels/4)
			for block := range blocks {
				logger.Info(fmt.Sprintf("Processing block at [%v, %v]", block.I, block.J))
				// read the block and generate cells
				cellsMap, err := r.indexBlock(block, batchesMap)
				if err != nil {
					logger.Error(err.Error())
					continue
				}
				r.distributeMergeWork(cellsMap, mergeWorkers, block)
			}
		})
	}

	// wait for all block reads to finish, then close in channels
	go func() {
		readWg.Wait()
		for _, w := range mergeWorkers {
			close(w.in)
		}
	}()
	// drain all out channels to the single sink
	fanInWg := sync.WaitGroup{}
	for i := range mergeWorkers {
		worker := mergeWorkers[i]
		fanInWg.Go(func() {
			for pack := range worker.out {
				for i := range pack {
					wkb, err := r.Indexer.CellIDToWKB(pack[i].ID)
					if err != nil {
						wkb = []byte{}
						logger.Warn("WKB could not be generated for cell:", pack[i].ID)
					}
					pack[i].WKB = wkb

				}
				resCh <- pack
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

func (r *RasterIndexingPipeline) distributeMergeWork(cellsMap map[uint64]*cellBatch, mergeWorkers []mergeWorker, block types.BlockCoord) {
	distributionPacks := make([][]cellMergeBundle, r.Config.NumMergeWorkers)
	for cell, batch := range cellsMap {
		bbox, err := r.Indexer.CellBBox(cell)
		if err != nil {
			logger.Error(err.Error())
			continue
		}
		expected := r.Source.GetBlocksIntersectingBBox(bbox)
		bundle := cellMergeBundle{*batch, expected}

		workerID := cellWorkerIndex(cell, r.Config.NumMergeWorkers)
		distributionPacks[workerID] = append(distributionPacks[workerID], bundle)
		if len(distributionPacks[workerID]) >= chanSendPackSize {
			mergeWorkers[workerID].in <- distributionPacks[workerID]
			distributionPacks[workerID] = make([]cellMergeBundle, 0, chanSendPackSize)
		}
	}
	for i := range distributionPacks {
		// send sentinel batch to signal end of results for the given blockCoord, triggering onBlockDone()
		distributionPacks[i] = append(distributionPacks[i], makeSentinelBundle(block, r.Indexer.SentinelCell()))
		mergeWorkers[i].in <- distributionPacks[i]
	}
}

func (r RasterIndexingPipeline) indexBlock(block types.BlockCoord, batchesMap map[uint64]*cellBatch) (map[uint64]*cellBatch, error) {
	clear(batchesMap)
	results, err := r.ReadBlockToRawCells(block)
	if err != nil {
		return nil, err
	}

	for _, cellData := range results {
		batch, ok := batchesMap[cellData.ID]
		if !ok {
			batchesMap[cellData.ID] = &cellBatch{
				cellData.ID,
				[]float64{cellData.Data},
				block,
			}
			continue
		}
		batch.values = append(batch.values, cellData.Data)
	}

	return batchesMap, nil
}

// ReadBlockToRawCells performs the basic operation of reading a single block of data from the types on disk (this required a TILED types to
// reasonably bound memory usage), iterating over each pixel and assigning it an S2 cell ID. The return is an unaggregated slice of S2CellData
// with WKB set to an empty byte array. The caller is responsible for aggregating to a single record per cell and generating valid WKB (see
// CellToWKB) if desired.
func (r *RasterIndexingPipeline) ReadBlockToRawCells(block types.BlockCoord) ([]IndexedCellData, error) {
	blockSizeX, blockSizeY := r.Source.BlockSize(block)
	// Read band into blockBuf
	blockBuf := make([]float64, blockSizeX*blockSizeY)

	blockOrigin, err := r.Source.ReadBlock(block, blockBuf)
	if err != nil {
		return nil, err
	}

	// TODO: move this check up to the top, it only needs to be done once
	noData := r.Source.NoData()
	xRes, yRes := r.Source.Resolution()

	results := make([]IndexedCellData, 0, len(blockBuf))
	for row := range blockSizeY {
		lat := blockOrigin.Lat + (float64(row)+0.5)*yRes
		// TODO: revisit where pixel area should live
		pixArea := sources.PixelArea(lat, xRes)
		for col := range blockSizeX {
			value := blockBuf[row*blockSizeX+col]
			if value == noData {
				continue
			}
			lng := blockOrigin.Lng + (float64(col)+0.5)*xRes

			cellID, err := r.Indexer.PointToCellID(types.LngLat{Lng: lng, Lat: lat})
			if err != nil {
				return nil, fmt.Errorf("error indexing pixel at row %d, col %d: %w", row, col, err)
			}

			if r.AggFunc.IsExtensive {
				cellArea, err := r.Indexer.CellArea(cellID)
				if err != nil {
					return nil, err
				}
				if cellArea < pixArea {
					value = value * (cellArea / pixArea)
				}
			}

			// WKB will be generated as the pipeline is drained
			cellData := IndexedCellData{cellID, value, []byte{}}
			results = append(results, cellData)
		}
	}
	return results, nil
}

func RunIndexingPipeline(path string, indexer Indexer, sink func(<-chan []IndexedCellData) error, aggFunc AggFunc, config Config) error {
	logger = config.Logger
	godal.RegisterAll()

	ds, err := godal.Open(path)
	if err != nil {
		logger.Error(err.Error())
		return err
	}
	defer func() {
		err = errors.Join(err, ds.Close())
	}()

	band, err := sources.NewBand(ds, 0)
	if err != nil {
		return err
	}
	pipeline := &RasterIndexingPipeline{
		band,
		indexer,
		sink,
		aggFunc,
		config,
	}

	startTime := time.Now()
	indexedData, err := pipeline.Run()
	if err != nil {
		return err
	}

	err = sink(indexedData)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Indexing took: %d", time.Since(startTime).Milliseconds()))
	return nil
}
