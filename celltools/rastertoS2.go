package celltools

import (
	"errors"
	"fmt"
	"s2-tools/dggs"
	"s2-tools/geotiff"
	"sync"
	"time"

	"github.com/airbusgeo/godal"
	"github.com/sirupsen/logrus"
)

const (
	cellWKBSize        int = 1 + 4 + 4 + 4 + 5*16
	cellDataSize       int = cellWKBSize + 16
	cellChanBufferSize int = 100 // testing found ~100 allowed saturation of workers
)

type Config struct {
	NumReadWorkers  int
	NumMergeWorkers int
	Verbose         bool
}

// cellBatch is an internal implementation detail of the ProcessBlocks step in the pipeline. It is exclusively for transport of cell values from
// rasterBlockToS2() to a mergeWorker
type cellBatch struct {
	id     uint64
	values []float64
	block  geotiff.BlockCoord
	ack    *sync.WaitGroup
}

// IndexedCellData is the output type of this pipeline. It describes a single S2 cell with a single value. The WKB included allows writing of valid
// GeoParquet directly from pipeline outputs.
type IndexedCellData struct {
	CellID uint64
	Data   float64
	WKB    []byte
}

func (c IndexedCellData) String() string {
	return fmt.Sprintf("%v;%v;%s", int64(c.CellID), c.Data, c.WKB)
}

type RasterIndexingPipeline struct {
	RasterBand *geotiff.Band
	Indexer    dggs.Indexer
	Sink       func(<-chan IndexedCellData) error
	AggFunc    AggFunc
	Config     Config
}

// Run simply reads block metadata from the supplied raster band, populating an input channel for the ProcessBlocks step
func (r *RasterIndexingPipeline) Run() (<-chan IndexedCellData, error) {
	// Asynchronous generation of blocks to be consumed.
	blocks := r.GenBlocks()
	// Parallel processing of each block produced above.
	resCh := r.ProcessBlocks(blocks)

	return resCh, nil
}

func (r *RasterIndexingPipeline) GenBlocks() <-chan godal.Block {
	blocks := make(chan godal.Block)
	struc := r.RasterBand.Structure()
	firstBlock := struc.FirstBlock()
	numBlocks := (struc.SizeX * struc.SizeY) / (struc.BlockSizeX * struc.BlockSizeY)
	var i int
	go func() {
		defer close(blocks)
		for block, ok := firstBlock, true; ok; block, ok = block.Next() {
			blocks <- block
			i++
			if !r.Config.Verbose {
				// If verbose is set, we'll be getting a flood of output from the workers, this gets drowned
				fmt.Printf("\rProcessing block %d of %d\n", i, numBlocks)
			}
		}
		fmt.Println()
	}()
	return blocks
}

// ProcessBlocks is the heart of the pipeline. It reads godal.Block definitions from an input channel across a pool of
// ConfigOpts.NumReadWorkers workers, which handle reading data from the underlying raster, indexing pixel values,
// and consolidating values into cellBatches in a per-cell map. The map is consumed and batches handed off to mergeWorkers
// based on a hash of the batch's cell ID. The merge workers ensure a single output value per cell, and their output channels
// get drained into a single return channel.
func (r *RasterIndexingPipeline) ProcessBlocks(blocks <-chan godal.Block) <-chan IndexedCellData {
	resCh := make(chan IndexedCellData, cellChanBufferSize)
	readWg := sync.WaitGroup{}
	numXBlocks, _ := r.RasterBand.Structure().BlockCount()
	mergeWorkers := newMergePool(numXBlocks, r.AggFunc, r.Config)

	// start merge workers listening here
	for _, w := range mergeWorkers {
		go func() {
			w.run()
		}()
	}

	for i := 0; i < r.Config.NumReadWorkers; i++ {
		readWg.Go(func() {
			logrus.Debug("Entered indexing goroutine")
			defer logrus.Debug("Exited indexing goroutine")
			for block := range blocks {
				logrus.Infof("Processing block at [%v, %v]", block.X0, block.Y0)
				// read the block and generate cells
				cellsMap, blockCoord, err := r.indexBlock(block)
				if err != nil {
					logrus.Error(err)
					continue
				}

				// pass cell-batches to merge workers to ensure a single record for each cell
				mergeWG := sync.WaitGroup{}
				mergeWG.Add(len(cellsMap))
				for cell, batch := range cellsMap {
					batch.ack = &mergeWG
					bbox := r.Indexer.CellBBox(cell)
					expected := r.RasterBand.GetBlocksIntersectingBBox(bbox)
					bundle := struct {
						batch          cellBatch
						expectedBlocks []geotiff.BlockCoord
					}{batch, expected}
					workerID := cellWorkerIndex(cell, r.Config.NumMergeWorkers)
					mergeWorkers[workerID].in <- bundle
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
				cell.WKB = r.Indexer.CellIDToWKB(cell.CellID)
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

func (r RasterIndexingPipeline) indexBlock(block godal.Block) (map[uint64]cellBatch, geotiff.BlockCoord, error) {
	results, err := r.ReadBlockToRawCells(block)
	if err != nil {
		return nil, geotiff.BlockCoord{}, err
	}

	blockCoord := geotiff.BlockCoord{I: block.X0 / r.RasterBand.Structure().BlockSizeX, J: block.Y0 / r.RasterBand.Structure().BlockSizeY}
	batchesMap := make(map[uint64]cellBatch)
	for _, cellData := range results {
		batch, ok := batchesMap[cellData.CellID]
		if !ok {
			batchesMap[cellData.CellID] = cellBatch{
				cellData.CellID,
				[]float64{cellData.Data},
				blockCoord,
				nil, // to be filled by the block worker's wg
			}
			continue
		}
		batchesMap[cellData.CellID] = cellBatch{
			batch.id,
			append(batch.values, cellData.Data),
			batch.block,
			batch.ack,
		}
	}

	return batchesMap, blockCoord, nil
}

// ReadBlockToRawCells performs the basic operation of reading a single block of data from the GeoTiff on disk (this required a TILED GeoTiff to
// reasonably bound memory usage), iterating over each pixel and assigning it an S2 cell ID. The return is an unaggregated slice of S2CellData
// with WKB set to an empty byte array. The caller is responsible for aggregating to a single record per cell and generating valid WKB (see
// CellToWKB) if desired.
func (r *RasterIndexingPipeline) ReadBlockToRawCells(block godal.Block) ([]IndexedCellData, error) {
	xRes, yRes := r.RasterBand.Resolution()
	blockOrigin, err := geotiff.BlockOrigin(block, []float64{xRes, yRes}, r.RasterBand.Origin())
	if err != nil {
		logrus.Error(err)
		return nil, err
	}
	// Read band into blockBuf
	blockBuf := make([]float64, block.H*block.W)

	if err := r.RasterBand.LockedBlockRead(block, blockBuf); err != nil {
		return nil, err
	}

	noData, hasNodata := r.RasterBand.NoData()
	if !hasNodata {
		logrus.Warn("NoData not set")
	}

	var results []IndexedCellData
	for pix := 0; pix < block.W*block.H; pix++ {
		value := blockBuf[pix]
		if hasNodata && value == noData {
			continue
		}

		// GDAL is row-major
		row := pix / block.W
		col := pix % block.W

		lat := blockOrigin.Lat + (float64(row)+0.5)*yRes
		lng := blockOrigin.Lng + (float64(col)+0.5)*xRes

		pixArea := geotiff.PixelArea(lat, xRes)

		cellID := r.Indexer.PointToCellID(geotiff.LngLat{Lng: lng, Lat: lat})

		cellArea := r.Indexer.CellArea(cellID)
		if (cellArea < pixArea) && r.AggFunc.isExtensive {
			value = value * (cellArea / pixArea)
		}

		// WKB will be generated as the pipeline is drained
		cellData := IndexedCellData{cellID, value, []byte{}}
		results = append(results, cellData)
	}
	return results, nil
}

func RunIndexingPipeline(path string, indexer dggs.Indexer, sink func(<-chan IndexedCellData) error, aggFunc AggFunc, config Config) error {
	godal.RegisterAll()

	ds, err := godal.Open(path)
	if err != nil {
		logrus.Error(err)
		return err
	}
	defer func() {
		err = errors.Join(err, ds.Close())
	}()

	band, err := geotiff.NewBandContainer(ds, 0)
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
	logrus.Info("\nIndexing took: ", time.Since(startTime))
	return nil
}
