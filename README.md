# s2-tools

CLI tools for converting geospatial rasters into Discrete Global Grid System
(DGGS) cell indexes. Currently exposes one command, `indexraster`, which reads
a GeoTIFF, assigns every pixel to a DGGS cell (S2 or H3, selectable) at a
chosen resolution, aggregates the pixel values per cell, and writes the
result as GeoParquet (cell ID, aggregated value, cell geometry).

Built on [godal](https://github.com/airbusgeo/godal) (GDAL bindings),
[golang/geo/s2](https://github.com/golang/geo), [h3-go](https://github.com/uber/h3-go),
and [parquet-go](https://github.com/parquet-go/parquet-go).

## Build

Requires Go 1.26+ and a working GDAL installation.

```bash
go build -o s2-tools .
```

## Usage

```bash
./s2-tools indexraster [flags] <input.tif> <output_path>
```

`output_path` is a directory; the writer shards it into one Parquet file per
sink worker (`<basename>-<i>.parquet`).

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--numReadWriteWorkers` | `-w` | 8 | Parallel block readers and sink writers |
| `--numMergeWorkers` | `-m` | 8 | Merge workers that accumulate per-cell values. Don't exceed CPU cores. |
| `--indexer` | `-i` | `s2` | DGGS to index with: `s2` or `h3` |
| `--indexLevel` | `-l` | 11 | DGGS cell level/resolution. Output resolution. |
| `--aggFunc` | `-a` | `mean` | Aggregation: `mean`, `sum`, `max`, `min`, `mode` |
| `--verbose` | `-v` | false | Info-level logs |
| `--debug` | `-d` | false | Debug-level logs |
| `--cpuprofile` | `-p` | "" | Write a CPU profile to this file (`go tool pprof`) |
| `--memprofile` | | "" | Write a heap memory profile to this file |
| `--blockprofile` | | "" | Write a goroutine blocking profile to this file |
| `--blockprofilerate` | | 1 | Sample one blocking event per N ns of blocked time |
| `--mutexprofile` | | "" | Write a mutex contention profile to this file |
| `--mutexprofilefraction` | | 1 | Sample 1/N mutex contention events |

Use tiled rasters. Untiled/stripe rasters read whole stripes into memory and
can blow past available memory.

## How the pipeline works

The pipeline is a fan-out / hash-shuffle / fan-in of goroutines connected by
channels. Memory stays bounded by the in-flight block frontier, not by raster
size, as long as the input is tiled.

```
genBlocks  →  chan godal.Block  (row-major)
    │
    ▼
ProcessBlocks  (NumReadWorkers goroutines)
    indexBlock  →  map[cell]*cellBatch + BlockCoord
    │
    └── cellBatch → mergeWorker[hash(cell) % N].in, batched in packs of
        up to 1024 bundles; after a block's bundles are sent, a sentinel
        bundle (tagged with Indexer.SentinelCell() + BlockCoord) is
        appended to each worker's final pack for that block
mergeWorker.run()  (NumMergeWorkers goroutines)
    accumulate per cell; on sentinel, onBlockDone() flushes cells whose
    expected blocks are all done  →  worker.out
fan-in:  workers[*].out  →  resCh  →  sink (Parquet)
```

1. **Block production** (`GenBlocks`): walks the raster's blocks row-major and
   feeds them onto a channel.
2. **Block processing** (`ProcessBlocks` / `indexBlock`): `NumReadWorkers`
   goroutines read each block, map every non-nodata pixel to a DGGS cell at
   `--indexLevel` via the configured `dggs.Indexer`, and group pixels by cell
   within the block into a reused `map[uint64]*cellBatch` (pointer values, so
   repeat cells append in place rather than round-tripping through the map).
3. **Hash shuffle**: each batch is routed to `mergeWorker[hash(cellID) % N]`
   using FNV-1a over the 8 bytes of the cell ID. Cell IDs at a given level
   share high bits, so `id % N` skews for power-of-two `N`; FNV disperses.
   This guarantees all pixels for a cell land on one worker, so each cell is
   aggregated exactly once across block boundaries. Batches are buffered per
   worker and flushed to the worker's `in` channel in packs of up to
   `chanSendPackSize` (1024) to amortize channel synchronization overhead.
4. **Sentinel-based block completion**: once a read worker has sent all of a
   block's batches to a merge worker, it appends one more bundle to that
   worker's final pack — a sentinel carrying `Indexer.SentinelCell()` (an
   indexer-specific value that can never be a real cell ID) and the block's
   coordinate. Because each block is processed by exactly one read worker,
   and Go channels preserve per-sender order, the sentinel is guaranteed to
   arrive at a merge worker after all of that block's real bundles. On
   receipt, the merge worker calls `onBlockDone` for that block. This
   replaced an earlier ack-based handshake (`sync.WaitGroup` per block plus a
   separate `blockDone` channel): the sentinel needs no synchronization
   primitive and removed a full park/wake cycle per block from the hot path.
5. **Merge workers**: accumulate raw pixel values per cell and flush once all
   expected blocks for that cell are done. `Indexer.CellBBox` plus
   `Band.GetBlocksIntersectingBBox` compute which blocks a cell overlaps
   (conservative — a rectangular bound around the cell's true geometry). A
   bounded `doneBlockRing` tracks completed blocks so a cell whose expected
   block emits no batch (sliver overlap or all-nodata) still flushes instead
   of hanging. On flush, the worker applies the aggregation; cell geometry as
   WKB is deferred to the fan-in stage, which has access to the indexer.
6. **Fan-in / sink**: one forwarder goroutine per worker drains `worker.out`,
   attaches WKB via `Indexer.CellIDToWKB`, and forwards batched
   `[]IndexedCellData` packs into a single sink channel; the sink writes
   GeoParquet with Zstd compression.

### Extensive vs intensive aggregation

When a DGGS cell is smaller than a pixel, an extensive aggregation would
over-count partial pixels, so the pixel value is scaled by the cell-to-pixel
area ratio. Extensive quantities are totals that scale with area (`sum`);
intensive ones are representative values that don't (`mean`, `max`, `min`,
`mode`).

Each `AggFunc` carries its `IsExtensive` flag as a field, set where the
function is defined (`celltools/agg.go`), so the scaling decision is explicit
rather than inferred at runtime. Cell area is only computed (`Indexer.CellArea`,
which is CGO-backed for H3) when `IsExtensive` is true, since the scaling
factor is unused otherwise.

## Library use

The pipeline is importable. Primary entry point:

```go
import (
    "github.com/Terence-jo/s2-tools/celltools"
    "github.com/Terence-jo/s2-tools/cellsio"
    "github.com/Terence-jo/s2-tools/dggs"
)

indexer, err := dggs.NewS2Indexer(11) // or dggs.NewH3Indexer(resolution)

config := celltools.Config{
    NumReadWorkers:  8,
    NumMergeWorkers: 8,
    Logger:          logger, // implements celltools.Logger
    Verbose:         false,
}

sink := func(cellData <-chan []celltools.IndexedCellData) error {
    return cellsio.StreamToParquet(cellData, outDir, 8)
}

err = celltools.RunIndexingPipeline("input.tif", indexer, sink, celltools.Mean, config)
```

Also exposed for lower-level integration:

- `celltools.RasterIndexingPipeline` represents a runnable instance of the
  pipeline, with a band to read from, an indexer, a sink, etc.
- `celltools.ProcessBlocks` runs the block-to-cell merge stage and returns a
  channel of batched `[]IndexedCellData`.
- `celltools.ReadBlockToRawCells` converts one GDAL block to raw per-pixel
  `IndexedCellData`.
- `geotiff.NewBand` wraps a GDAL band with geotransform metadata.
- `dggs.NewS2Indexer` / `dggs.NewH3Indexer` construct DGGS indexers; both
  implement `dggs.Indexer` (`PointToCellID`, `CellIDToWKB`, `CellArea`,
  `CellBBox`, `SentinelCell`, ...).

Packages:

- `celltools` — raster-to-cell pipeline, merge workers, aggregations
  (`agg.go`).
- `dggs` — the `Indexer` interface and its S2 and H3 implementations: cell
  ID lookup, cell area, bounding box, and WKB geometry serialization.
- `geotiff` — `Band`, a thin wrapper over `godal.Band` exposing geotransform
  metadata, block I/O, block-to-bbox lookups, and pixel area calculations.
- `cellsio` — sink writers. `StreamToParquet` writes GeoParquet.

## Tests

```bash
go test ./...
```

Tests live in `celltools/` and cover block-to-cell mapping, cell-to-block
lookups (`Band.GetBlocksIntersectingBBox`), accumulator creation, the
`doneBlockRing`, and WKB output, for both the S2 and H3 indexers. The
race-sensitive dedup/shuffle path should be run with `go test -race`.

## Unfinished and known limitations

- **Pole handling is indexer-dependent.** `S2Indexer.CellBBox` (via
  `s2.RectBound`) does not special-case cells overlapping a pole.
  `H3Indexer.CellBBox` clamps latitude to ±90° and expands longitude to
  ±180° when a cell contains a pole, but does not handle antimeridian
  crossing. Rare for real rasters, but not fully correct in either indexer.
- **No hard rejection of untiled/coarse inputs.** The command warns about
  stripes exceeding memory, but does not error out. The bounded-memory
  guarantee also assumes a DGGS cell is smaller than a block in both axes; a
  too-coarse level makes the block frontier unbounded and is not yet
  rejected.
- **`doneBlockRing` window sizing assumes bounded out-of-orderness.** The
  sentinel protocol removed the per-block barrier that used to throttle read
  workers to roughly `NumReadWorkers` blocks in flight. Read workers can now
  race ahead bounded only by channel buffer capacity (`cellChanBufferSize`),
  so blocks can complete more out of order than the ring's `activeWindow` was
  originally tuned for. If the watermark advances past `activeWindow` before
  a lagging block's sentinel is processed, that block is silently dropped
  from the ring, which can orphan accumulators that expected it. This
  surfaces as the "orphans detected" warning at merge-worker shutdown. Watch
  for it on rasters with highly variable per-block density; increasing
  `frontierGaps` in `newBlockRing` (celltools/mergeworker.go) trades memory
  for a larger safety margin.
