# s2-tools

CLI tools for converting geospatial rasters into S2 cell indexes. Currently
exposes one command, `indexraster`, which reads a GeoTIFF, assigns every pixel
to an S2 cell at a chosen level, aggregates the pixel values per cell, and
writes the result as GeoParquet (S2 cell ID, aggregated value, cell geometry).

Built on [godal](https://github.com/airbusgeo/godal) (GDAL bindings),
[golang/geo/s2](https://github.com/golang/geo), and
[parquet-go](https://github.com/parquet-go/parquet-go).

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
| `--s2Lvl` | `-l` | 11 | S2 cell level. Output resolution. |
| `--aggFunc` | `-a` | `mean` | Aggregation: `mean`, `sum`, `max`, `min`, `mode` |
| `--memLimitGB` | `-g` | 8 | Memory budget in GB |
| `--verbose` | `-v` | false | Info-level logs |
| `--debug` | `-d` | false | Debug-level logs |

Use tiled rasters. Untiled/stripe rasters read whole stripes into memory and
can blow past the memory budget. Compression is parsed but not tested.

## How the pipeline works

The pipeline is a fan-out / hash-shuffle / fan-in of goroutines connected by
channels. Memory stays bounded by the in-flight block frontier, not by raster
size, as long as the input is tiled.

```
genBlocks  →  chan godal.Block  (row-major)
    │
    ▼
processBlocks  (NumReadWorkers goroutines)
    rasterBlockToS2  →  map[cell]CellBatch + BlockCoord
    │
    ├── CellBatch → mergeWorker[hash(cell) % N].in   (owner acks each batch)
    └── after acks: BlockCoord → all N blockDone chans
mergeWorker.run()  (NumMergeWorkers goroutines)
    accumulate per cell, flush when all expected blocks done  →  worker.out
fan-in:  workers[*].out  →  sinkCh  →  sink (Parquet)
```

1. **Block production** (`genBlocks`): walks the raster's blocks row-major and
   feeds them onto a channel.
2. **Block processing** (`processBlocks`): `NumReadWorkers` goroutines read
   each block, map every non-nodata pixel to an S2 cell at `--s2Lvl`, and group
   pixels by cell within the block. Each cell's values become a `CellBatch`.
3. **Hash shuffle**: each batch is routed to `mergeWorker[hash(cellID) % N]`
   using FNV-1a over the 8 bytes of the cell ID. S2 cell IDs at a level share
   high bits, so `int64 % N` skews for power-of-two `N`; FNV disperses. This
   guarantees all pixels for a cell land on one worker, so each cell is
   aggregated exactly once across block boundaries.
4. **Ack handshake**: a block's done-signal is broadcast to all merge workers
   only after its batches have been consumed and acknowledged (per-block
   `sync.WaitGroup`). This closes a race where Go's nondeterministic `select`
   could deliver `done` before the batch and flush a cell twice with partial
   values.
5. **Merge workers**: accumulate raw pixel values per cell and flush once all
   expected blocks for that cell are done. `expectedBlocksForCell` computes
   which blocks a cell overlaps (conservative, via `s2.RectBound`). A bounded
   `doneBlockRing` tracks completed blocks so a cell whose expected block emits
   no batch (sliver overlap or all-nodata) still flushes instead of hanging.
   On flush, the worker applies the aggregation and emits the cell geometry as
   WKB.
6. **Fan-in / sink**: one forwarder goroutine per worker drains `worker.out`
   into a single sink channel; the sink writes GeoParquet with Zstd
   compression.

### Extensive vs intensive aggregation

When an S2 cell is smaller than a pixel, an extensive aggregation would
over-count partial pixels, so the pixel value is scaled by the cell-to-pixel
area ratio. Extensive quantities are totals that scale with area (`sum`);
intensive ones are representative values that don't (`mean`, `max`, `min`,
`mode`).

Each `AggFunc` carries its `isExtensive` flag as a field, set where the
function is defined (`celltools/agg.go`), so the scaling decision is explicit
rather than inferred at runtime.

## Library use

The pipeline is importable. Primary entry point:

```go
import "s2-tools/celltools"

opts := celltools.ConfigOpts{
    NumReadWorkers:  8,
    NumMergeWorkers: 8,
    S2Lvl:           11,
    AggFunc:         celltools.Mean,   // or Sum, Max, Min, Mode
    Verbose:         false,
}

sink := func(cellData <-chan celltools.S2CellData) error {
    return cellsio.StreamToParquet(cellData, outDir, 8, 8)
}

err := celltools.RasterToS2("input.tif", opts, sink)
```

Also exposed for lower-level integration:

- `celltools.ProcessBlocks` runs the block-to-cell merge stage and returns an
  output channel.
- `celltools.ReadBlockToCells` converts one GDAL block to raw per-pixel
  `S2CellData`.
- `celltools.NewBandContainer` wraps a GDAL band with geotransform metadata.
- `celltools.CellToWKB` serializes an S2 cell geometry to WKB.

Packages:

- `celltools` — raster-to-S2 pipeline, merge workers, aggregations, geometry
  serialization (`cellToWKB`, `cellToWKT`).
- `cellsio` — sink writers. `StreamToParquet` writes GeoParquet.

## Tests

```bash
go test ./...
```

Tests live in `celltools/` and cover block-to-cell mapping,
`expectedBlocksForCell` block sets, accumulator creation, the `doneBlockRing`,
and WKB output. The race-sensitive dedup path should be run with
`go test -race`.

## Unfinished and known limitations

- **Pole handling deferred.** `expectedBlocksForCell` does not handle cells
  overlapping a pole. Documented as a known issue; rare for real rasters.
- **No hard rejection of untiled/coarse inputs.** The command warns about
  stripes exceeding memory, but does not error out. The bounded-memory
  guarantee also assumes an S2 cell is smaller than a block in both axes; a
  too-coarse level makes the block frontier unbounded and is not yet rejected.