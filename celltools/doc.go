// Package celltools converts geospatial rasters into S2 cell indices.
//
// RasterToS2 reads a GeoTIFF, maps each non-nodata pixel to an S2 cell at a
// chosen level, and aggregates pixel values per cell. The pipeline fans out
// across goroutines: block readers produce CellBatches, a hash shuffle (FNV-1a
// over the cell ID) routes each cell's pixels to a single merge worker so cells
// straddling block boundaries aggregate exactly once, and an ack-based
// block-done handshake keeps that routing race-free. Memory stays bounded by
// the in-flight block frontier, not the raster size, for tiled inputs.
//
// Aggregations are values of type AggFunc: Mean, Sum, Max, Min, Mode. Each
// function carries an isExtensive flag set in agg.go. Sum is extensive
// (scales with area); Mean, Max, Min, and Mode are intensive. When an S2 cell
// is smaller than a pixel, extensive aggregations scale the pixel value by the
// cell-to-pixel area ratio to avoid over-counting partial pixels.
//
// Geometry helpers serialize S2 cell geometries as WKB (cellToWKB) and WKT
// (cellToWKT).
package celltools
