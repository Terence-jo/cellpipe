package dggs

// An Indexer turns coordinates into cell IDs from a Discrete Global Grid System (DGGS)
type Indexer interface {
	Name() string
	PointToCellID(point Point) uint64
	CellIDToPoint(cell uint64) Point
	CellIDToWKB(cell uint64) []byte
	CellArea(cell uint64) float64
	CellBBox(cell uint64) [4]float64
}