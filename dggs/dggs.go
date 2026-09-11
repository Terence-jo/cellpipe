package dggs

import (
	"encoding/binary"
	"errors"
	"math"
	"s2-tools/geotiff"

	"github.com/golang/geo/s2"
)

const (
	earthRadius float64 = 6371000
)

// An Indexer turns coordinates into cell IDs from a Discrete Global Grid System (DGGS)
type Indexer interface {
	Name() string
	PointToCellID(point geotiff.LngLat) uint64
	CellIDToPoint(cell uint64) geotiff.LngLat
	CellIDToWKB(cell uint64) []byte
	CellArea(cell uint64) float64
	CellBBox(cell uint64) [4]float64
}

type S2Indexer struct {
	level int
}

func NewS2Indexer(level int) (*S2Indexer, error) {
	if level < 0 || level > 30 {
		return nil, errors.New("Supplied level was outside the range of possible S2 levels")
	}
	return &S2Indexer{level}, nil
}

func (S2Indexer) Name() string {
	return "S2"
}

// Interprets point and Lng/Lat and converts to S2 cell ID
func (s *S2Indexer) PointToCellID(point geotiff.LngLat) uint64 {
	latlng := s2.LatLngFromDegrees(point.Lat, point.Lng)
	return uint64(s2.CellIDFromLatLng(latlng).Parent(s.level))
}

func (S2Indexer) CellIDToPoint(id uint64) geotiff.LngLat {
	latlng := s2.CellID(id).LatLng()
	return geotiff.LngLat{Lat: latlng.Lng.Degrees(), Lng: latlng.Lat.Degrees()}
}

func (S2Indexer) CellIDToWKB(id uint64) []byte {
	cell := s2.CellFromCellID(s2.CellID(id))
	wkb := make([]byte, 0, 77)
	var littleEndianMarker byte = 1
	var polygonType uint32 = 3
	var numRings uint32 = 1
	var numPoints uint32 = 5
	wkb = append(wkb, littleEndianMarker)
	wkb = binary.LittleEndian.AppendUint32(wkb, polygonType)
	wkb = binary.LittleEndian.AppendUint32(wkb, numRings)
	wkb = binary.LittleEndian.AppendUint32(wkb, numPoints)

	for k := range 4 {
		latlng := s2.LatLngFromPoint(cell.Vertex(k))
		latBits := math.Float64bits(latlng.Lat.Degrees())
		lngBits := math.Float64bits(latlng.Lng.Degrees())
		wkb = binary.LittleEndian.AppendUint64(wkb, lngBits)
		wkb = binary.LittleEndian.AppendUint64(wkb, latBits)
	}
	latlng := s2.LatLngFromPoint(cell.Vertex(0))
	latBits := math.Float64bits(latlng.Lat.Degrees())
	lngBits := math.Float64bits(latlng.Lng.Degrees())
	wkb = binary.LittleEndian.AppendUint64(wkb, lngBits)
	wkb = binary.LittleEndian.AppendUint64(wkb, latBits)

	return wkb
}

func (S2Indexer) CellArea(id uint64) float64 {
	return s2.CellFromCellID(s2.CellID(id)).ApproxArea() * earthRadius * earthRadius
}

func (S2Indexer) CellBBox(id uint64) [4]float64 {
	cellRect := s2.CellFromCellID(s2.CellID(id)).RectBound()
	return [4]float64{
		cellRect.Lo().Lng.Degrees(),
		cellRect.Lo().Lat.Degrees(),
		cellRect.Hi().Lng.Degrees(),
		cellRect.Hi().Lat.Degrees(),
	}
}
