package dggs

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/Terence-jo/s2-tools/types"

	"github.com/golang/geo/s2"
	"github.com/uber/h3-go/v4"
)

const (
	earthRadius float64 = 6371000
)

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
func (s *S2Indexer) PointToCellID(point types.LngLat) (uint64, error) {
	latlng := s2.LatLngFromDegrees(point.Lat, point.Lng)
	return uint64(s2.CellIDFromLatLng(latlng).Parent(s.level)), nil
}

func (S2Indexer) CellIDToPoint(id uint64) (types.LngLat, error) {
	latlng := s2.CellID(id).LatLng()
	return types.LngLat{Lat: latlng.Lng.Degrees(), Lng: latlng.Lat.Degrees()}, nil
}

func (S2Indexer) CellIDToWKB(id uint64) ([]byte, error) {
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

	return wkb, nil
}

func (S2Indexer) CellArea(id uint64) (float64, error) {
	return s2.CellFromCellID(s2.CellID(id)).ApproxArea() * earthRadius * earthRadius, nil
}

func (S2Indexer) CellBBox(id uint64) ([4]float64, error) {
	cellRect := s2.CellFromCellID(s2.CellID(id)).RectBound()
	return [4]float64{
		cellRect.Lo().Lng.Degrees(),
		cellRect.Lo().Lat.Degrees(),
		cellRect.Hi().Lng.Degrees(),
		cellRect.Hi().Lat.Degrees(),
	}, nil
}

func (S2Indexer) SentinelCell() uint64 {
	return uint64(s2.SentinelCellID)
}

type H3Indexer struct {
	resolution int
}

func NewH3Indexer(resolution int) (*H3Indexer, error) {
	if resolution < 0 || resolution > 15 {
		return nil, errors.New("Supplied level was outside the range of possible H3 resolutions")
	}
	return &H3Indexer{resolution}, nil
}

func (H3Indexer) Name() string {
	return "H3"
}

// Interprets point and Lng/Lat and converts to S2 cell ID
func (h *H3Indexer) PointToCellID(point types.LngLat) (uint64, error) {
	latlng := h3.NewLatLng(point.Lat, point.Lng)
	cell, err := h3.LatLngToCell(latlng, h.resolution)
	if err != nil {
		return 0, err
	}
	return uint64(cell), nil
}

func (H3Indexer) CellIDToPoint(id uint64) (types.LngLat, error) {
	latlng, err := h3.CellToLatLng(h3.Cell(id))
	if err != nil {
		return types.LngLat{}, err
	}
	return types.LngLat{Lat: latlng.Lng, Lng: latlng.Lat}, nil
}

func (H3Indexer) CellIDToWKB(id uint64) ([]byte, error) {
	cell := h3.Cell(id)
	boundary, err := cell.Boundary()
	if err != nil {
		return nil, err
	}
	numVertices := len(boundary)

	wkb := make([]byte, 0, 77)
	var littleEndianMarker byte = 1
	var polygonType uint32 = 3
	var numRings uint32 = 1
	var numPoints uint32 = uint32(numVertices)
	wkb = append(wkb, littleEndianMarker)
	wkb = binary.LittleEndian.AppendUint32(wkb, polygonType)
	wkb = binary.LittleEndian.AppendUint32(wkb, numRings)
	wkb = binary.LittleEndian.AppendUint32(wkb, numPoints)

	for k := range numVertices {
		latBits := math.Float64bits(boundary[k].Lat)
		lngBits := math.Float64bits(boundary[k].Lng)
		wkb = binary.LittleEndian.AppendUint64(wkb, lngBits)
		wkb = binary.LittleEndian.AppendUint64(wkb, latBits)
	}
	latBits := math.Float64bits(boundary[0].Lat)
	lngBits := math.Float64bits(boundary[0].Lng)
	wkb = binary.LittleEndian.AppendUint64(wkb, lngBits)
	wkb = binary.LittleEndian.AppendUint64(wkb, latBits)

	return wkb, nil
}

func (H3Indexer) CellArea(id uint64) (float64, error) {
	return h3.CellAreaM2(h3.Cell(id))
}

func (H3Indexer) CellBBox(id uint64) ([4]float64, error) {
	boundary, err := h3.Cell(id).Boundary()
	if err != nil {
		return [4]float64{}, err
	}

	bbox := [4]float64{
		boundary[0].Lng,
		boundary[0].Lat,
		boundary[0].Lng,
		boundary[0].Lat,
	}
	for i := 1; i < len(boundary); i++ {
		latlng := boundary[i]
		if latlng.Lng < bbox[0] {
			bbox[0] = latlng.Lng
		}
		if latlng.Lat < bbox[1] {
			bbox[1] = latlng.Lat
		}
		if latlng.Lng > bbox[2] {
			bbox[2] = latlng.Lng
		}
		if latlng.Lat > bbox[3] {
			bbox[3] = latlng.Lat
		}
	}

	// TODO: meridian handling

	// pole handling: clamp and expand x bounds if the pole is intersected
	containsPole := false
	if bbox[3] >= 90 {
		bbox[3] = 90
		containsPole = true
	}
	if bbox[1] <= -90 {
		bbox[1] = -90
		containsPole = true
	}
	if containsPole {
		bbox[0] = -180
		bbox[2] = 180
	}
	return bbox, nil
}

func (H3Indexer) SentinelCell() uint64 {
	return uint64(h3.InvalidH3Index)
}
