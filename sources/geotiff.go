package sources

import (
	"math"
	"sync"

	"github.com/Terence-jo/s2-tools/types"

	"github.com/airbusgeo/godal"
)

const (
	EarthRadius float64 = 6371000
)

// Band is a thin wrapper over a godal.Band, including a mutex for concurrent readers, and the GeoTransform, which is
// otherwise only available at the scope of the godal.Dataset. It exposes some convenience functions for handling the transform.
type Band struct {
	*sync.Mutex
	godal.Band
	geoTransform [6]float64
	Structure    godal.BandStructure
}

// Origin retrieves the origin of the raster from the GeoTransform, usually the top-left
func (b *Band) Origin() types.LngLat {
	return types.LngLat{Lng: b.geoTransform[0], Lat: b.geoTransform[3]}
}

// Resolution returns an xRes, yRes tuple derived from the GeoTransform. yRes is commonly negative.
func (b *Band) Resolution() (float64, float64) {
	xRes := b.geoTransform[1]
	yRes := b.geoTransform[5]
	return xRes, yRes
}

func (b *Band) GetBlocksIntersectingBBox(bbox [4]float64) []types.BlockCoord {
	// need to get the cell rectangle first, and safely convert from radians. two key cases to account for:
	// 1. Anti-meridian - spanning -180/180deg longitude may lead to a cell wrapping back around to the other side of the raster
	// 2. Pole - A cell overlapping 90 or -90 deg latitude will potentially span many blocks on the top or bottom rows
	// defer pole handling, it is **extremely** rare for a raster to overlap the pole. document known issue.
	blockRect := b.toBlockRectangle(bbox)

	expectedBlocks := make([]types.BlockCoord, 0)
	for i := blockRect[0]; i <= blockRect[2]; i++ {
		for j := blockRect[1]; j <= blockRect[3]; j++ {
			expectedBlocks = append(expectedBlocks, types.BlockCoord{I: i, J: j})
		}
	}
	return expectedBlocks
}

func (b *Band) NumBlocks() (int, int) {
	return b.Structure.BlockCount()
}

func (b *Band) BlockSize(block types.BlockCoord) (int, int) {
	return b.Structure.ActualBlockSize(block.I, block.J)
}

func (b *Band) GenerateBlocks() <-chan types.BlockCoord {
	blocks := make(chan types.BlockCoord)
	numXBlocks, numYBlocks := b.NumBlocks()

	go func() {
		defer close(blocks)
		for j := 0; j < numYBlocks; j++ {
			for i := 0; i < numXBlocks; i++ {
				blocks <- types.BlockCoord{I: i, J: j}
			}
		}
	}()
	return blocks
}

func (b *Band) ReadBlock(block types.BlockCoord, buf []float64) (types.LngLat, error) {
	xRes, yRes := b.Resolution()
	actualXSize, actualYSize := b.Structure.ActualBlockSize(block.I, block.J)
	gBlock := godal.Block{
		X0: block.I * b.Structure.BlockSizeX,
		Y0: block.J * b.Structure.BlockSizeY,
		W:  actualXSize,
		H:  actualYSize,
	}
	blockOrigin, err := BlockOrigin(gBlock, []float64{xRes, yRes}, b.Origin())
	if err != nil {
		return types.LngLat{}, err
	}
	// Read band into blockBuf
	if err := b.LockedBlockRead(gBlock, buf); err != nil {
		return types.LngLat{}, err
	}
	return blockOrigin, nil
}

func (b *Band) NoData() float64 {
	nodata, ok := b.Band.NoData()
	if !ok {
		nodata = math.NaN()
	}
	return nodata
}

// toBlockRectangle takes a rectangle described by an array of [minX, minY, maxX, maxY] and calculates the horizontal and vertical block ranges
// it overlaps in the supplied raster. The return is another[minX, minY, maxX, maxY], described in integer block coordinates rather than the
// input raster's coordinate reference system.
func (b *Band) toBlockRectangle(rect [4]float64) [4]int {
	xRes, yRes := b.Resolution()
	pixMinCol := math.Floor((rect[0] - b.Origin().Lng) / xRes)
	pixMaxCol := math.Ceil((rect[2] - b.Origin().Lng) / xRes)
	// Convention with rasters is for Y resolution to be negative, with the top-left corner as the origin. This makes the minimum Y value the
	// maximum number of rows down, and the maximum Y value (less negative) the minimum number of rows down.
	pixMinRow := math.Floor((rect[3] - b.Origin().Lat) / yRes)
	pixMaxRow := math.Ceil((rect[1] - b.Origin().Lat) / yRes)

	numXBlocks, numYBlocks := b.Structure.BlockCount()
	return [4]int{
		max(0, int(pixMinCol)/b.Structure.BlockSizeX),
		max(0, int(pixMinRow)/b.Structure.BlockSizeY),
		min(numXBlocks-1, int(pixMaxCol)/b.Structure.BlockSizeX),
		min(numYBlocks-1, int(pixMaxRow)/b.Structure.BlockSizeY),
	}
}

func (b *Band) LockedBlockRead(block godal.Block, blockBuf []float64) error {
	b.Lock()
	defer b.Unlock()
	if err := b.Read(block.X0, block.Y0, blockBuf, block.W, block.H); err != nil {
		return err
	}
	return nil
}

func NewBand(ds *godal.Dataset, bandIdx int) (*Band, error) {
	gt, err := ds.GeoTransform()
	if err != nil {
		return nil, err
	}
	band := ds.Bands()[bandIdx]
	return &Band{&sync.Mutex{}, band, gt, band.Structure()}, nil

}

func BlockOrigin(rasterBlock godal.Block, resolution []float64, origin types.LngLat) (types.LngLat, error) {
	originLng := float64(rasterBlock.X0)*resolution[0] + origin.Lng
	originLat := float64(rasterBlock.Y0)*resolution[1] + origin.Lat
	return types.LngLat{Lng: originLng, Lat: originLat}, nil
}

func PixelArea(latitude float64, resolution float64) float64 {
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
