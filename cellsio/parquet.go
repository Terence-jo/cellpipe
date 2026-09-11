package cellsio

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Terence-jo/s2-tools/celltools"

	"github.com/parquet-go/parquet-go"
	"github.com/sirupsen/logrus"
)

const (
	CellRowSize   = 8 + 8 + 19*5 + 11
	RowGroupSize  = 1_000_000
	RowBufferSize = 100_000
)

type CellRow struct {
	CellID int64   `parquet:"cell_id, type=INT64"`
	Value  float64 `parquet:"value, type=DOUBLE"`
	Geom   []byte  `parquet:"geometry, type=GEOGRAPHY"`
}

func StreamToParquet(cellData <-chan celltools.IndexedCellData, path string, numWorkers int) error {
	var wg sync.WaitGroup

	err := os.RemoveAll(path)
	if err != nil {
		return err
	}
	err = os.Mkdir(path, 0755)
	if err != nil {
		return err
	}
	schema := parquet.SchemaOf(new(CellRow))

	wg.Add(numWorkers)
	for i := range numWorkers {
		go func() error {
			var j int
			defer wg.Done()

			partPath := fmt.Sprintf("%s/%s-%d.parquet", path, filepath.Base(path), i)
			output, err := os.Create(partPath)
			if err != nil {
				return err
			}
			writer := parquet.NewGenericWriter[CellRow](output, schema, parquet.Compression(&parquet.Zstd), parquet.MaxRowsPerRowGroup(RowGroupSize))
			defer func() {
				if err := writer.Close(); err != nil {
					logrus.Error(err)
				}
				if err := output.Close(); err != nil {
					logrus.Error(err)
				}
			}()

			rowBatch := make([]CellRow, 0, RowBufferSize)
			for cell := range cellData {
				row := CellRow{int64(cell.ID), cell.Data, cell.WKB}
				rowBatch = append(rowBatch, row)
				flushData := ((j+1)%RowBufferSize == 0)
				if flushData {
					logrus.Infof("Writing cell %d", j)
					if _, err := writer.Write(rowBatch); err != nil {
						return err
					}
					rowBatch = make([]CellRow, 0, RowBufferSize)
				}
				j++
			}
			if _, err := writer.Write(rowBatch); err != nil {
				return err
			}
			return nil
		}()
	}
	wg.Wait()
	return nil
}
