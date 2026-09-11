package cellsio

import (
	"os"
	"s2-tools/celltools"
	"sort"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/sirupsen/logrus"
)

const (
	CellRowSize   = 8 + 8 + 19*5 + 11
	RowGroupSize = 1000000
	RowBufferSize = 100000
)

type CellRow struct {
	S2ID  int64   `parquet:"s2_id, type=INT64"`
	Value float64 `parquet:"value, type=DOUBLE"`
	Geom  []byte  `parquet:"geometry, type=GEOGRAPHY"`
}

func StreamToParquet(cellData chan celltools.S2CellData, path string, numWorkers int, memLimitGB int) error {
	var mu sync.Mutex
	var wg sync.WaitGroup

	output, err := os.Create(path)
	if err != nil {
		return err
	}

	schema := parquet.SchemaOf(new(CellRow))
	writer := parquet.NewGenericWriter[CellRow](output, schema, parquet.Compression(&parquet.Zstd), parquet.MaxRowsPerRowGroup(RowGroupSize))
	defer func() {
		if err := writer.Close(); err != nil {
			logrus.Error(err)
		}
		if err := output.Close(); err != nil {
			logrus.Error(err)
		}
	}()

	wg.Add(numWorkers)
	for range numWorkers {
		go func() error {
			var i int
			defer wg.Done()
			buffer := parquet.NewGenericBuffer[CellRow](parquet.SortingRowGroupConfig(
				parquet.SortingColumns(parquet.Ascending("s2_id")),
			))
			rowBatch := make([]CellRow, 0, RowBufferSize)
			for cell := range cellData {
				row := CellRow{int64(cell.Cell), cell.Data, cell.WKB}
				rowBatch = append(rowBatch, row)
				flushData := ((i+1)%RowBufferSize == 0)
				if flushData {
					logrus.Infof("Writing cell %d", i)
					// Encode and sort on the parallel path
					buffer.Write(rowBatch)
					sort.Sort(buffer)
					mu.Lock()
					// Compression remains serial for now
					if _, err := parquet.CopyRows(writer, buffer.Rows()); err != nil {
						return err
					}
					mu.Unlock()
					rowBatch = make([]CellRow, 0, RowBufferSize)
					buffer.Reset()
				}
				i++
			}
			buffer.Write(rowBatch)
			sort.Sort(buffer)
			mu.Lock()
			if _, err := parquet.CopyRows(writer, buffer.Rows()); err != nil {
				return err
			}
			mu.Unlock()
			return nil
		}()
	}
	wg.Wait()
	return nil
}
