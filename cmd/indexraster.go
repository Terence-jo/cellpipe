// Package cmd /*
package cmd

import (
	"fmt"
	"s2-tools/cellsio"
	"s2-tools/celltools"
	"s2-tools/dggs"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var memLimit int
var numReadWriteWorkers int
var numMergeWorkers int
var indexLevel int

// indexrasterCmd represents the indexraster command
var indexrasterCmd = &cobra.Command{
	Use:   "indexraster [flags] inputRaster output",
	Short: "Convert a raster to S2 cells, aggregating over each cell",
	Long: `Convert a GeoTIFF to a CSV (more options to follow) file
	containing S2 cell IDs and aggregated values for the raster cells
	contained.

	Use tiled rasters for best performance. Untiled rasters can exceed
	memory limits when stripes are read. Compression is now properly
	supported, but not tested.

	Options:
		--numReadWriteWorkers: Number of workers to spawn for parallel reads and sink processing. Tune
									to manage availability of ready work for merge workers and
									write back-pressure.
		--numMergeWorkers: Number of workers to spawn for parallel processing. Not recommended
									to exceed number of CPU cores.
		--indexer: Which DGGS to use to index the raster. Currently only S2 is implemented.
		--indexLevel:			S2 cell level to generate results for. Essentially output resolution.
		--aggFunc:		Function to use when aggregating to S2 cell. Default is the mean,
									choose from: mean, sum, max, min, mode`,
	RunE: func(cmd *cobra.Command, args []string) error {
		setLogLevels()
		sink := func(cellData <-chan celltools.IndexedCellData) error {
			return cellsio.StreamToParquet(cellData, args[1], numReadWriteWorkers, memLimit)
		}

		aggFunc := chooseAggFunc(viper.GetString("aggFunc"))
		indexer, err := getIndexer(viper.GetString("indexer"), indexLevel)
		if err != nil {
			return err
		}

		config := celltools.Config{
			NumReadWorkers:  numReadWriteWorkers,
			NumMergeWorkers: numMergeWorkers,
			Verbose:         viper.GetBool("verbose"),
		}

		if len(args) == 0 {
			return fmt.Errorf("indexraster requires two arguments")
		}

		fmt.Printf("supplied path: %s\n", args[0])
		err = celltools.RunIndexingPipeline(args[0], indexer, sink, aggFunc, config)
		if err != nil {
			return err
		}

		return nil
	},
}

func getIndexer(name string, level int) (dggs.Indexer, error) {
	switch name {
	default:
		return dggs.NewS2Indexer(level)
	}
}

func chooseAggFunc(funcFlag string) celltools.AggFunc {
	switch funcFlag {
	case "mean":
		return celltools.Mean
	case "sum":
		return celltools.Sum
	case "max":
		return celltools.Max
	case "min":
		return celltools.Min
	case "mode":
		return celltools.Mode
	default:
		logrus.Warnf("Aggregation function %s not recognized, using mean", funcFlag)
		return celltools.Mean
	}
}
func setLogLevels() {
	if viper.GetBool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	} else if viper.GetBool("verbose") {
		logrus.SetLevel(logrus.InfoLevel)
	} else {
		logrus.SetLevel(logrus.WarnLevel)
	}
}

func init() {
	rootCmd.AddCommand(indexrasterCmd)

	indexrasterCmd.Flags().IntVarP(&numReadWriteWorkers, "numReadWriteWorkers", "w", 8, "Number of workers to spawn for parallel reads and sink processing")
	err := viper.BindPFlag("numReadWriteWorkers", indexrasterCmd.Flags().Lookup("numReadWriteWorkers"))
	if err != nil {
		logrus.Exit(1)
	}

	indexrasterCmd.Flags().IntVarP(&numMergeWorkers, "numMergeWorkers", "m", 8, "Number of workers to spawn for parallel processing")
	err = viper.BindPFlag("numMergeWorkers", indexrasterCmd.Flags().Lookup("numMergeWorkers"))
	if err != nil {
		logrus.Exit(1)
	}

	indexrasterCmd.Flags().IntVarP(&indexLevel, "indexLevel", "l", 11, "S2 cell level to generate results for. Essentially output resolution")
	err = viper.BindPFlag("indexLevel", indexrasterCmd.Flags().Lookup("indexLevel"))
	if err != nil {
		logrus.Exit(1)
	}

	indexrasterCmd.Flags().StringP("indexer", "i", "s2", "Indexing strategy to use. Currently implemented: S2")
	err = viper.BindPFlag("indexer", indexrasterCmd.Flags().Lookup("indexer"))
	if err != nil {
		logrus.Exit(1)
	}

	indexrasterCmd.Flags().StringP("aggFunc", "a", "mean", "Function to use when aggregating to S2 cell. Default is the mean, choose from: mean, sum, max, min, mode")
	err = viper.BindPFlag("aggFunc", indexrasterCmd.Flags().Lookup("aggFunc"))
	if err != nil {
		logrus.Exit(1)
	}

	indexrasterCmd.Flags().IntVarP(&memLimit, "memLimitGB", "g", 8, "Memory limit in GB for raster processing")
	err = viper.BindPFlag("memLimitGB", indexrasterCmd.Flags().Lookup("memLimitGB"))
	if err != nil {
		logrus.Exit(1)
	}
}
