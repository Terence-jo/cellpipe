// Package cmd /*
package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/Terence-jo/s2-tools/cellsio"
	"github.com/Terence-jo/s2-tools/celltools"
	"github.com/Terence-jo/s2-tools/dggs"
	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	numReadWriteWorkers  int
	numMergeWorkers      int
	indexLevel           int
	cpuProfile           string
	memProfile           string
	blockProfile         string
	blockProfileRate     int
	mutexProfile         string
	mutexProfileFraction int
	logger               *slog.Logger
)

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
		if cpuProfile != "" {
			f, err := os.Create(cpuProfile)
			if err != nil {
				return fmt.Errorf("could not create CPU profile: %w", err)
			}
			defer f.Close()
			if err := pprof.StartCPUProfile(f); err != nil {
				return fmt.Errorf("could not start CPU profile: %w", err)
			}
			defer pprof.StopCPUProfile()
		}
		if memProfile != "" {
			f, err := os.Create(memProfile)
			if err != nil {
				return fmt.Errorf("could not create memory profile: %w", err)
			}
			defer f.Close()
			defer func() {
				if err := pprof.Lookup("heap").WriteTo(f, 0); err != nil {
					logger.Error("could not write memory profile", "err", err)
				}
			}()
		}
		if blockProfile != "" {
			runtime.SetBlockProfileRate(blockProfileRate)
			defer runtime.SetBlockProfileRate(0)
			f, err := os.Create(blockProfile)
			if err != nil {
				return fmt.Errorf("could not create block profile: %w", err)
			}
			defer f.Close()
			defer func() {
				if err := pprof.Lookup("block").WriteTo(f, 0); err != nil {
					logger.Error("could not write block profile", "err", err)
				}
			}()
		}
		if mutexProfile != "" {
			runtime.SetMutexProfileFraction(mutexProfileFraction)
			defer runtime.SetMutexProfileFraction(0)
			f, err := os.Create(mutexProfile)
			if err != nil {
				return fmt.Errorf("could not create mutex profile: %w", err)
			}
			defer f.Close()
			defer func() {
				if err := pprof.Lookup("mutex").WriteTo(f, 0); err != nil {
					logger.Error("could not write mutex profile", "err", err)
				}
			}()
		}
		sink := func(cellData <-chan []celltools.IndexedCellData) error {
			return cellsio.StreamToParquet(cellData, args[1], numReadWriteWorkers)
		}

		aggFunc := chooseAggFunc(viper.GetString("aggFunc"))
		indexer, err := getIndexer(viper.GetString("indexer"), indexLevel)
		if err != nil {
			return err
		}

		config := celltools.Config{
			NumReadWorkers:  numReadWriteWorkers,
			NumMergeWorkers: numMergeWorkers,
			Logger:          logger,
			Verbose:         viper.GetBool("verbose"),
		}

		if len(args) == 0 {
			return fmt.Errorf("indexraster requires two arguments")
		}

		err = celltools.RunIndexingPipeline(args[0], indexer, sink, aggFunc, config)
		if err != nil {
			return err
		}

		return nil
	},
}

func getIndexer(name string, level int) (dggs.Indexer, error) {
	name = strings.ToLower(name)
	switch name {
	case "s2":
		return dggs.NewS2Indexer(level)
	case "h3":
		return dggs.NewH3Indexer(level)
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
	level := slog.LevelWarn
	if viper.GetBool("debug") {
		level = slog.LevelDebug
	} else if viper.GetBool("verbose") {
		level = slog.LevelInfo
	}
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	logger = slog.New(handler)
}

func init() {
	rootCmd.AddCommand(indexrasterCmd)
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})
	logger = slog.New(handler)

	indexrasterCmd.Flags().IntVarP(&numReadWriteWorkers, "numReadWriteWorkers", "w", 8, "Number of workers to spawn for parallel reads and sink processing")
	err := viper.BindPFlag("numReadWriteWorkers", indexrasterCmd.Flags().Lookup("numReadWriteWorkers"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	indexrasterCmd.Flags().IntVarP(&numMergeWorkers, "numMergeWorkers", "m", 8, "Number of workers to spawn for parallel processing")
	err = viper.BindPFlag("numMergeWorkers", indexrasterCmd.Flags().Lookup("numMergeWorkers"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	indexrasterCmd.Flags().IntVarP(&indexLevel, "indexLevel", "l", 11, "S2 cell level to generate results for. Essentially output resolution")
	err = viper.BindPFlag("indexLevel", indexrasterCmd.Flags().Lookup("indexLevel"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	indexrasterCmd.Flags().StringP("indexer", "i", "s2", "Indexing strategy to use. Currently implemented: S2")
	err = viper.BindPFlag("indexer", indexrasterCmd.Flags().Lookup("indexer"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	indexrasterCmd.Flags().StringP("aggFunc", "a", "mean", "Function to use when aggregating to S2 cell. Default is the mean, choose from: mean, sum, max, min, mode")
	err = viper.BindPFlag("aggFunc", indexrasterCmd.Flags().Lookup("aggFunc"))
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	indexrasterCmd.Flags().StringVarP(&cpuProfile, "cpuprofile", "p", "", "Write a CPU profile to this file (for profiling with `go tool pprof`)")
	indexrasterCmd.Flags().StringVar(&memProfile, "memprofile", "", "Write a heap memory profile to this file (for profiling with `go tool pprof`)")

	indexrasterCmd.Flags().StringVar(&blockProfile, "blockprofile", "", "Write a goroutine blocking profile to this file (for profiling with `go tool pprof`)")
	indexrasterCmd.Flags().IntVar(&blockProfileRate, "blockprofilerate", 1, "Sample one blocking event per N nanoseconds of blocked time (passed to runtime.SetBlockProfileRate); 1 samples every event")

	indexrasterCmd.Flags().StringVar(&mutexProfile, "mutexprofile", "", "Write a mutex contention profile to this file (for profiling with `go tool pprof`)")
	indexrasterCmd.Flags().IntVar(&mutexProfileFraction, "mutexprofilefraction", 1, "Sample 1/N mutex contention events (passed to runtime.SetMutexProfileFraction); 1 samples every event")
}
