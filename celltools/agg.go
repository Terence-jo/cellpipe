package celltools

import "math"

type AggFunc struct {
	Apply       func(...float64) float64
	IsExtensive bool
}

var Mean = AggFunc{
	Apply: func(inData ...float64) float64 {
		sum := Sum.Apply(inData...)
		return sum / float64(len(inData))
	},
	IsExtensive: false,
}

var Sum = AggFunc{
	Apply: func(inData ...float64) float64 {
		var sum float64
		for _, val := range inData {
			sum += val
		}
		return sum
	},
	IsExtensive: true,
}

var Max = AggFunc{
	Apply: func(inData ...float64) float64 {
		if len(inData) == 0 {
			return math.NaN()
		}
		maxVal := inData[0]
		for _, val := range inData {
			if val > maxVal {
				maxVal = val
			}
		}
		return maxVal
	},
	IsExtensive: false,
}

var Min = AggFunc{
	Apply: func(inData ...float64) float64 {
		if len(inData) == 0 {
			return math.NaN()
		}
		minVal := inData[0]
		for _, val := range inData[1:] {
			if val < minVal {
				minVal = val
			}
		}
		return minVal
	},
	IsExtensive: false,
}

var Mode = AggFunc{
	Apply: func(inData ...float64) float64 {
		counts := make(map[float64]int)
		for _, val := range inData {
			counts[val]++
		}
		var mode float64
		var maxCount int
		for val, count := range counts {
			if count > maxCount {
				mode = val
				maxCount = count
			}
		}
		return mode
	},
	IsExtensive: false,
}
