package celltools

import "math"

type AggFunc struct {
	apply       func(...float64) float64
	isExtensive bool
}

var Mean = AggFunc{
	apply: func(inData ...float64) float64 {
		sum := Sum.apply(inData...)
		return sum / float64(len(inData))
	},
	isExtensive: false,
}

var Sum = AggFunc{
	apply: func(inData ...float64) float64 {
		var sum float64
		for _, val := range inData {
			sum += val
		}
		return sum
	},
	isExtensive: true,
}

var Max = AggFunc{
	apply: func(inData ...float64) float64 {
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
	isExtensive: false,
}

var Min = AggFunc{
	apply: func(inData ...float64) float64 {
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
	isExtensive: false,
}

var Mode = AggFunc{
	apply: func(inData ...float64) float64 {
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
	isExtensive: false,
}
