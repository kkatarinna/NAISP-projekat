package bloom

import "math"

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func CalculateElem(m uint, falsePositiveRate float64) uint {
	return uint((float64(m) * math.Pow(math.Log(2), float64(2))) / math.Abs(math.Log(falsePositiveRate)))
}
