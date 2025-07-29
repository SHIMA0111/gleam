package series

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow/array"
	"math"
)

type Number64 interface {
	~int64 | ~uint64 | ~float64
}

func checkOverflowAndConvertToFloat64[T Number64](v T) (float64, error) {
	floatingValue := float64(v)
	if math.IsNaN(floatingValue) || math.IsInf(floatingValue, 0) {
		return 0, fmt.Errorf("infinity or nan float detected: %v", v)
	}

	// Float64's significant digits are 2^53 (-2^53), and 2^53 is enough in the almost case in the real world.
	// In data analytics, if the data is over this value, you may do normalize/standardize the data.
	if 1<<53 <= math.Abs(floatingValue) {
		return floatingValue, fmt.Errorf("overflow detected over 53 digits: %d", uint64(v))
	}

	return floatingValue, nil
}

func sumInt8Array(arr *array.Int8) int64 {
	var sum int64
	for i := 0; i < arr.Len(); i++ {
		sum += int64(arr.Value(i))
	}
	return sum
}

func sumInt16Array(arr *array.Int16) int64 {
	var sum int64
	for i := 0; i < arr.Len(); i++ {
		sum += int64(arr.Value(i))
	}

	return sum
}

func sumInt32Array(arr *array.Int32) int64 {
	var sum int64
	for i := 0; i < arr.Len(); i++ {
		sum += int64(arr.Value(i))
	}

	return sum
}

func sumUInt8Array(arr *array.Uint8) uint64 {
	var sum uint64
	for i := 0; i < arr.Len(); i++ {
		sum += uint64(arr.Value(i))
	}

	return sum
}

func sumUInt16Array(arr *array.Uint16) uint64 {
	var sum uint64
	for i := 0; i < arr.Len(); i++ {
		sum += uint64(arr.Value(i))
	}

	return sum
}

func sumUInt32Array(arr *array.Uint32) uint64 {
	var sum uint64
	for i := 0; i < arr.Len(); i++ {
		sum += uint64(arr.Value(i))
	}

	return sum
}

func sumFloat32Array(arr *array.Float32) float64 {
	var sum float64
	for i := 0; i < arr.Len(); i++ {
		sum += float64(arr.Value(i))
	}

	return sum
}
