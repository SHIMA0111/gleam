package series

import (
	"github.com/apache/arrow-go/v18/arrow/array"
)

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
