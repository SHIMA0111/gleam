package utils

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	arrayMath "github.com/apache/arrow-go/v18/arrow/math"
)

type Number64 interface {
	~int64 | ~uint64 | ~float64
}

func CheckOverflowAndConvertToFloat64[T Number64](v T) (float64, error) {
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

func SumInt8Array(arr *array.Int8) int64 {
	var sum int64
	for i := 0; i < arr.Len(); i++ {
		sum += int64(arr.Value(i))
	}
	return sum
}

func SumInt16Array(arr *array.Int16) int64 {
	var sum int64
	for i := 0; i < arr.Len(); i++ {
		sum += int64(arr.Value(i))
	}

	return sum
}

func SumInt32Array(arr *array.Int32) int64 {
	var sum int64
	for i := 0; i < arr.Len(); i++ {
		sum += int64(arr.Value(i))
	}

	return sum
}

func SumUInt8Array(arr *array.Uint8) uint64 {
	var sum uint64
	for i := 0; i < arr.Len(); i++ {
		sum += uint64(arr.Value(i))
	}

	return sum
}

func SumUInt16Array(arr *array.Uint16) uint64 {
	var sum uint64
	for i := 0; i < arr.Len(); i++ {
		sum += uint64(arr.Value(i))
	}

	return sum
}

func SumUInt32Array(arr *array.Uint32) uint64 {
	var sum uint64
	for i := 0; i < arr.Len(); i++ {
		sum += uint64(arr.Value(i))
	}

	return sum
}

func SumFloat32Array(arr *array.Float32) float64 {
	var sum float64
	for i := 0; i < arr.Len(); i++ {
		sum += float64(arr.Value(i))
	}

	return sum
}

func CastSumInt(ctx context.Context, arr arrow.Array) (float64, error) {
	op := compute.NewCastOptions(arrow.PrimitiveTypes.Int64, true)
	castedArray, err := compute.CastArray(ctx, arr, op)

	if err != nil {
		return 0, err
	}
	defer castedArray.Release()

	// Cast arrow.array to *array.Int64
	i64Array, ok := castedArray.(*array.Int64)
	if !ok {
		return 0, fmt.Errorf("failed to cast the array to Int64 from %s: %w", arr.DataType(), err)
	}
	sumIntValue := arrayMath.Int64.Sum(i64Array)

	// To abstraction, all sum functions return float64, but float64's significant digits are 53 digits
	// 53 digits are enough in the real world because the value is a cornucopia
	if 1<<53 <= sumIntValue {
		return 0, fmt.Errorf("overflow: %d", sumIntValue)
	}
	return CheckOverflowAndConvertToFloat64[int64](sumIntValue)
}

func CastSumUInt(ctx context.Context, arr arrow.Array) (float64, error) {
	// Cast the data to UInt64
	castedArray, err := compute.CastToType(ctx, arr, arrow.PrimitiveTypes.Uint64)
	if err != nil {
		return 0, err
	}
	defer castedArray.Release()

	// Convert to array to *array.Uint64
	u64Array, ok := castedArray.(*array.Uint64)
	if !ok {
		return 0, fmt.Errorf("failed to cast the array to Uint64 from %s: %w", arr.DataType(), err)
	}
	sumUIntValue := arrayMath.Uint64.Sum(u64Array)

	return CheckOverflowAndConvertToFloat64[uint64](sumUIntValue)
}

func CastSumFloat(ctx context.Context, arr arrow.Array) (float64, error) {
	castedArray, err := compute.CastToType(ctx, arr, arrow.PrimitiveTypes.Float64)
	if err != nil {
		return 0, err
	}
	defer castedArray.Release()

	f64Array := castedArray.(*array.Float64)
	sumFloatValue := arrayMath.Float64.Sum(f64Array)

	return CheckOverflowAndConvertToFloat64[float64](sumFloatValue)
}
