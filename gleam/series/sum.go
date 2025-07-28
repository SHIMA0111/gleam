package series

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/math"
	"github.com/apache/arrow-go/v18/arrow/memory"

	internalCompute "github.com/SHIMA0111/gleam/internal/compute"
)

const SumThreshold = 150_000

// Sum calculates the sum of all elements in the Series, returning the result as a new Series with 64-bit number(overflow safe).
// Returns an error if unsupported.
// In arrow-go, there is a math.(Int64, UInt64, Float64).Sum, which is the optimized function with assembly.
// We use this method with cast the array data type.
// However, in a small sum execution, the Go loop is faster than the arrow sum function
// what from the overhead cast and so. (In small, the 64-bit numeric is still fastest)
// Sum uses a threshold to judge the sum operation method, go loop and cast and arrow sum.
func (s *Series) Sum() (*Series, error) {
	ctx := context.Background()
	mem := memory.DefaultAllocator

	// Drop null values from the array because the underlying math.(--).Sum() function
	// doesn't consider the null values
	arrayData, err := internalCompute.DropNullArray(ctx, s.array)
	if err != nil {
		return nil, err
	}
	defer arrayData.Release()

	var newArray arrow.Array

	switch s.DType().ID() {
	case arrow.INT8:
		// Cast the data to Int64
		var result int64
		if arrayData.Len() < SumThreshold {
			result = sumInt8Array(arrayData.(*array.Int8))
		} else {
			result, err = castSumInt(ctx, arrayData)
			if err != nil {
				return nil, err
			}
		}

		// Create a new Array builder
		b := array.NewInt64Builder(mem)
		defer b.Release()

		// Append the result
		b.Append(result)

		// Create a new array
		newArray = b.NewArray()
		defer newArray.Release()
	case arrow.INT16:
		// Cast the data to Int64
		var result int64
		if arrayData.Len() < SumThreshold {
			result = sumInt16Array(arrayData.(*array.Int16))
		} else {
			result, err = castSumInt(ctx, arrayData)
			if err != nil {
				return nil, err
			}
		}

		// Create a new Array builder
		b := array.NewInt64Builder(mem)
		defer b.Release()

		// Append the result
		b.Append(result)

		// Create a new array
		newArray = b.NewArray()
		defer newArray.Release()
	case arrow.INT32:
		// Cast the data to Int64
		var result int64
		if arrayData.Len() < SumThreshold {
			result = sumInt32Array(arrayData.(*array.Int32))
		} else {
			result, err = castSumInt(ctx, arrayData)
			if err != nil {
				return nil, err
			}
		}

		// Create a new Array builder
		b := array.NewInt64Builder(mem)
		defer b.Release()

		// Append the result
		b.Append(result)

		// Create a new array
		newArray = b.NewArray()
		defer newArray.Release()
	case arrow.INT64:
		i64Array, ok := arrayData.(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("failed to cast the array to Int64 from %s: %w", s.DType(), err)
		}

		// Sum the array and the result should be int64
		result := math.Int64.Sum(i64Array)

		// Create a new Array builder
		b := array.NewInt64Builder(mem)
		defer b.Release()

		// Append the result
		b.Append(result)

		// Create a new array
		newArray = b.NewArray()
		defer newArray.Release()
	case arrow.UINT8:
		var result uint64

		if arrayData.Len() < SumThreshold {
			result = sumUInt8Array(arrayData.(*array.Uint8))
		} else {
			result, err = castSumUInt(ctx, arrayData)
			if err != nil {
				return nil, err
			}
		}

		// Create a new array builder
		b := array.NewUint64Builder(mem)
		defer b.Release()

		// Append the result
		b.Append(result)

		// Create a new array
		newArray = b.NewArray()
		defer newArray.Release()
	case arrow.UINT16:
		var result uint64

		if arrayData.Len() < SumThreshold {
			result = sumUInt16Array(arrayData.(*array.Uint16))
		} else {
			result, err = castSumUInt(ctx, arrayData)
			if err != nil {
				return nil, err
			}
		}

		// Create a new array builder
		b := array.NewUint64Builder(mem)
		defer b.Release()

		// Append the result
		b.Append(result)

		// Create a new array
		newArray = b.NewArray()
		defer newArray.Release()
	case arrow.UINT32:
		var result uint64

		if arrayData.Len() < SumThreshold {
			result = sumUInt32Array(arrayData.(*array.Uint32))
		} else {
			result, err = castSumUInt(ctx, arrayData)
		}

		// Create a new array builder
		b := array.NewUint64Builder(mem)
		defer b.Release()

		// Append the result
		b.Append(result)

		// Create a new array
		newArray = b.NewArray()
		defer newArray.Release()
	case arrow.UINT64:
		u64Array, ok := arrayData.(*array.Uint64)
		if !ok {
			return nil, fmt.Errorf("failed to cast the array to Uint64 from %s: %w", s.DType(), err)
		}

		// Calculate the sum of an array
		result := math.Uint64.Sum(u64Array)

		// Create a new array builder
		b := array.NewUint64Builder(mem)
		defer b.Release()

		// Append the result
		b.Append(result)

		// Create a new array
		newArray = b.NewArray()
		defer newArray.Release()
	case arrow.FLOAT32:
		var result float64

		if arrayData.Len() < SumThreshold {
			result = sumFloat32Array(arrayData.(*array.Float32))
		} else {
			result, err = castSumFloat(ctx, arrayData)
		}

		// Create a new array builder
		b := array.NewFloat64Builder(mem)
		defer b.Release()

		// Append the result
		b.Append(result)

		// Create a new array
		newArray = b.NewArray()
		defer newArray.Release()
	case arrow.FLOAT64:
		f64Array, ok := arrayData.(*array.Float64)
		if !ok {
			return nil, fmt.Errorf("failed to cast the array to Float64 from %s: %w", s.DType(), err)
		}

		// Calculate the sum of an array
		result := math.Float64.Sum(f64Array)

		// Create a new array builder
		b := array.NewFloat64Builder(mem)
		defer b.Release()

		// Append the result
		b.Append(result)

		// Create a new array
		newArray = b.NewArray()
		defer newArray.Release()
	default:
		// If the data type is not supported, return an error
		return nil, fmt.Errorf("sum is not supported for %s", s.DType())
	}

	return NewSeries(s.name, newArray), nil
}

func castSumInt(ctx context.Context, arr arrow.Array) (int64, error) {
	castedArray, err := compute.CastToType(ctx, arr, arrow.PrimitiveTypes.Int64)
	if err != nil {
		return 0, err
	}
	defer castedArray.Release()

	// Cast arrow.array to *array.Int64
	i64Array, ok := castedArray.(*array.Int64)
	if !ok {
		return 0, fmt.Errorf("failed to cast the array to Int64 from %s: %w", arr.DataType(), err)
	}
	defer i64Array.Release()

	return math.Int64.Sum(i64Array), nil
}

func castSumUInt(ctx context.Context, arr arrow.Array) (uint64, error) {
	// Cast the data to UInt64
	castedArray, err := compute.CastToType(ctx, arr, arrow.PrimitiveTypes.Uint64)
	if err != nil {
		return 0, err
	}
	defer castedArray.Release()

	// Convert to array to *array.Uint64
	u64Array := castedArray.(*array.Uint64)
	defer u64Array.Release()

	// Calculate the sum of an array
	return math.Uint64.Sum(u64Array), nil
}

func castSumFloat(ctx context.Context, arr arrow.Array) (float64, error) {
	castedArray, err := compute.CastToType(ctx, arr, arrow.PrimitiveTypes.Float64)
	if err != nil {
		return 0, err
	}
	defer castedArray.Release()

	f64Array := castedArray.(*array.Float64)
	defer f64Array.Release()

	return math.Float64.Sum(f64Array), nil
}
