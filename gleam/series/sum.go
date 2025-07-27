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

// Sum calculates the sum of all elements in the Series, returning the result as a new Series with 64-bit number(overflow safe).
// Returns an error if unsupported.
// In arrow-go, there is a math.(Int64, UInt64, Float64).Sum, which is the optimized function with assembly.
// We use this method with cast the array data type.
// However, in a simple sum execution, the Go loop is faster than the arrow sum function.
// TODO: Consider which is the best way to implement Sum, Arrow Sum + Cast or Go loop sum
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
	case arrow.INT8, arrow.INT16, arrow.INT32:
		// Cast the data to Int64
		castedArray, err := compute.CastToType(ctx, arrayData, arrow.PrimitiveTypes.Int64)
		if err != nil {
			return nil, err
		}
		defer castedArray.Release()

		// Cast arrow.array to *array.Int64
		i64Array, ok := castedArray.(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("failed to cast the array to Int64 from %s: %w", s.DType(), err)
		}
		defer i64Array.Release()

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
	case arrow.INT64:
		i64Array, ok := s.array.(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("failed to cast the array to Int64 from %s: %w", s.DType(), err)
		}
		defer i64Array.Release()

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
	case arrow.UINT8, arrow.UINT16, arrow.UINT32:
		// Cast the data to UInt64
		castedArray, err := compute.CastToType(ctx, arrayData, arrow.PrimitiveTypes.Uint64)
		if err != nil {
			return nil, err
		}
		defer castedArray.Release()

		// Convert to array to *array.Uint64
		u64Array := castedArray.(*array.Uint64)
		defer u64Array.Release()

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
	case arrow.UINT64:
		u64Array, ok := s.array.(*array.Uint64)
		if !ok {
			return nil, fmt.Errorf("failed to cast the array to Uint64 from %s: %w", s.DType(), err)
		}
		defer u64Array.Release()

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
		// Cast the data to Float64
		castedArray, err := compute.CastToType(ctx, arrayData, arrow.PrimitiveTypes.Float64)
		if err != nil {
			return nil, err
		}
		defer castedArray.Release()

		// Convert to array to *array.Float64
		f64Array := castedArray.(*array.Float64)
		defer f64Array.Release()

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
	case arrow.FLOAT64:
		f64Array, ok := s.array.(*array.Float64)
		if !ok {
			return nil, fmt.Errorf("failed to cast the array to Float64 from %s: %w", s.DType(), err)
		}
		defer f64Array.Release()

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
