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
	"github.com/SHIMA0111/gleam/internal/utils"
)

// Sum calculates the sum of all elements in the Series, returning the result as a new Series. Returns an error if unsupported.
// In arrow-go, there is a math.(Int64, UInt64, Float64).Sum, which is the maximum optimized function.
// We use this method with cast the array data type.
// If sometimes this is an obstacle to flexibility, we consider it refactor with kernel functions.
func (s *Series) Sum() (*Series, error) {
	ctx := context.Background()
	mem := memory.DefaultAllocator

	var arrayData arrow.Array
	var err error
	if s.NullCount() > 0 {
		// Drop null values from the array because the underlying math.(--).Sum() function
		// doesn't consider the null values
		arrayData, err = internalCompute.DropNullArray(ctx, s.array)
		if err != nil {
			return nil, err
		}
		defer arrayData.Release()
	} else {
		arrayData = s.array
	}

	switch s.DType().ID() {
	// Int64.Sum() can cover INT8/INT16/INT32/INT64
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
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

		// Check the overflow for the original data type
		if utils.IsIntOverflow(result, utils.GetBitLenFromDataType(s.DType())) {
			return nil, fmt.Errorf("overflow: the array sum = %d in %s", result, s.DType())
		}

		// Create a new Array builder
		b := array.NewInt64Builder(mem)
		defer b.Release()

		// Append the result
		b.Append(result)

		// Create a new array
		newArray := b.NewArray()
		defer newArray.Release()

		// Overflow safe: Already check the overflow value
		resultArray, err := compute.CastToType(ctx, newArray, s.DType())
		if err != nil {
			return nil, fmt.Errorf("failed to cast the result to %s: %w", s.DType(), err)
		}
		defer resultArray.Release()

		return NewSeries(s.name, resultArray), nil

	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
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

		// Check the overflow for the original data type
		if utils.IsUintOverflow(result, utils.GetBitLenFromDataType(s.DType())) {
			return nil, fmt.Errorf("overflow: the array sum = %d in %s", result, s.DType())
		}

		// Create a new array builder
		b := array.NewUint64Builder(mem)
		defer b.Release()

		// Append the result
		b.Append(result)

		// Create a new array
		newArray := b.NewArray()
		defer newArray.Release()

		// Overflow safe: Check the overflow prior so this convert is value safe
		resultArray, err := compute.CastToType(ctx, newArray, s.DType())
		if err != nil {
			return nil, fmt.Errorf("failed to cast the result to %s: %w", s.DType(), err)
		}
		defer resultArray.Release()

		return NewSeries(s.name, resultArray), nil
	case arrow.FLOAT32, arrow.FLOAT64:
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

		if utils.IsFloatOverflow(result, utils.GetBitLenFromDataType(s.DType())) {
			return nil, fmt.Errorf("overflow: the array sum = %f in %s", result, s.DType())
		}

		// Create a new array builder
		b := array.NewFloat64Builder(mem)
		defer b.Release()

		// Append the result
		b.Append(result)

		// Create a new array
		newArray := b.NewArray()
		defer newArray.Release()

		// Overflow safe: Check the overflow prior so this convert is value safe
		resultArray, err := compute.CastToType(ctx, newArray, s.DType())
		if err != nil {
			return nil, fmt.Errorf("failed to cast the result to %s: %w", s.DType(), err)
		}
		defer resultArray.Release()

		return NewSeries(s.name, resultArray), nil
	default:
		// If the data type is not supported, return an error
		return nil, fmt.Errorf("sum is not supported for %s", s.DType())
	}
}
