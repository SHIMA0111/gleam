package series

import (
	"context"
	"fmt"
	internalCompute "github.com/SHIMA0111/gleam/internal/compute"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/math"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
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

	sumVal, err := s.sum(ctx)
	if err != nil {
		return nil, err
	}

	scl := scalar.NewFloat64Scalar(sumVal)
	newArray, err := scalar.MakeArrayFromScalar(scl, 1, mem)
	if err != nil {
		return nil, err
	}
	defer newArray.Release()

	return NewSeries(s.name, newArray), nil
}

func (s *Series) sum(ctx context.Context) (float64, error) {
	droppedArray, err := internalCompute.DropNullArray(ctx, s.array)
	if err != nil {
		return 0, err
	}
	defer droppedArray.Release()

	switch s.DType().ID() {
	case arrow.INT8:
		if s.Len() < SumThreshold {
			return float64(sumInt8Array(droppedArray.(*array.Int8))), nil
		} else {
			if v, err := castSumInt(ctx, droppedArray); err == nil {
				return float64(v), nil
			}
			return 0, err
		}
	case arrow.INT16:
		if s.Len() < SumThreshold {
			return float64(sumInt16Array(droppedArray.(*array.Int16))), nil
		} else {
			if v, err := castSumInt(ctx, droppedArray); err == nil {
				return float64(v), nil
			}
			return 0, err
		}
	case arrow.INT32:
		if s.Len() < SumThreshold {
			return float64(sumInt32Array(droppedArray.(*array.Int32))), nil
		} else {
			if v, err := castSumInt(ctx, droppedArray); err == nil {
				return float64(v), nil
			}
			return 0, err
		}
	case arrow.INT64:
		i64Array, ok := droppedArray.(*array.Int64)
		if !ok {
			return 0, fmt.Errorf("failed to cast the array to Int64 from %s: %w", s.DType(), err)
		}

		return float64(math.Int64.Sum(i64Array)), nil
	case arrow.UINT8:
		if s.Len() < SumThreshold {
			return float64(sumUInt8Array(droppedArray.(*array.Uint8))), nil
		} else {
			if v, err := castSumUInt(ctx, droppedArray); err == nil {
				return float64(v), nil
			}
			return 0, err
		}
	case arrow.UINT16:
		if s.Len() < SumThreshold {
			return float64(sumUInt16Array(droppedArray.(*array.Uint16))), nil
		} else {
			if v, err := castSumUInt(ctx, droppedArray); err == nil {
				return float64(v), nil
			}
			return 0, err
		}
	case arrow.UINT32:
		if s.Len() < SumThreshold {
			return float64(sumUInt32Array(droppedArray.(*array.Uint32))), nil
		} else {
			if v, err := castSumUInt(ctx, droppedArray); err == nil {
				return float64(v), nil
			}
			return 0, err
		}
	case arrow.UINT64:
		u64Array, ok := droppedArray.(*array.Uint64)
		if !ok {
			return 0, fmt.Errorf("failed to cast the array to Uint64 from %s: %w", s.DType(), err)
		}

		return float64(math.Uint64.Sum(u64Array)), nil
	case arrow.FLOAT32:
		if s.Len() < SumThreshold {
			return sumFloat32Array(droppedArray.(*array.Float32)), nil
		} else {
			return castSumFloat(ctx, droppedArray)
		}
	case arrow.FLOAT64:
		f64Array, ok := droppedArray.(*array.Float64)
		if !ok {
			return 0, fmt.Errorf("failed to cast the array to Float64 from %s: %w", s.DType(), err)
		}

		return math.Float64.Sum(f64Array), nil
	default:
		return 0, fmt.Errorf("sum is not supported for %s", s.DType())
	}
}

func castSumInt(ctx context.Context, arr arrow.Array) (int64, error) {
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
	u64Array, ok := castedArray.(*array.Uint64)
	if !ok {
		return 0, fmt.Errorf("failed to cast the array to Uint64 from %s: %w", arr.DataType(), err)
	}

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

	return math.Float64.Sum(f64Array), nil
}
