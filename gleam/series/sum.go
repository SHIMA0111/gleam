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
	"runtime"
	"sync"
)

const SumThreshold = 150_000

// Sum calculates the sum of all elements in the Series,
// returning the result as a new Series with 64-bit float Series. Or, returns an error if unsupported.
// In arrow-go, there is a math.(Int64, UInt64, Float64).Sum, which is the optimized function with assembly.
// We use this method with cast the array data type.
// However, in a small sum execution, the Go loop is faster than the arrow sum function
// what from the overhead cast and so. (In small, the 64-bit numeric is still fastest)
// Sum uses a threshold to judge the sum operation method, go loop and cast and arrow sum.
func (s *Series) Sum() (*Series, error) {
	ctx := context.Background()
	mem := memory.DefaultAllocator

	sumVal, err := s.concurrentSum(ctx)
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

	return sum(ctx, droppedArray)
}

func (s *Series) concurrentSum(ctx context.Context) (float64, error) {
	droppedArray, err := internalCompute.DropNullArray(ctx, s.array)
	if err != nil {
		return 0, err
	}
	defer droppedArray.Release()

	// Go non-float number division works as a truncation float point so add 1
	chunkSize := s.Len()/runtime.NumCPU() + 1

	floatChan := make(chan float64, runtime.NumCPU())
	var wg sync.WaitGroup

	for i := 0; i < s.Len(); i += chunkSize {
		wg.Add(1)
		end := i + chunkSize
		if end > s.Len() {
			end = s.Len()
		}

		arrowView := array.NewSlice(s.array, int64(i), int64(end))
		go func() {
			sumVal, err := sum(ctx, arrowView)
			if err != nil {
				panic(err)
			}
			floatChan <- sumVal
			wg.Done()
			arrowView.Release()
		}()
	}

	wg.Wait()
	close(floatChan)

	total := 0.
	for res := range floatChan {
		total += res
	}

	return total, nil
}

func sum(ctx context.Context, arr arrow.Array) (float64, error) {
	switch arr.DataType().ID() {
	case arrow.INT8:
		if arr.Len() < SumThreshold {
			return checkOverflowAndConvertToFloat64(sumInt8Array(arr.(*array.Int8)))
		} else {
			return castSumInt(ctx, arr)
		}
	case arrow.INT16:
		if arr.Len() < SumThreshold {
			return checkOverflowAndConvertToFloat64(sumInt16Array(arr.(*array.Int16)))
		} else {
			return castSumInt(ctx, arr)
		}
	case arrow.INT32:
		if arr.Len() < SumThreshold {
			return checkOverflowAndConvertToFloat64(sumInt32Array(arr.(*array.Int32)))
		} else {
			return castSumInt(ctx, arr)
		}
	case arrow.INT64:
		i64Array, ok := arr.(*array.Int64)
		if !ok {
			return 0, fmt.Errorf("failed to cast the array to Int64 from %s", arr.DataType())
		}

		return checkOverflowAndConvertToFloat64[int64](math.Int64.Sum(i64Array))
	case arrow.UINT8:
		if arr.Len() < SumThreshold {
			return checkOverflowAndConvertToFloat64(sumUInt8Array(arr.(*array.Uint8)))
		} else {
			return castSumUInt(ctx, arr)
		}
	case arrow.UINT16:
		if arr.Len() < SumThreshold {
			return checkOverflowAndConvertToFloat64(sumUInt16Array(arr.(*array.Uint16)))
		} else {
			return castSumUInt(ctx, arr)
		}
	case arrow.UINT32:
		if arr.Len() < SumThreshold {
			return checkOverflowAndConvertToFloat64(sumUInt32Array(arr.(*array.Uint32)))
		} else {
			return castSumUInt(ctx, arr)
		}
	case arrow.UINT64:
		u64Array, ok := arr.(*array.Uint64)
		if !ok {
			return 0, fmt.Errorf("failed to cast the array to Uint64 from %s", arr.DataType())
		}

		return checkOverflowAndConvertToFloat64(math.Uint64.Sum(u64Array))
	case arrow.FLOAT32:
		if arr.Len() < SumThreshold {
			return checkOverflowAndConvertToFloat64(sumFloat32Array(arr.(*array.Float32)))
		} else {
			return castSumFloat(ctx, arr)
		}
	case arrow.FLOAT64:
		f64Array, ok := arr.(*array.Float64)
		if !ok {
			return 0, fmt.Errorf("failed to cast the array to Float64 from %s", arr.DataType())
		}

		return checkOverflowAndConvertToFloat64(math.Float64.Sum(f64Array))
	default:
		return 0, fmt.Errorf("sum is not supported for %s", arr.DataType())
	}
}

func castSumInt(ctx context.Context, arr arrow.Array) (float64, error) {
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
	sumIntValue := math.Int64.Sum(i64Array)

	// To abstraction, all sum functions return float64, but float64's significant digits are 53 digits
	// 53 digits are enough in the real world because the value is a cornucopia
	if 1<<53 <= sumIntValue {
		return 0, fmt.Errorf("overflow: %d", sumIntValue)
	}
	return checkOverflowAndConvertToFloat64[int64](sumIntValue)
}

func castSumUInt(ctx context.Context, arr arrow.Array) (float64, error) {
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
	sumUIntValue := math.Uint64.Sum(u64Array)

	return checkOverflowAndConvertToFloat64[uint64](sumUIntValue)
}

func castSumFloat(ctx context.Context, arr arrow.Array) (float64, error) {
	castedArray, err := compute.CastToType(ctx, arr, arrow.PrimitiveTypes.Float64)
	if err != nil {
		return 0, err
	}
	defer castedArray.Release()

	f64Array := castedArray.(*array.Float64)
	sumFloatValue := math.Float64.Sum(f64Array)

	return checkOverflowAndConvertToFloat64[float64](sumFloatValue)
}
