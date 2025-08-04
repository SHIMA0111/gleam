package series

import (
	"context"
	internalCompute "github.com/SHIMA0111/gleam/internal/compute/array"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"runtime"
	"sync"
)

const ConcurrentSumThreshold = 100_000

// Sum calculates the sum of all elements in the Series,
// returning the result as a new Series with 64-bit float Series. Or, returns an error if unsupported.
// In arrow-go, there is a math.(Int64, UInt64, Float64).Sum, which is the optimized function with assembly.
// We use this method with cast the array data type.
// However, in a small sum execution, the Go loop is faster than the arrow sum function
// what from the overhead cast and so. (In small, the 64-bit numeric is still fastest)
// Sum uses a threshold to judge the sum operation method, go loop and cast and arrow sum.
func (s *Series) Sum() (*Series, error) {
	ctx := context.Background()

	var sumArr arrow.Array
	var err error

	if s.Len() < ConcurrentSumThreshold {
		sumArr, err = s.sum(ctx)
	} else {
		sumArr, err = s.concurrentSum(ctx)
	}

	if err != nil {
		return nil, err
	}

	return NewSeries(s.name, sumArr), nil
}

func (s *Series) sum(ctx context.Context) (arrow.Array, error) {
	droppedArray, err := internalCompute.DropNullArray(ctx, s.array)
	if err != nil {
		return nil, err
	}
	defer droppedArray.Release()

	return internalCompute.SumArray(ctx, droppedArray, s.mem)
}

func (s *Series) concurrentSum(ctx context.Context) (arrow.Array, error) {
	droppedArray, err := internalCompute.DropNullArray(ctx, s.array)
	if err != nil {
		return nil, err
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
			sumVal, err := internalCompute.Sum(ctx, arrowView)
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

	scl := scalar.NewFloat64Scalar(total)
	newArray, err := scalar.MakeArrayFromScalar(scl, 1, s.mem)

	return newArray, nil
}
