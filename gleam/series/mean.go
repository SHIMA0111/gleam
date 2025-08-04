package series

import (
	"context"
	"fmt"
	"github.com/SHIMA0111/gleam/internal/compute/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

func (s *Series) Mean() (*Series, error) {
	mean, err := s.mean(context.Background())
	if err != nil {
		return nil, err
	}
	mem := memory.DefaultAllocator

	scl := scalar.NewFloat64Scalar(mean)
	arr, err := scalar.MakeArrayFromScalar(scl, 1, mem)
	if err != nil {
		return nil, err
	}
	defer arr.Release()

	return NewSeries(s.name, arr), nil
}

func (s *Series) mean(ctx context.Context) (float64, error) {
	if s.Len() == 0 {
		return 0, fmt.Errorf("cannot find mean value of empty Series")
	}

	count, err := s.count()
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, nil
	}

	sumVal, err := array.Sum(ctx, s.array)
	if err != nil {
		return 0, err
	}
	return sumVal / float64(count), nil
}
