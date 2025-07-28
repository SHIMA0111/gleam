package series

import (
	"context"
	"github.com/apache/arrow-go/v18/arrow/scalar"

	"github.com/apache/arrow-go/v18/arrow/memory"

	internalCompute "github.com/SHIMA0111/gleam/internal/compute"
)

func (s *Series) Count() (*Series, error) {
	ctx := context.Background()
	mem := memory.DefaultAllocator

	count, err := s.count(ctx)
	if err != nil {
		return nil, err
	}

	scl := scalar.NewInt64Scalar(count)
	newArray, err := scalar.MakeArrayFromScalar(scl, 1, mem)
	if err != nil {
		return nil, err
	}
	defer newArray.Release()

	return NewSeries(s.name, newArray), nil
}

func (s *Series) count(ctx context.Context) (int64, error) {
	droppedArray, err := internalCompute.DropNullArray(ctx, s.array)
	if err != nil {
		return 0, err
	}
	defer droppedArray.Release()

	return int64(droppedArray.Len()), nil
}
