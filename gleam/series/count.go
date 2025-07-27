package series

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	internalCompute "github.com/SHIMA0111/gleam/internal/compute"
)

func (s *Series) Count() (*Series, error) {
	ctx := context.Background()
	skippedNaArray, err := internalCompute.DropNullArray(ctx, s.array)
	if err != nil {
		return nil, fmt.Errorf("failed to drop null values: %w", err)
	}
	defer skippedNaArray.Release()

	count := skippedNaArray.Len()

	mem := memory.DefaultAllocator

	b := array.NewInt64Builder(mem)
	defer b.Release()

	b.Append(int64(count))
	newArray := b.NewArray()
	defer newArray.Release()

	return NewSeries(s.name, newArray), nil
}
