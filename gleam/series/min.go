package series

import (
	"context"
	"github.com/SHIMA0111/gleam/internal/compute/array"
)

func (s *Series) Min() (*Series, error) {
	ctx := context.Background()

	newArray, err := array.MinArray(ctx, s.array, s.mem)
	if err != nil {
		return nil, err
	}
	defer newArray.Release()

	return NewSeries(s.name, newArray), nil
}
