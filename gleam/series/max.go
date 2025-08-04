package series

import (
	"context"
	"github.com/SHIMA0111/gleam/internal/compute/array"
)

func (s *Series) Max() (*Series, error) {
	ctx := context.Background()

	newArray, err := array.MaxArray(ctx, s.array, s.mem)
	if err != nil {
		return nil, err
	}
	defer newArray.Release()

	return NewSeries(s.name, newArray), nil
}
