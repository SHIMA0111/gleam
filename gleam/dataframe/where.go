package dataframe

import (
	"context"
	"github.com/SHIMA0111/gleam/gleam/series"
)

func (df *DataFrame) Where(filterArray series.ComparisonArray) (*DataFrame, error) {
	ctx := context.Background()

}
