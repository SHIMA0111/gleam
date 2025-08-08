package dataframe

import (
	"context"
	"fmt"
	"sync"

	"github.com/SHIMA0111/gleam/gleam/series"
	internalArray "github.com/SHIMA0111/gleam/internal/compute/array"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
)

// Where filters rows of the DataFrame based on the provided boolean comparison array.
// The filterArray must be a boolean Arrow array with the same length as the DataFrame rows.
// Each column is filtered concurrently and the resulting DataFrame retains the column order.
func (df *DataFrame) Where(filterArray series.ComparisonArray) (*DataFrame, error) {
	if filterArray == nil {
		return nil, fmt.Errorf("filter array must not be nil")
	}
	if filterArray.DataType().ID() != arrow.BOOL {
		return nil, fmt.Errorf("filter array must be boolean type")
	}
	if int(filterArray.Len()) != df.numRows {
		return nil, fmt.Errorf("filter array length %d does not match dataframe rows %d", filterArray.Len(), df.numRows)
	}

	ctx := context.Background()
	filterOpts := compute.DefaultFilterOptions()

	newColumns := make([]arrow.Array, df.numCols)
	var wg sync.WaitGroup
	errCh := make(chan error, df.numCols)

	for i, col := range df.columns {
		wg.Add(1)
		go func(i int, col arrow.Array) {
			defer wg.Done()
			filtered, err := internalArray.Filter(ctx, col, arrow.Array(filterArray), *filterOpts)
			if err != nil {
				errCh <- err
				return
			}
			newColumns[i] = filtered
		}(i, col)
	}

	wg.Wait()
	close(errCh)
	if err, ok := <-errCh; ok {
		for _, col := range newColumns {
			if col != nil {
				col.Release()
			}
		}
		return nil, err
	}

	names := make([]string, df.numCols)
	for i := 0; i < df.numCols; i++ {
		names[i] = df.schema.Field(i).Name
	}

	return NewDataFrame(newColumns, names)
}
