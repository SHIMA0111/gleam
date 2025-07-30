package dataframe

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"golang.org/x/exp/maps"
	"slices"
)

func (df *DataFrame) Select(cols []string) (*DataFrame, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("select columns must be non-empty")
	}

	idxes := make([]int32, len(cols))
	for i, name := range cols {
		if !slices.Contains(maps.Keys(df.colMap), name) {
			return nil, fmt.Errorf("column %s not found", name)
		}
		idxes[i] = df.colMap[name]
	}

	fields := make([]arrow.Field, len(idxes))
	columns := make([]arrow.Array, len(idxes))
	for i, idx := range idxes {
		fields[i] = df.record.Schema().Field(int(idx))
		columns[i] = df.record.Column(int(idx))
	}

	schema := arrow.NewSchema(fields, nil)
	newRecord := array.NewRecord(schema, columns, int64(columns[0].Len()))

	return NewDataFrame(df.mem, newRecord), nil
}
