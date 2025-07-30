package dataframe

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// DataFrame works as a container of the Apache Arrow and other metadata
// Unfortunately, current arrow-go's arrow.Record slower the Filter and other operations.
// DataFrame avoids the bottleneck touch the raw arrow and schema.
// Someday, the Record performance increases extremely; we may decide to pander to the manner.
type DataFrame struct {
	schema  arrow.Schema
	columns []arrow.Array
	mem     memory.Allocator
	numRows int
	numCols int
}

func NewDataFrame(columns []arrow.Array, names []string) (*DataFrame, error) {
	if len(columns) != len(names) && len(names) != 0 {
		return nil, fmt.Errorf("names should have same length as the columns or 0")
	}

	colNum := len(columns)
	rowNum := columns[0].Len()
	if len(names) == 0 {
		for i := 0; i < colNum; i++ {
			names = append(names, fmt.Sprintf("column_%d", i))
		}
	}

	fields := make([]arrow.Field, len(columns))
	for i := 0; i < colNum; i++ {
		arr := columns[i]

		if arr.Len() != rowNum {
			return nil, fmt.Errorf("columns should have same length, expect %d but got %s has %d rows", arr.Len(), names[i], rowNum)
		}

		nullable := arr.NullN() > 0

		fil := arrow.Field{
			Name:     names[i],
			Type:     arr.DataType(),
			Nullable: nullable,
		}
		fields[i] = fil
	}
	schema := arrow.NewSchema(fields, nil)

	return &DataFrame{
		schema:  *schema,
		columns: columns,
		mem:     memory.DefaultAllocator,
		numRows: columns[0].Len(),
		numCols: colNum,
	}, nil
}

func (df *DataFrame) Release() {
	for _, column := range df.columns {
		column.Release()
	}
}
