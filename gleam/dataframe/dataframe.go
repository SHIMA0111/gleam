package dataframe

import "github.com/apache/arrow-go/v18/arrow"

type DataFrame struct {
	schema  *arrow.Schema
	columns []arrow.Array
	nRows   int64
}
