package dataframe

import (
	"testing"

	"github.com/SHIMA0111/gleam/gleam/series"
	"github.com/SHIMA0111/gleam/gleam/utils"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestDataFrameWhere(t *testing.T) {
	mem := memory.NewGoAllocator()

	b1 := array.NewInt64Builder(mem)
	b2 := array.NewFloat64Builder(mem)
	b1.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	b2.AppendValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5}, nil)
	arr1 := b1.NewArray()
	arr2 := b2.NewArray()
	b1.Release()
	b2.Release()

	df, err := NewDataFrame([]arrow.Array{arr1, arr2}, []string{"col1", "col2"})
	if err != nil {
		t.Fatalf("failed to create dataframe: %v", err)
	}
	defer df.Release()

	s := series.NewSeries("col1", df.columns[0])
	defer s.Release()

	filter, err := s.Comparison(utils.Greater, int64(2))
	if err != nil {
		t.Fatalf("comparison failed: %v", err)
	}
	defer filter.Release()

	res, err := df.Where(filter)
	if err != nil {
		t.Fatalf("where failed: %v", err)
	}
	defer res.Release()

	if res.numRows != 3 {
		t.Fatalf("expected 3 rows, got %d", res.numRows)
	}

	intCol := res.columns[0].(*array.Int64)
	floatCol := res.columns[1].(*array.Float64)
	expectedInts := []int64{3, 4, 5}
	expectedFloats := []float64{3.3, 4.4, 5.5}
	for i := 0; i < 3; i++ {
		if intCol.Value(i) != expectedInts[i] {
			t.Errorf("col1[%d] = %d, want %d", i, intCol.Value(i), expectedInts[i])
		}
		if floatCol.Value(i) != expectedFloats[i] {
			t.Errorf("col2[%d] = %f, want %f", i, floatCol.Value(i), expectedFloats[i])
		}
	}
}
