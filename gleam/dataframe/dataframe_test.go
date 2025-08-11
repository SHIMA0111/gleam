package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestNewDataFrame_AutoNamesAndSchema(t *testing.T) {
	mem := memory.NewGoAllocator()

	ib := array.NewInt64Builder(mem)
	sb := array.NewStringBuilder(mem)

	ib.AppendValues([]int64{1, 2, 3}, nil)
	sb.AppendValues([]string{"a", "b", "c"}, []bool{true, false, true}) // make one null

	intArr := ib.NewArray()
	strArr := sb.NewArray()
	ib.Release()
	sb.Release()

	df, err := NewDataFrame([]arrow.Array{intArr, strArr}, nil)
	if err != nil {
		t.Fatalf("failed to create dataframe: %v", err)
	}
	defer df.Release()

	if df.numRows != 3 || df.numCols != 2 {
		t.Fatalf("unexpected shape rows=%d cols=%d", df.numRows, df.numCols)
	}

	// auto names
	if df.schema.Field(0).Name != "column_0" || df.schema.Field(1).Name != "column_1" {
		t.Fatalf("unexpected auto names: %s, %s", df.schema.Field(0).Name, df.schema.Field(1).Name)
	}

	// nullable flag
	if df.schema.Field(0).Nullable {
		t.Fatalf("int column should not be nullable")
	}
	if !df.schema.Field(1).Nullable {
		t.Fatalf("string column should be nullable due to nulls")
	}
}

func TestNewDataFrame_NamesLengthMismatch(t *testing.T) {
	mem := memory.NewGoAllocator()

	ib := array.NewInt64Builder(mem)
	jb := array.NewInt64Builder(mem)
	ib.AppendValues([]int64{1, 2}, nil)
	jb.AppendValues([]int64{3, 4}, nil)
	arr1 := ib.NewArray()
	arr2 := jb.NewArray()
	ib.Release()
	jb.Release()

	_, err := NewDataFrame([]arrow.Array{arr1, arr2}, []string{"only_one"})
	if err == nil {
		t.Fatalf("expected error for names length mismatch, got nil")
	}
}

func TestNewDataFrame_ColumnLengthMismatch(t *testing.T) {
	mem := memory.NewGoAllocator()

	ib := array.NewInt64Builder(mem)
	jb := array.NewInt64Builder(mem)
	ib.AppendValues([]int64{1, 2, 3}, nil)
	jb.AppendValues([]int64{4, 5}, nil)
	arr1 := ib.NewArray()
	arr2 := jb.NewArray()
	ib.Release()
	jb.Release()

	_, err := NewDataFrame([]arrow.Array{arr1, arr2}, []string{"a", "b"})
	if err == nil {
		t.Fatalf("expected error for column length mismatch, got nil")
	}
}
