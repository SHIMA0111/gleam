package dataframe

import (
	"sync"
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

func TestDataFrameWhere_EdgeCases(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("NilFilter", func(t *testing.T) {
		b := array.NewInt64Builder(mem)
		b.AppendValues([]int64{1, 2, 3}, nil)
		arr := b.NewArray()
		b.Release()
		df, err := NewDataFrame([]arrow.Array{arr}, []string{"col"})
		if err != nil {
			t.Fatalf("failed to create dataframe: %v", err)
		}
		defer df.Release()

		if _, err := df.Where(nil); err == nil {
			t.Fatalf("expected error for nil filter")
		}
	})

	t.Run("WrongFilterLength", func(t *testing.T) {
		b := array.NewInt64Builder(mem)
		b.AppendValues([]int64{1, 2, 3}, nil)
		arr := b.NewArray()
		b.Release()
		df, err := NewDataFrame([]arrow.Array{arr}, []string{"col"})
		if err != nil {
			t.Fatalf("failed to create dataframe: %v", err)
		}
		defer df.Release()

		fb := array.NewBooleanBuilder(mem)
		fb.AppendValues([]bool{true, false}, nil)
		filter := fb.NewArray()
		fb.Release()
		defer filter.Release()

		if _, err := df.Where(series.ComparisonArray(filter)); err == nil {
			t.Fatalf("expected error for wrong filter length")
		}
	})

	t.Run("NonBooleanFilter", func(t *testing.T) {
		b := array.NewInt64Builder(mem)
		b.AppendValues([]int64{1, 2, 3}, nil)
		arr := b.NewArray()
		b.Release()
		df, err := NewDataFrame([]arrow.Array{arr}, []string{"col"})
		if err != nil {
			t.Fatalf("failed to create dataframe: %v", err)
		}
		defer df.Release()

		ib := array.NewInt64Builder(mem)
		ib.AppendValues([]int64{1, 0, 1}, nil)
		filter := ib.NewArray()
		ib.Release()
		defer filter.Release()

		if _, err := df.Where(series.ComparisonArray(filter)); err == nil {
			t.Fatalf("expected error for non-boolean filter")
		}
	})

	t.Run("EmptyDataFrame", func(t *testing.T) {
		b := array.NewInt64Builder(mem)
		arr := b.NewArray()
		b.Release()
		df, err := NewDataFrame([]arrow.Array{arr}, []string{"col"})
		if err != nil {
			t.Fatalf("failed to create dataframe: %v", err)
		}
		defer df.Release()

		fb := array.NewBooleanBuilder(mem)
		filter := fb.NewArray()
		fb.Release()
		defer filter.Release()

		res, err := df.Where(series.ComparisonArray(filter))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer res.Release()
		if res.numRows != 0 {
			t.Fatalf("expected 0 rows, got %d", res.numRows)
		}
	})

	t.Run("NoMatchingRows", func(t *testing.T) {
		b := array.NewInt64Builder(mem)
		b.AppendValues([]int64{1, 2, 3}, nil)
		arr := b.NewArray()
		b.Release()
		df, err := NewDataFrame([]arrow.Array{arr}, []string{"col"})
		if err != nil {
			t.Fatalf("failed to create dataframe: %v", err)
		}
		defer df.Release()

		fb := array.NewBooleanBuilder(mem)
		fb.AppendValues([]bool{false, false, false}, nil)
		filter := fb.NewArray()
		fb.Release()
		defer filter.Release()

		res, err := df.Where(series.ComparisonArray(filter))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer res.Release()
		if res.numRows != 0 {
			t.Fatalf("expected 0 rows, got %d", res.numRows)
		}
	})

	t.Run("AllRowsMatching", func(t *testing.T) {
		b := array.NewInt64Builder(mem)
		b.AppendValues([]int64{1, 2, 3}, nil)
		arr := b.NewArray()
		b.Release()
		df, err := NewDataFrame([]arrow.Array{arr}, []string{"col"})
		if err != nil {
			t.Fatalf("failed to create dataframe: %v", err)
		}
		defer df.Release()

		fb := array.NewBooleanBuilder(mem)
		fb.AppendValues([]bool{true, true, true}, nil)
		filter := fb.NewArray()
		fb.Release()
		defer filter.Release()

		res, err := df.Where(series.ComparisonArray(filter))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer res.Release()
		if res.numRows != 3 {
			t.Fatalf("expected 3 rows, got %d", res.numRows)
		}
	})
}

func TestDataFrameWhere_DifferentTypes(t *testing.T) {
	mem := memory.NewGoAllocator()

	b1 := array.NewInt64Builder(mem)
	b2 := array.NewFloat32Builder(mem)
	b3 := array.NewStringBuilder(mem)
	b4 := array.NewBooleanBuilder(mem)
	b1.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	b2.AppendValues([]float32{1.1, 2.2, 3.3, 4.4, 5.5}, nil)
	b3.AppendValues([]string{"a", "b", "c", "d", "e"}, nil)
	b4.AppendValues([]bool{true, false, true, false, true}, nil)
	arr1 := b1.NewArray()
	arr2 := b2.NewArray()
	arr3 := b3.NewArray()
	arr4 := b4.NewArray()
	b1.Release()
	b2.Release()
	b3.Release()
	b4.Release()

	df, err := NewDataFrame([]arrow.Array{arr1, arr2, arr3, arr4}, []string{"ints", "floats", "strings", "bools"})
	if err != nil {
		t.Fatalf("failed to create dataframe: %v", err)
	}
	defer df.Release()

	s := series.NewSeries("ints", df.columns[0])
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
	floatCol := res.columns[1].(*array.Float32)
	stringCol := res.columns[2].(*array.String)
	boolCol := res.columns[3].(*array.Boolean)
	expectedInts := []int64{3, 4, 5}
	expectedFloats := []float32{3.3, 4.4, 5.5}
	expectedStrings := []string{"c", "d", "e"}
	expectedBools := []bool{true, false, true}
	for i := 0; i < 3; i++ {
		if intCol.Value(i) != expectedInts[i] {
			t.Errorf("ints[%d] = %d, want %d", i, intCol.Value(i), expectedInts[i])
		}
		if floatCol.Value(i) != expectedFloats[i] {
			t.Errorf("floats[%d] = %f, want %f", i, floatCol.Value(i), expectedFloats[i])
		}
		if stringCol.Value(i) != expectedStrings[i] {
			t.Errorf("strings[%d] = %s, want %s", i, stringCol.Value(i), expectedStrings[i])
		}
		if boolCol.Value(i) != expectedBools[i] {
			t.Errorf("bools[%d] = %t, want %t", i, boolCol.Value(i), expectedBools[i])
		}
	}
}

func TestDataFrameWhere_ConcurrentAccess(t *testing.T) {
	mem := memory.NewGoAllocator()

	b := array.NewInt64Builder(mem)
	b.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	arr := b.NewArray()
	b.Release()

	df, err := NewDataFrame([]arrow.Array{arr}, []string{"col"})
	if err != nil {
		t.Fatalf("failed to create dataframe: %v", err)
	}
	defer df.Release()

	s := series.NewSeries("col", df.columns[0])
	filter, err := s.Comparison(utils.Greater, int64(2))
	if err != nil {
		t.Fatalf("comparison failed: %v", err)
	}
	defer filter.Release()
	s.Release()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := df.Where(filter)
			if err != nil {
				t.Errorf("where failed: %v", err)
				return
			}
			defer res.Release()
			if res.numRows != 3 {
				t.Errorf("expected 3 rows, got %d", res.numRows)
			}
		}()
	}
	wg.Wait()
}
