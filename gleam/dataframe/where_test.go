package dataframe

import (
	"fmt"
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

func TestDataFrameWhere_Errors(t *testing.T) {
	mem := memory.NewGoAllocator()

	b1 := array.NewInt64Builder(mem)
	b2 := array.NewInt64Builder(mem)
	b1.AppendValues([]int64{1, 2, 3}, nil)
	b2.AppendValues([]int64{10, 20, 30}, nil)
	arr1 := b1.NewArray()
	arr2 := b2.NewArray()
	b1.Release()
	b2.Release()

	df, err := NewDataFrame([]arrow.Array{arr1, arr2}, []string{"a", "b"})
	if err != nil {
		t.Fatalf("failed to create dataframe: %v", err)
	}
	defer df.Release()

	// nil filter
	if _, err := df.Where(nil); err == nil {
		t.Fatalf("expected error for nil filter")
	}

	// wrong type filter (int array as filter)
	ib := array.NewInt64Builder(mem)
	ib.AppendValues([]int64{1, 0, 1}, nil)
	wrong := ib.NewArray()
	ib.Release()

	sWrong := series.ComparisonArray(wrong)
	if _, err := df.Where(sWrong); err == nil {
		t.Fatalf("expected error for non-boolean filter array")
	}

	// length mismatch
	bb := array.NewBooleanBuilder(mem)
	bb.AppendValues([]bool{true, false}, nil) // shorter
	short := bb.NewArray()
	bb.Release()

	if _, err := df.Where(series.ComparisonArray(short)); err == nil {
		t.Fatalf("expected error for length mismatch")
	}
}

func TestDataFrameWhere_WithNullsAndNamesPreserved(t *testing.T) {
	mem := memory.NewGoAllocator()

	ib := array.NewInt64Builder(mem)
	fb := array.NewFloat64Builder(mem)
	sb := array.NewStringBuilder(mem)

	ib.AppendValues([]int64{1, 2, 3, 4}, nil)
	fb.AppendValues([]float64{1.0, 2.0, 3.0, 4.0}, []bool{true, true, false, true}) // one null
	sb.AppendValues([]string{"x", "y", "z", "w"}, []bool{true, false, true, true})

	a1 := ib.NewArray()
	a2 := fb.NewArray()
	a3 := sb.NewArray()
	ib.Release()
	fb.Release()
	sb.Release()

	df, err := NewDataFrame([]arrow.Array{a1, a2, a3}, []string{"id", "val", "name"})
	if err != nil {
		t.Fatalf("failed to create dataframe: %v", err)
	}
	defer df.Release()

	s := series.NewSeries("id", df.columns[0])
	defer s.Release()

	filter, err := s.Comparison(utils.GreaterEqual, int64(2))
	if err != nil {
		t.Fatalf("comparison failed: %v", err)
	}
	defer filter.Release()

	res, err := df.Where(filter)
	if err != nil {
		t.Fatalf("where failed: %v", err)
	}
	defer res.Release()

	if res.numRows != 3 || res.numCols != 3 {
		t.Fatalf("unexpected shape rows=%d cols=%d", res.numRows, res.numCols)
	}

	// names should be preserved
	if res.schema.Field(0).Name != "id" || res.schema.Field(1).Name != "val" || res.schema.Field(2).Name != "name" {
		t.Fatalf("column names are not preserved")
	}

	// check values including the row that had null in "val" column (row index 2)
	outIDs := res.columns[0].(*array.Int64)
	expected := []int64{2, 3, 4}
	for i := 0; i < 3; i++ {
		if outIDs.Value(i) != expected[i] {
			t.Fatalf("unexpected id[%d]=%d want=%d", i, outIDs.Value(i), expected[i])
		}
	}
}

func TestDataFrameWhere_EdgeCases(t *testing.T) {
	mem := memory.NewGoAllocator()

	// 1) Empty DataFrame
	ib := array.NewInt64Builder(mem)
	fb := array.NewFloat64Builder(mem)
	bb := array.NewBooleanBuilder(mem)

	// no values appended -> length 0 arrays
	aInt := ib.NewArray()
	aFlt := fb.NewArray()
	aBool := bb.NewArray()
	ib.Release()
	fb.Release()
	bb.Release()

	dfEmpty, err := NewDataFrame([]arrow.Array{aInt, aFlt, aBool}, []string{"i", "f", "b"})
	if err != nil {
		t.Fatalf("failed to create empty df: %v", err)
	}
	defer dfEmpty.Release()

	// empty filter (len 0)
	bbf := array.NewBooleanBuilder(mem)
	emptyFilter := bbf.NewArray()
	bbf.Release()
	resEmpty, err := dfEmpty.Where(series.ComparisonArray(emptyFilter))
	if err != nil {
		t.Fatalf("where on empty df failed: %v", err)
	}
	defer resEmpty.Release()
	if resEmpty.numRows != 0 || resEmpty.numCols != 3 {
		t.Fatalf("unexpected empty result shape rows=%d cols=%d", resEmpty.numRows, resEmpty.numCols)
	}

	// 2) No matching rows (all false)
	ib2 := array.NewInt64Builder(mem)
	fb2 := array.NewFloat64Builder(mem)
	sb2 := array.NewStringBuilder(mem)
	ib2.AppendValues([]int64{1, 2, 3, 4}, nil)
	fb2.AppendValues([]float64{1, 2, 3, 4}, nil)
	sb2.AppendValues([]string{"a", "b", "c", "d"}, nil)
	ai2 := ib2.NewArray()
	af2 := fb2.NewArray()
	as2 := sb2.NewArray()
	ib2.Release()
	fb2.Release()
	sb2.Release()
	df2, err := NewDataFrame([]arrow.Array{ai2, af2, as2}, []string{"i", "f", "s"})
	if err != nil {
		t.Fatalf("failed to create df2: %v", err)
	}
	defer df2.Release()

	bfAllFalse := array.NewBooleanBuilder(mem)
	bfAllFalse.AppendValues([]bool{false, false, false, false}, nil)
	allFalse := bfAllFalse.NewArray()
	bfAllFalse.Release()

	resNone, err := df2.Where(series.ComparisonArray(allFalse))
	if err != nil {
		t.Fatalf("where all-false failed: %v", err)
	}
	defer resNone.Release()
	if resNone.numRows != 0 {
		t.Fatalf("expected 0 rows, got %d", resNone.numRows)
	}

	// 3) All rows matching (all true)
	bfAllTrue := array.NewBooleanBuilder(mem)
	bfAllTrue.AppendValues([]bool{true, true, true, true}, nil)
	allTrue := bfAllTrue.NewArray()
	bfAllTrue.Release()

	resAll, err := df2.Where(series.ComparisonArray(allTrue))
	if err != nil {
		t.Fatalf("where all-true failed: %v", err)
	}
	defer resAll.Release()
	if resAll.numRows != 4 || resAll.numCols != 3 {
		t.Fatalf("unexpected shape rows=%d cols=%d", resAll.numRows, resAll.numCols)
	}
	// ensure values preserved for a different type (string)
	outS := resAll.columns[2].(*array.String)
	expectedS := []string{"a", "b", "c", "d"}
	for i := range expectedS {
		if outS.Value(i) != expectedS[i] {
			t.Fatalf("unexpected string[%d]=%s want=%s", i, outS.Value(i), expectedS[i])
		}
	}
}

func TestDataFrameWhere_ConcurrentAccess(t *testing.T) {
	mem := memory.NewGoAllocator()

	ib := array.NewInt64Builder(mem)
	fb := array.NewFloat64Builder(mem)
	ib.AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8}, nil)
	fb.AppendValues([]float64{1, 2, 3, 4, 5, 6, 7, 8}, nil)
	a1 := ib.NewArray()
	a2 := fb.NewArray()
	ib.Release()
	fb.Release()

	df, err := NewDataFrame([]arrow.Array{a1, a2}, []string{"i", "f"})
	if err != nil {
		t.Fatalf("failed to create df: %v", err)
	}
	defer df.Release()

	s := series.NewSeries("i", df.columns[0])
	defer s.Release()
	filt, err := s.Comparison(utils.Greater, int64(3))
	if err != nil {
		t.Fatalf("comparison failed: %v", err)
	}
	defer filt.Release()

	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers)
	errs := make(chan error, workers)

	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			res, err := df.Where(filt)
			if err != nil {
				errs <- err
				return
			}
			// quick sanity check
			if res.numRows != 5 {
				errs <- fmt.Errorf("unexpected rows=%d", res.numRows)
			}
			res.Release()
		}()
	}

	wg.Wait()
	close(errs)
	if err, ok := <-errs; ok {
		t.Fatalf("concurrent where failed: %v", err)
	}
}
