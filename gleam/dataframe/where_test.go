package dataframe

import (
	"strings"
	"testing"

	"github.com/SHIMA0111/gleam/gleam/series"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestDataFrame_Where(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	t.Run("filter with equal condition on int64", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
			"col2": []float64{1.1, 2.2, 3.3, 4.4, 5.5},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Create a comparison array using the Series.Comparison method
		s, err := df.Get("col1")
		if err != nil {
			t.Fatalf("failed to get col1: %v", err)
		}
		defer s.Release()

		filterArray, err := s.Comparison(series.Equal, int64(3))
		if err != nil {
			t.Fatalf("failed to create comparison array: %v", err)
		}

		// Apply the filter
		result, err := df.Where(filterArray)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.record.NumRows() != 1 {
			t.Errorf("expected 1 row, got %d", result.record.NumRows())
		}

		// Check if the filtered row has the expected values
		resultCol1, err := result.Get("col1")
		if err != nil {
			t.Fatalf("failed to get col1: %v", err)
		}
		defer resultCol1.Release()
		resultCol2, err := result.Get("col2")
		if err != nil {
			t.Fatalf("failed to get col2: %v", err)
		}
		defer resultCol2.Release()

		// Check the length of the result columns
		if resultCol1.Len() != 1 {
			t.Errorf("expected length 1 for col1, got %d", resultCol1.Len())
		}
		if resultCol2.Len() != 1 {
			t.Errorf("expected length 1 for col2, got %d", resultCol2.Len())
		}

		// Check the string representation contains expected values
		col1Str := resultCol1.String()
		if !strings.Contains(col1Str, "3") {
			t.Errorf("expected col1 to contain value 3, got %s", col1Str)
		}

		col2Str := resultCol2.String()
		if !strings.Contains(col2Str, "3.3") {
			t.Errorf("expected col2 to contain value 3.3, got %s", col2Str)
		}
	})

	t.Run("filter with greater condition on float64", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
			"col2": []float64{1.1, 2.2, 3.3, 4.4, 5.5},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Create a comparison array using the Series.Comparison method
		s, err := df.Get("col2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer s.Release()

		filterArray, err := s.Comparison(series.Greater, 3.0)
		if err != nil {
			t.Fatalf("failed to create comparison array: %v", err)
		}

		// Apply the filter
		result, err := df.Where(filterArray)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.record.NumRows() != 3 {
			t.Errorf("expected 3 rows, got %d", result.record.NumRows())
		}

		// Check if the filtered rows have the expected values
		resultCol2, err := result.Get("col2")
		if err != nil {
			t.Fatalf("failed to get col2: %v", err)
		}
		defer resultCol2.Release()

		// Check the length of the result column
		if resultCol2.Len() != 3 {
			t.Errorf("expected length 3 for col2, got %d", resultCol2.Len())
		}

		// Check the string representation contains expected values
		col2Str := resultCol2.String()
		for _, expected := range []string{"3.3", "4.4", "5.5"} {
			if !strings.Contains(col2Str, expected) {
				t.Errorf("expected col2 to contain value %s, got %s", expected, col2Str)
			}
		}
	})

	t.Run("filter with less_equal condition on string", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
			"col2": []string{"a", "b", "c", "d", "e"},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Create a comparison array using the Series.Comparison method
		s, err := df.Get("col2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer s.Release()

		filterArray, err := s.Comparison(series.LessEqual, "c")
		if err != nil {
			t.Fatalf("failed to create comparison array: %v", err)
		}

		// Apply the filter
		result, err := df.Where(filterArray)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.record.NumRows() != 3 {
			t.Errorf("expected 3 rows, got %d", result.record.NumRows())
		}

		// Check if the filtered rows have the expected values
		resultCol2, err := result.Get("col2")
		if err != nil {
			t.Fatalf("failed to get col2: %v", err)
		}
		defer resultCol2.Release()

		// Check the length of the result column
		if resultCol2.Len() != 3 {
			t.Errorf("expected length 3 for col2, got %d", resultCol2.Len())
		}

		// Check the string representation contains expected values
		col2Str := resultCol2.String()
		for _, expected := range []string{"a", "b", "c"} {
			if !strings.Contains(col2Str, expected) {
				t.Errorf("expected col2 to contain value %s, got %s", expected, col2Str)
			}
		}
	})

	t.Run("filter with not_equal condition on boolean", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []bool{true, false, true, false, true},
			"col2": []int64{1, 2, 3, 4, 5},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Create a comparison array using the Series.Comparison method
		s, err := df.Get("col1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer s.Release()

		filterArray, err := s.Comparison(series.NotEqual, true)
		if err != nil {
			t.Fatalf("failed to create comparison array: %v", err)
		}

		// Apply the filter
		result, err := df.Where(filterArray)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.record.NumRows() != 2 {
			t.Errorf("expected 2 rows, got %d", result.record.NumRows())
		}

		// Check if the filtered rows have the expected values
		resultCol1, err := result.Get("col1")
		if err != nil {
			t.Fatalf("failed to get col1: %v", err)
		}
		defer resultCol1.Release()
		resultCol2, err := result.Get("col2")
		if err != nil {
			t.Fatalf("failed to get col2: %v", err)
		}
		defer resultCol2.Release()

		// Check the length of the result columns
		if resultCol1.Len() != 2 {
			t.Errorf("expected length 2 for col1, got %d", resultCol1.Len())
		}
		if resultCol2.Len() != 2 {
			t.Errorf("expected length 2 for col2, got %d", resultCol2.Len())
		}

		// Check the string representation contains expected values
		col1Str := resultCol1.String()
		if !strings.Contains(col1Str, "false") {
			t.Errorf("expected col1 to contain value false, got %s", col1Str)
		}

		col2Str := resultCol2.String()
		for _, expected := range []string{"2", "4"} {
			if !strings.Contains(col2Str, expected) {
				t.Errorf("expected col2 to contain value %s, got %s", expected, col2Str)
			}
		}
	})

	t.Run("filter with no matching rows", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Create a comparison array using the Series.Comparison method
		s, err := df.Get("col1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer s.Release()

		filterArray, err := s.Comparison(series.Equal, int64(10)) // No matching value
		if err != nil {
			t.Fatalf("failed to create comparison array: %v", err)
		}

		// Apply the filter
		result, err := df.Where(filterArray)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.record.NumRows() != 0 {
			t.Errorf("expected 0 rows, got %d", result.record.NumRows())
		}
	})

	t.Run("filter with all matching rows", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Create a comparison array using the Series.Comparison method
		s, err := df.Get("col1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer s.Release()

		filterArray, err := s.Comparison(series.GreaterEqual, int64(1)) // All values match
		if err != nil {
			t.Fatalf("failed to create comparison array: %v", err)
		}

		// Apply the filter
		result, err := df.Where(filterArray)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.record.NumRows() != 5 {
			t.Errorf("expected 5 rows, got %d", result.record.NumRows())
		}

		// Check if all rows are present in the result
		resultCol1, err := result.Get("col1")
		if err != nil {
			t.Fatalf("failed to get col1: %v", err)
		}
		defer resultCol1.Release()

		// Check the length of the result column
		if resultCol1.Len() != 5 {
			t.Errorf("expected length 5 for col1, got %d", resultCol1.Len())
		}

		// Check the string representation contains expected values
		col1Str := resultCol1.String()
		for _, expected := range []string{"1", "2", "3", "4", "5"} {
			if !strings.Contains(col1Str, expected) {
				t.Errorf("expected col1 to contain value %s, got %s", expected, col1Str)
			}
		}
	})
}

func TestDataFrame_WhereBy(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	t.Run("filter with equal condition on int64", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
			"col2": []float64{1.1, 2.2, 3.3, 4.4, 5.5},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Apply the filter using WhereBy
		result, err := df.WhereBy("col1", series.Equal, int64(3))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.record.NumRows() != 1 {
			t.Errorf("expected 1 row, got %d", result.record.NumRows())
		}

		// Check if the filtered row has the expected values
		resultCol1, err := result.Get("col1")
		if err != nil {
			t.Fatalf("failed to get col1: %v", err)
		}
		defer resultCol1.Release()

		resultCol2, err := result.Get("col2")
		if err != nil {
			t.Fatalf("failed to get col2: %v", err)
		}
		defer resultCol2.Release()

		// Check the length of the result columns
		if resultCol1.Len() != 1 {
			t.Errorf("expected length 1 for col1, got %d", resultCol1.Len())
		}
		if resultCol2.Len() != 1 {
			t.Errorf("expected length 1 for col2, got %d", resultCol2.Len())
		}

		// Check the string representation contains expected values
		col1Str := resultCol1.String()
		if !strings.Contains(col1Str, "3") {
			t.Errorf("expected col1 to contain value 3, got %s", col1Str)
		}

		col2Str := resultCol2.String()
		if !strings.Contains(col2Str, "3.3") {
			t.Errorf("expected col2 to contain value 3.3, got %s", col2Str)
		}
	})

	t.Run("filter with greater condition on float64", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
			"col2": []float64{1.1, 2.2, 3.3, 4.4, 5.5},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Apply the filter using WhereBy
		result, err := df.WhereBy("col2", series.Greater, 3.0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.record.NumRows() != 3 {
			t.Errorf("expected 3 rows, got %d", result.record.NumRows())
		}

		// Check if the filtered rows have the expected values
		resultCol2, err := result.Get("col2")
		if err != nil {
			t.Fatalf("failed to get col2: %v", err)
		}
		defer resultCol2.Release()

		// Check the length of the result column
		if resultCol2.Len() != 3 {
			t.Errorf("expected length 3 for col2, got %d", resultCol2.Len())
		}

		// Check the string representation contains expected values
		col2Str := resultCol2.String()
		for _, expected := range []string{"3.3", "4.4", "5.5"} {
			if !strings.Contains(col2Str, expected) {
				t.Errorf("expected col2 to contain value %s, got %s", expected, col2Str)
			}
		}
	})

	t.Run("filter with less_equal condition on string", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
			"col2": []string{"a", "b", "c", "d", "e"},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Apply the filter using WhereBy
		result, err := df.WhereBy("col2", series.LessEqual, "c")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.record.NumRows() != 3 {
			t.Errorf("expected 3 rows, got %d", result.record.NumRows())
		}

		// Check if the filtered rows have the expected values
		resultCol2, err := result.Get("col2")
		if err != nil {
			t.Fatalf("failed to get col2: %v", err)
		}
		defer resultCol2.Release()

		// Check the length of the result column
		if resultCol2.Len() != 3 {
			t.Errorf("expected length 3 for col2, got %d", resultCol2.Len())
		}

		// Check the string representation contains expected values
		col2Str := resultCol2.String()
		for _, expected := range []string{"a", "b", "c"} {
			if !strings.Contains(col2Str, expected) {
				t.Errorf("expected col2 to contain value %s, got %s", expected, col2Str)
			}
		}
	})

	t.Run("filter with not_equal condition on boolean", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []bool{true, false, true, false, true},
			"col2": []int64{1, 2, 3, 4, 5},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Apply the filter using WhereBy
		result, err := df.WhereBy("col1", series.NotEqual, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.record.NumRows() != 2 {
			t.Errorf("expected 2 rows, got %d", result.record.NumRows())
		}

		// Check if the filtered rows have the expected values
		resultCol1, err := result.Get("col1")
		if err != nil {
			t.Fatalf("failed to get col1: %v", err)
		}
		defer resultCol1.Release()

		resultCol2, err := result.Get("col2")
		if err != nil {
			t.Fatalf("failed to get col2: %v", err)
		}
		defer resultCol2.Release()

		// Check the length of the result columns
		if resultCol1.Len() != 2 {
			t.Errorf("expected length 2 for col1, got %d", resultCol1.Len())
		}
		if resultCol2.Len() != 2 {
			t.Errorf("expected length 2 for col2, got %d", resultCol2.Len())
		}

		// Check the string representation contains expected values
		col1Str := resultCol1.String()
		if !strings.Contains(col1Str, "false") {
			t.Errorf("expected col1 to contain value false, got %s", col1Str)
		}

		col2Str := resultCol2.String()
		for _, expected := range []string{"2", "4"} {
			if !strings.Contains(col2Str, expected) {
				t.Errorf("expected col2 to contain value %s, got %s", expected, col2Str)
			}
		}
	})

	t.Run("filter with no matching rows", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Apply the filter using WhereBy
		result, err := df.WhereBy("col1", series.Equal, int64(10)) // No matching value
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.record.NumRows() != 0 {
			t.Errorf("expected 0 rows, got %d", result.record.NumRows())
		}
	})

	t.Run("filter with all matching rows", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Apply the filter using WhereBy
		result, err := df.WhereBy("col1", series.GreaterEqual, int64(1)) // All values match
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.record.NumRows() != 5 {
			t.Errorf("expected 5 rows, got %d", result.record.NumRows())
		}

		// Check if all rows are present in the result
		resultCol1, err := result.Get("col1")
		if err != nil {
			t.Fatalf("failed to get col1: %v", err)
		}
		defer resultCol1.Release()

		// Check the length of the result column
		if resultCol1.Len() != 5 {
			t.Errorf("expected length 5 for col1, got %d", resultCol1.Len())
		}

		// Check the string representation contains expected values
		col1Str := resultCol1.String()
		for _, expected := range []string{"1", "2", "3", "4", "5"} {
			if !strings.Contains(col1Str, expected) {
				t.Errorf("expected col1 to contain value %s, got %s", expected, col1Str)
			}
		}
	})

	t.Run("filter with non-existent column", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Apply the filter using WhereBy with a non-existent column
		_, err = df.WhereBy("non_existent", series.Equal, int64(3))

		// Check that an error was returned
		if err == nil {
			t.Errorf("expected error for non-existent column, got nil")
		} else if err.Error() != "no such column: non_existent" {
			t.Errorf("unexpected error message: %v", err)
		}
	})
}
