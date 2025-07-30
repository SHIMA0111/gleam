package dataframe

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestNewDataFrame(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	// Create a simple record batch
	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "col1", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer record.Release()

	// Create a DataFrame
	df := NewDataFrame(mem, record)
	defer df.Release()

	// Check that the DataFrame was created correctly
	if df.record.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", df.record.NumRows())
	}
	if df.record.NumCols() != 1 {
		t.Errorf("expected 1 column, got %d", df.record.NumCols())
	}
	if len(df.colMap) != 1 {
		t.Errorf("expected 1 column in colMap, got %d", len(df.colMap))
	}
	if _, ok := df.colMap["col1"]; !ok {
		t.Errorf("expected column 'col1' to exist in colMap")
	}
}

func TestNewDataFrameFromMapWithMemory(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	t.Run("create with valid data", func(t *testing.T) {
		// Create a DataFrame from a map
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
			"col2": []float64{1.1, 2.2, 3.3, 4.4, 5.5},
			"col3": []string{"a", "b", "c", "d", "e"},
			"col4": []bool{true, false, true, false, true},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer df.Release()

		// Check that the DataFrame was created correctly
		if df.record.NumRows() != 5 {
			t.Errorf("expected 5 rows, got %d", df.record.NumRows())
		}
		if df.record.NumCols() != 4 {
			t.Errorf("expected 4 columns, got %d", df.record.NumCols())
		}
		if len(df.colMap) != 4 {
			t.Errorf("expected 4 columns in colMap, got %d", len(df.colMap))
		}

		// Check that all columns exist in colMap
		for _, col := range []string{"col1", "col2", "col3", "col4"} {
			if _, ok := df.colMap[col]; !ok {
				t.Errorf("expected column '%s' to exist in colMap", col)
			}
		}

		// Check that the columns have the correct types
		schema := df.record.Schema()
		if !arrow.TypeEqual(schema.Field(int(df.colMap["col1"])).Type, arrow.PrimitiveTypes.Int64) {
			t.Errorf("expected col1 to have type Int64, got %s", schema.Field(int(df.colMap["col1"])).Type)
		}
		if !arrow.TypeEqual(schema.Field(int(df.colMap["col2"])).Type, arrow.PrimitiveTypes.Float64) {
			t.Errorf("expected col2 to have type Float64, got %s", schema.Field(int(df.colMap["col2"])).Type)
		}
		if !arrow.TypeEqual(schema.Field(int(df.colMap["col3"])).Type, arrow.BinaryTypes.String) {
			t.Errorf("expected col3 to have type String, got %s", schema.Field(int(df.colMap["col3"])).Type)
		}
		if !arrow.TypeEqual(schema.Field(int(df.colMap["col4"])).Type, arrow.FixedWidthTypes.Boolean) {
			t.Errorf("expected col4 to have type Boolean, got %s", schema.Field(int(df.colMap["col4"])).Type)
		}
	})

	t.Run("create with empty data", func(t *testing.T) {
		// Try to create a DataFrame with empty data
		_, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{})
		if err == nil {
			t.Errorf("expected error for empty data, got nil")
		} else if err.Error() != "data must not be empty" {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("create with unsupported data type", func(t *testing.T) {
		// Try to create a DataFrame with unsupported data type
		_, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []complex128{1 + 2i, 3 + 4i},
		})
		if err == nil {
			t.Errorf("expected error for unsupported data type, got nil")
		} else if !strings.Contains(err.Error(), "unsupported data type") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("create with columns of different lengths", func(t *testing.T) {
		// Try to create a DataFrame with columns of different lengths
		_, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
			"col2": []float64{1.1, 2.2, 3.3},
		})
		if err == nil {
			t.Errorf("expected error for columns of different lengths, got nil")
		} else if err.Error() != "all columns must have the same length" {
			t.Errorf("unexpected error message: %v", err)
		}
	})
}

func TestNewDataFrameFromMap(t *testing.T) {
	// Create a DataFrame from a map using the default allocator
	df, err := NewDataFrameFromMap(map[string]interface{}{
		"col1": []int64{1, 2, 3, 4, 5},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer df.Release()

	// Check that the DataFrame was created correctly
	if df.record.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", df.record.NumRows())
	}
	if df.record.NumCols() != 1 {
		t.Errorf("expected 1 column, got %d", df.record.NumCols())
	}
	if len(df.colMap) != 1 {
		t.Errorf("expected 1 column in colMap, got %d", len(df.colMap))
	}
	if _, ok := df.colMap["col1"]; !ok {
		t.Errorf("expected column 'col1' to exist in colMap")
	}
}

func TestDataFrame_Release(t *testing.T) {
	// Create a DataFrame
	df, err := NewDataFrameFromMap(map[string]interface{}{
		"col1": []int64{1, 2, 3, 4, 5},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Release the DataFrame
	df.Release()

	// Check that the record was released
	// Note: We can't directly check if the record was released,
	// but we can check that calling Release again doesn't panic
	df.Release() // This should not panic
}

func TestDataFrame_String(t *testing.T) {
	// Create a DataFrame
	df, err := NewDataFrameFromMap(map[string]interface{}{
		"col1": []int64{1, 2, 3, 4, 5},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer df.Release()

	// Get the string representation
	str := df.String()

	// Check that the string representation contains expected values
	if !strings.Contains(str, "DataFrame") {
		t.Errorf("expected string representation to contain 'DataFrame', got %s", str)
	}
	if !strings.Contains(str, "col1") {
		t.Errorf("expected string representation to contain 'col1', got %s", str)
	}
	for _, expected := range []string{"1", "2", "3", "4", "5"} {
		if !strings.Contains(str, expected) {
			t.Errorf("expected string representation to contain '%s', got %s", expected, str)
		}
	}
}

func TestDataFrame_Get(t *testing.T) {
	// Create a DataFrame
	df, err := NewDataFrameFromMap(map[string]interface{}{
		"col1": []int64{1, 2, 3, 4, 5},
		"col2": []float64{1.1, 2.2, 3.3, 4.4, 5.5},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer df.Release()

	t.Run("get existing column", func(t *testing.T) {
		// Get a column
		series, err := df.Get("col1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer series.Release()

		// Check that the series was created correctly
		if series.Len() != 5 {
			t.Errorf("expected 5 rows, got %d", series.Len())
		}
		if series.Name() != "col1" {
			t.Errorf("expected name 'col1', got '%s'", series.Name())
		}
		if !arrow.TypeEqual(series.DType(), arrow.PrimitiveTypes.Int64) {
			t.Errorf("expected type Int64, got %s", series.DType())
		}

		// Check the string representation contains expected values
		str := series.String()
		for _, expected := range []string{"1", "2", "3", "4", "5"} {
			if !strings.Contains(str, expected) {
				t.Errorf("expected string representation to contain '%s', got %s", expected, str)
			}
		}
	})

	t.Run("get non-existent column", func(t *testing.T) {
		// Try to get a non-existent column
		_, err := df.Get("non_existent")
		if err == nil {
			t.Errorf("expected error for non-existent column, got nil")
		} else if err.Error() != "no such column: non_existent" {
			t.Errorf("unexpected error message: %v", err)
		}
	})
}
