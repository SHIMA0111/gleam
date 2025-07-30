package dataframe

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestDataFrame_Select(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	t.Run("select single column", func(t *testing.T) {
		// Create a test DataFrame with multiple columns
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
			"col2": []float64{1.1, 2.2, 3.3, 4.4, 5.5},
			"col3": []string{"a", "b", "c", "d", "e"},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Select a single column
		result, err := df.Select([]string{"col2"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if len(result.colMap) != 1 {
			t.Errorf("expected 1 column, got %d", len(result.colMap))
		}

		// Check if the column exists in the result
		if _, ok := result.colMap["col2"]; !ok {
			t.Errorf("expected column 'col2' to exist in result")
		}

		// Check if the column has the correct type
		field := result.record.Schema().Field(0)
		if field.Name != "col2" {
			t.Errorf("expected column name 'col2', got '%s'", field.Name)
		}
		if !arrow.TypeEqual(field.Type, arrow.PrimitiveTypes.Float64) {
			t.Errorf("expected column type Float64, got %s", field.Type)
		}

		// Check if the column has the correct length
		if result.record.Column(0).Len() != 5 {
			t.Errorf("expected column length 5, got %d", result.record.Column(0).Len())
		}
	})

	t.Run("select multiple columns", func(t *testing.T) {
		// Create a test DataFrame with multiple columns
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
			"col2": []float64{1.1, 2.2, 3.3, 4.4, 5.5},
			"col3": []string{"a", "b", "c", "d", "e"},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Select multiple columns
		result, err := df.Select([]string{"col1", "col3"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if len(result.colMap) != 2 {
			t.Errorf("expected 2 columns, got %d", len(result.colMap))
		}

		// Check if the columns exist in the result
		if _, ok := result.colMap["col1"]; !ok {
			t.Errorf("expected column 'col1' to exist in result")
		}
		if _, ok := result.colMap["col3"]; !ok {
			t.Errorf("expected column 'col3' to exist in result")
		}

		// Check if the columns have the correct types
		field1 := result.record.Schema().Field(0)
		if field1.Name != "col1" {
			t.Errorf("expected column name 'col1', got '%s'", field1.Name)
		}
		if !arrow.TypeEqual(field1.Type, arrow.PrimitiveTypes.Int64) {
			t.Errorf("expected column type Int64, got %s", field1.Type)
		}

		field2 := result.record.Schema().Field(1)
		if field2.Name != "col3" {
			t.Errorf("expected column name 'col3', got '%s'", field2.Name)
		}
		if !arrow.TypeEqual(field2.Type, arrow.BinaryTypes.String) {
			t.Errorf("expected column type String, got %s", field2.Type)
		}

		// Check if the columns have the correct length
		if result.record.Column(0).Len() != 5 {
			t.Errorf("expected column length 5, got %d", result.record.Column(0).Len())
		}
		if result.record.Column(1).Len() != 5 {
			t.Errorf("expected column length 5, got %d", result.record.Column(1).Len())
		}
	})

	t.Run("select with empty column list", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Try to select with empty column list
		_, err = df.Select([]string{})
		if err == nil {
			t.Errorf("expected error for empty column list, got nil")
		}
	})

	t.Run("select non-existent column", func(t *testing.T) {
		// Create a test DataFrame
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Try to select a non-existent column
		// This should return an error
		_, err = df.Select([]string{"non_existent"})
		if err == nil {
			t.Errorf("expected error for non-existent column, got nil")
		} else if err.Error() != "column non_existent not found" {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("select all columns in different order", func(t *testing.T) {
		// Create a test DataFrame with multiple columns
		df, err := NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
			"col1": []int64{1, 2, 3, 4, 5},
			"col2": []float64{1.1, 2.2, 3.3, 4.4, 5.5},
			"col3": []string{"a", "b", "c", "d", "e"},
		})
		if err != nil {
			t.Fatalf("failed to create DataFrame: %v", err)
		}
		defer df.Release()

		// Select all columns in a different order
		result, err := df.Select([]string{"col3", "col1", "col2"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if len(result.colMap) != 3 {
			t.Errorf("expected 3 columns, got %d", len(result.colMap))
		}

		// Check if the columns exist in the result
		if _, ok := result.colMap["col1"]; !ok {
			t.Errorf("expected column 'col1' to exist in result")
		}
		if _, ok := result.colMap["col2"]; !ok {
			t.Errorf("expected column 'col2' to exist in result")
		}
		if _, ok := result.colMap["col3"]; !ok {
			t.Errorf("expected column 'col3' to exist in result")
		}

		// Check if the columns are in the correct order
		field1 := result.record.Schema().Field(0)
		if field1.Name != "col3" {
			t.Errorf("expected first column name 'col3', got '%s'", field1.Name)
		}

		field2 := result.record.Schema().Field(1)
		if field2.Name != "col1" {
			t.Errorf("expected second column name 'col1', got '%s'", field2.Name)
		}

		field3 := result.record.Schema().Field(2)
		if field3.Name != "col2" {
			t.Errorf("expected third column name 'col2', got '%s'", field3.Name)
		}
	})
}
