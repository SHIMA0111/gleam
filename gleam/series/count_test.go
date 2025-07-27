package series

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestSeries_Count(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	t.Run("int32 array with no nulls", func(t *testing.T) {
		// Create a builder for int32 values
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values with no nulls
		builder.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int32", arr)
		defer s.Release()

		// Calculate count
		result, err := s.Count()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Int64)
		if resultArr.Value(0) != 5 {
			t.Errorf("expected count 5, got %d", resultArr.Value(0))
		}
	})

	t.Run("float64 array with some nulls", func(t *testing.T) {
		// Create a builder for float64 values
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()

		// Append values with some nulls
		values := []float64{1.1, 2.2, 3.3, 4.4, 5.5}
		valids := []bool{true, false, true, false, true} // 2 nulls
		builder.AppendValues(values, valids)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_float64", arr)
		defer s.Release()

		// Calculate count
		result, err := s.Count()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Int64)
		if resultArr.Value(0) != 3 {
			t.Errorf("expected count 3 (excluding nulls), got %d", resultArr.Value(0))
		}
	})

	t.Run("string array with all nulls", func(t *testing.T) {
		// Create a builder for string values
		builder := array.NewStringBuilder(mem)
		defer builder.Release()

		// Append only null values
		valids := []bool{false, false, false} // all nulls
		builder.AppendValues([]string{"", "", ""}, valids)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_string", arr)
		defer s.Release()

		// Calculate count
		result, err := s.Count()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Int64)
		if resultArr.Value(0) != 0 {
			t.Errorf("expected count 0 (all nulls), got %d", resultArr.Value(0))
		}
	})

	t.Run("boolean array with mixed nulls", func(t *testing.T) {
		// Create a builder for boolean values
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()

		// Append values with mixed null pattern
		builder.AppendValues([]bool{true, false, true}, []bool{true, true, false})
		builder.AppendNull()
		builder.Append(true)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_boolean", arr)
		defer s.Release()

		// Calculate count
		result, err := s.Count()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Int64)
		if resultArr.Value(0) != 3 {
			t.Errorf("expected count 3 (excluding nulls), got %d", resultArr.Value(0))
		}
	})

	t.Run("empty array", func(t *testing.T) {
		// Create a builder for int32 values
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Create an empty array
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("empty", arr)
		defer s.Release()

		// Calculate count
		result, err := s.Count()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Int64)
		if resultArr.Value(0) != 0 {
			t.Errorf("expected count 0 for empty array, got %d", resultArr.Value(0))
		}
	})

	t.Run("large array", func(t *testing.T) {
		// Create a builder for int32 values
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append a large number of values with some nulls
		const size = 10000
		values := make([]int32, size)
		valids := make([]bool, size)
		nullCount := 0

		for i := 0; i < size; i++ {
			values[i] = int32(i)
			// Make every 3rd value null
			valids[i] = i%3 != 0
			if !valids[i] {
				nullCount++
			}
		}

		builder.AppendValues(values, valids)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("large_array", arr)
		defer s.Release()

		// Calculate count
		result, err := s.Count()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Int64)
		expectedCount := size - nullCount
		if resultArr.Value(0) != int64(expectedCount) {
			t.Errorf("expected count %d, got %d", expectedCount, resultArr.Value(0))
		}
	})
}
