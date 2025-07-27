package series

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestSeries_Max(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	// Test cases for integer types
	t.Run("int8 max", func(t *testing.T) {
		builder := array.NewInt8Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int8{5, 2, 9, 1, 7}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int8", arr)
		defer s.Release()

		// Calculate max
		result, err := s.Max()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Int8)
		if resultArr.Value(0) != 9 {
			t.Errorf("expected max 9, got %d", resultArr.Value(0))
		}
	})

	t.Run("int16 max", func(t *testing.T) {
		builder := array.NewInt16Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int16{500, 200, 900, 100, 700}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int16", arr)
		defer s.Release()

		// Calculate max
		result, err := s.Max()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Int16)
		if resultArr.Value(0) != 900 {
			t.Errorf("expected max 900, got %d", resultArr.Value(0))
		}
	})

	t.Run("int32 max", func(t *testing.T) {
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int32{5000, 2000, 9000, 1000, 7000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int32", arr)
		defer s.Release()

		// Calculate max
		result, err := s.Max()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Int32)
		if resultArr.Value(0) != 9000 {
			t.Errorf("expected max 9000, got %d", resultArr.Value(0))
		}
	})

	t.Run("int64 max", func(t *testing.T) {
		builder := array.NewInt64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int64{50000, 20000, 90000, 10000, 70000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int64", arr)
		defer s.Release()

		// Calculate max
		result, err := s.Max()
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
		if resultArr.Value(0) != 90000 {
			t.Errorf("expected max 90000, got %d", resultArr.Value(0))
		}
	})

	// Test cases for unsigned integer types
	t.Run("uint8 max", func(t *testing.T) {
		builder := array.NewUint8Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint8{50, 20, 90, 10, 70}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_uint8", arr)
		defer s.Release()

		// Calculate max
		result, err := s.Max()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Uint8)
		if resultArr.Value(0) != 90 {
			t.Errorf("expected max 90, got %d", resultArr.Value(0))
		}
	})

	t.Run("uint16 max", func(t *testing.T) {
		builder := array.NewUint16Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint16{5000, 2000, 9000, 1000, 7000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_uint16", arr)
		defer s.Release()

		// Calculate max
		result, err := s.Max()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Uint16)
		if resultArr.Value(0) != 9000 {
			t.Errorf("expected max 9000, got %d", resultArr.Value(0))
		}
	})

	t.Run("uint32 max", func(t *testing.T) {
		builder := array.NewUint32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint32{50000, 20000, 90000, 10000, 70000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_uint32", arr)
		defer s.Release()

		// Calculate max
		result, err := s.Max()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Uint32)
		if resultArr.Value(0) != 90000 {
			t.Errorf("expected max 90000, got %d", resultArr.Value(0))
		}
	})

	t.Run("uint64 max", func(t *testing.T) {
		builder := array.NewUint64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint64{500000, 200000, 900000, 100000, 700000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_uint64", arr)
		defer s.Release()

		// Calculate max
		result, err := s.Max()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Uint64)
		if resultArr.Value(0) != 900000 {
			t.Errorf("expected max 900000, got %d", resultArr.Value(0))
		}
	})

	// Test cases for float types
	t.Run("float32 max", func(t *testing.T) {
		builder := array.NewFloat32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]float32{5.5, 2.2, 9.9, 1.1, 7.7}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_float32", arr)
		defer s.Release()

		// Calculate max
		result, err := s.Max()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Float32)
		if resultArr.Value(0) != 9.9 {
			t.Errorf("expected max 9.9, got %f", resultArr.Value(0))
		}
	})

	t.Run("float64 max", func(t *testing.T) {
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]float64{50.5, 20.2, 90.9, 10.1, 70.7}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_float64", arr)
		defer s.Release()

		// Calculate max
		result, err := s.Max()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Float64)
		if resultArr.Value(0) != 90.9 {
			t.Errorf("expected max 90.9, got %f", resultArr.Value(0))
		}
	})

	// Edge cases
	t.Run("empty array", func(t *testing.T) {
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Create an empty array
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("empty", arr)
		defer s.Release()

		// Calculate max - should return an error for empty array
		_, err := s.Max()
		if err == nil {
			t.Fatalf("expected error for empty array, got nil")
		}
	})

	t.Run("array with null values", func(t *testing.T) {
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values with nulls
		builder.Append(10)   // Valid value
		builder.AppendNull() // Null value
		builder.Append(30)   // Valid value (maximum)
		builder.AppendNull() // Null value
		builder.Append(5)    // Valid value

		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("with_nulls", arr)
		defer s.Release()

		// Calculate max
		result, err := s.Max()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Int32)
		if resultArr.Value(0) != 30 {
			t.Errorf("expected max 30 (excluding nulls), got %d", resultArr.Value(0))
		}
	})

	// Unsupported type
	t.Run("unsupported type", func(t *testing.T) {
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]bool{true, false, true}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("boolean_test", arr)
		defer s.Release()

		// Calculate max - should return unsupported type error
		_, err := s.Max()
		if err == nil {
			t.Fatalf("expected unsupported type error, got nil")
		}

		// Check that the error message mentions the boolean type
		expectedType := arrow.FixedWidthTypes.Boolean.String()
		if !strings.Contains(err.Error(), expectedType) {
			t.Errorf("expected error message to contain %q, got: %v", expectedType, err)
		}
	})

	// Test with negative values
	t.Run("int32 with negative values", func(t *testing.T) {
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values including negatives
		builder.AppendValues([]int32{5, -10, 3, -20, 7}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int32_neg", arr)
		defer s.Release()

		// Calculate max
		result, err := s.Max()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check value
		resultArr := result.array.(*array.Int32)
		if resultArr.Value(0) != 7 {
			t.Errorf("expected max 7, got %d", resultArr.Value(0))
		}
	})
}
