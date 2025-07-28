package series

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"strings"
	"testing"
)

func TestSeries_Mean(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	// Test cases for integer types
	t.Run("int8 mean", func(t *testing.T) {
		builder := array.NewInt8Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int8{1, 2, 3, 4, 5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int8", arr)
		defer s.Release()

		// Calculate mean
		result, err := s.Mean()
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
		if resultArr.Value(0) != 3.0 {
			t.Errorf("expected mean 3.0, got %f", resultArr.Value(0))
		}
	})

	t.Run("int16 mean", func(t *testing.T) {
		builder := array.NewInt16Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int16{100, 200, 300, 400, 500}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int16", arr)
		defer s.Release()

		// Calculate mean
		result, err := s.Mean()
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
		if resultArr.Value(0) != 300.0 {
			t.Errorf("expected mean 300.0, got %f", resultArr.Value(0))
		}
	})

	t.Run("int32 mean", func(t *testing.T) {
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int32{1000, 2000, 3000, 4000, 5000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int32", arr)
		defer s.Release()

		// Calculate mean
		result, err := s.Mean()
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
		if resultArr.Value(0) != 3000.0 {
			t.Errorf("expected mean 3000.0, got %f", resultArr.Value(0))
		}
	})

	t.Run("int64 mean", func(t *testing.T) {
		builder := array.NewInt64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int64{10000, 20000, 30000, 40000, 50000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int64", arr)
		defer s.Release()

		// Calculate mean
		result, err := s.Mean()
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
		if resultArr.Value(0) != 30000.0 {
			t.Errorf("expected mean 30000.0, got %f", resultArr.Value(0))
		}
	})

	// Test cases for unsigned integer types
	t.Run("uint8 mean", func(t *testing.T) {
		builder := array.NewUint8Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint8{10, 20, 30, 40, 50}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_uint8", arr)
		defer s.Release()

		// Calculate mean
		result, err := s.Mean()
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
		if resultArr.Value(0) != 30.0 {
			t.Errorf("expected mean 30.0, got %f", resultArr.Value(0))
		}
	})

	t.Run("uint16 mean", func(t *testing.T) {
		builder := array.NewUint16Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint16{1000, 2000, 3000, 4000, 5000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_uint16", arr)
		defer s.Release()

		// Calculate mean
		result, err := s.Mean()
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
		if resultArr.Value(0) != 3000.0 {
			t.Errorf("expected mean 3000.0, got %f", resultArr.Value(0))
		}
	})

	t.Run("uint32 mean", func(t *testing.T) {
		builder := array.NewUint32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint32{10000, 20000, 30000, 40000, 50000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_uint32", arr)
		defer s.Release()

		// Calculate mean
		result, err := s.Mean()
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
		if resultArr.Value(0) != 30000.0 {
			t.Errorf("expected mean 30000.0, got %f", resultArr.Value(0))
		}
	})

	t.Run("uint64 mean", func(t *testing.T) {
		builder := array.NewUint64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint64{100000, 200000, 300000, 400000, 500000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_uint64", arr)
		defer s.Release()

		// Calculate mean
		result, err := s.Mean()
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
		if resultArr.Value(0) != 300000.0 {
			t.Errorf("expected mean 300000.0, got %f", resultArr.Value(0))
		}
	})

	// Test cases for float types
	t.Run("float32 mean", func(t *testing.T) {
		builder := array.NewFloat32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]float32{1.1, 2.2, 3.3, 4.4, 5.5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_float32", arr)
		defer s.Release()

		// Calculate mean
		result, err := s.Mean()
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
		expectedMean := 3.3
		if resultArr.Value(0) < expectedMean-0.0001 || resultArr.Value(0) > expectedMean+0.0001 {
			t.Errorf("expected mean approximately 3.3, got %f", resultArr.Value(0))
		}
	})

	t.Run("float64 mean", func(t *testing.T) {
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]float64{10.1, 20.2, 30.3, 40.4, 50.5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_float64", arr)
		defer s.Release()

		// Calculate mean
		result, err := s.Mean()
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
		expectedMean := 30.3
		if resultArr.Value(0) < expectedMean-0.0001 || resultArr.Value(0) > expectedMean+0.0001 {
			t.Errorf("expected mean approximately 30.3, got %f", resultArr.Value(0))
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

		// Calculate mean - should return an error for empty array
		_, err := s.Mean()
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
		builder.Append(20)   // Valid value
		builder.AppendNull() // Null value
		builder.Append(30)   // Valid value

		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("with_nulls", arr)
		defer s.Release()

		// Calculate mean
		result, err := s.Mean()
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
		if resultArr.Value(0) != 20.0 {
			t.Errorf("expected mean 20.0 (excluding nulls), got %f", resultArr.Value(0))
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

		// Calculate mean - should return unsupported type error
		_, err := s.Mean()
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
		builder.AppendValues([]int32{-10, -5, 0, 5, 10}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int32_neg", arr)
		defer s.Release()

		// Calculate mean
		result, err := s.Mean()
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
		if resultArr.Value(0) != 0.0 {
			t.Errorf("expected mean 0.0, got %f", resultArr.Value(0))
		}
	})
}
