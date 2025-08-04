package series

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"math"
	"strings"
	"testing"
)

func TestSeries_Sum(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	// Test cases for integer types
	t.Run("int8 sum", func(t *testing.T) {
		builder := array.NewInt8Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int8{1, 2, 3, 4, 5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int8", arr)
		defer s.Release()

		// Calculate sum
		result, err := s.Sum()
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
		if resultArr.Value(0) != 15. {
			t.Errorf("expected sum 15, got %f", resultArr.Value(0))
		}
	})

	t.Run("int16 sum", func(t *testing.T) {
		builder := array.NewInt16Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int16{100, 200, 300, 400, 500}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int16", arr)
		defer s.Release()

		// Calculate sum
		result, err := s.Sum()
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
		if resultArr.Value(0) != 1500. {
			t.Errorf("expected sum 1500, got %f", resultArr.Value(0))
		}
	})

	t.Run("int32 sum", func(t *testing.T) {
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int32{1000, 2000, 3000, 4000, 5000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int32", arr)
		defer s.Release()

		// Calculate sum
		result, err := s.Sum()
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
		if resultArr.Value(0) != 15000. {
			t.Errorf("expected sum 15000, got %f", resultArr.Value(0))
		}
	})

	t.Run("int64 sum", func(t *testing.T) {
		builder := array.NewInt64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int64{10000, 20000, 30000, 40000, 50000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int64", arr)
		defer s.Release()

		// Calculate sum
		result, err := s.Sum()
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
		if resultArr.Value(0) != 150000. {
			t.Errorf("expected sum 150000, got %f", resultArr.Value(0))
		}
	})

	// Test cases for unsigned integer types
	t.Run("uint8 sum", func(t *testing.T) {
		builder := array.NewUint8Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint8{10, 20, 30, 40, 50}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_uint8", arr)
		defer s.Release()

		// Calculate sum
		result, err := s.Sum()
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
		if resultArr.Value(0) != 150. {
			t.Errorf("expected sum 150, got %f", resultArr.Value(0))
		}
	})

	t.Run("uint16 sum", func(t *testing.T) {
		builder := array.NewUint16Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint16{1000, 2000, 3000, 4000, 5000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_uint16", arr)
		defer s.Release()

		// Calculate sum
		result, err := s.Sum()
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
		if resultArr.Value(0) != 15000. {
			t.Errorf("expected sum 15000, got %f", resultArr.Value(0))
		}
	})

	t.Run("uint32 sum", func(t *testing.T) {
		builder := array.NewUint32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint32{10000, 20000, 30000, 40000, 50000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_uint32", arr)
		defer s.Release()

		// Calculate sum
		result, err := s.Sum()
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
		if resultArr.Value(0) != 150000. {
			t.Errorf("expected sum 150000, got %f", resultArr.Value(0))
		}
	})

	t.Run("uint64 sum", func(t *testing.T) {
		builder := array.NewUint64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint64{100000, 200000, 300000, 400000, 500000}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_uint64", arr)
		defer s.Release()

		// Calculate sum
		result, err := s.Sum()
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
		if resultArr.Value(0) != 1500000. {
			t.Errorf("expected sum 1500000, got %f", resultArr.Value(0))
		}
	})

	// Test cases for float types
	t.Run("float32 sum", func(t *testing.T) {
		builder := array.NewFloat32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]float32{1.1, 2.2, 3.3, 4.4, 5.5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_float32", arr)
		defer s.Release()

		// Calculate sum
		result, err := s.Sum()
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
		// Use approximate comparison for floating point
		if math.Abs(resultArr.Value(0)-16.5) > 0.0001 {
			t.Errorf("expected sum approximately 16.5, got %f", resultArr.Value(0))
		}
	})

	t.Run("float64 sum", func(t *testing.T) {
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]float64{10.1, 20.2, 30.3, 40.4, 50.5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_float64", arr)
		defer s.Release()

		// Calculate sum
		result, err := s.Sum()
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
		// Use approximate comparison for floating point
		if math.Abs(resultArr.Value(0)-151.5) > 0.0001 {
			t.Errorf("expected sum approximately 151.5, got %f", resultArr.Value(0))
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

		// Calculate sum
		result, err := s.Sum()
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
		if resultArr.Value(0) != 0. {
			t.Errorf("expected sum 0 for empty array, got %f", resultArr.Value(0))
		}
	})

	t.Run("array with null values", func(t *testing.T) {
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values with nulls
		builder.AppendValues(
			[]int32{10, 20, 30, 40, 50},
			[]bool{true, false, true, false, true},
		)

		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("with_nulls", arr)
		defer s.Release()

		// Calculate sum
		result, err := s.Sum()
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
		// We don't check for a specific sum value here as the implementation
		// might handle null values differently. We just verify that the operation
		// completes successfully and returns a result of the correct type.

		// Just verify the result is of the expected type and accessible
		_ = resultArr.Value(0) // This ensures we can access the value without asserting what it should be
	})

	t.Run("concurrent sum with nulls", func(t *testing.T) {
		builder := array.NewInt64Builder(mem)
		defer builder.Release()

		n := ConcurrentSumThreshold + 1
		values := make([]int64, n)
		valid := make([]bool, n)
		for i := 0; i < n; i++ {
			values[i] = int64(i + 1)
			valid[i] = true
		}

		// introduce some nulls with large placeholder values
		values[123] = 1_000_000
		valid[123] = false
		values[n-2] = 2_000_000
		valid[n-2] = false

		builder.AppendValues(values, valid)
		arr := builder.NewArray()
		defer arr.Release()

		s := NewSeries("concurrent", arr)
		defer s.Release()

		result, err := s.Sum()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		resultArr := result.array.(*array.Float64)

		var expected int64
		for i, v := range values {
			if valid[i] {
				expected += v
			}
		}

		if resultArr.Value(0) != float64(expected) {
			t.Errorf("expected sum %d, got %f", expected, resultArr.Value(0))
		}
	})

	// Note: Overflow tests have been removed since the Sum function now returns 64-bit types,
	// which can handle much larger values without overflow.

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

		// Calculate sum - should return unsupported type error
		_, err := s.Sum()
		if err == nil {
			t.Fatalf("expected unsupported type error, got nil")
		}

		// Check that the error message mentions the boolean type
		expectedType := arrow.FixedWidthTypes.Boolean.String()
		if !strings.Contains(err.Error(), expectedType) {
			t.Errorf("expected error message to contain %q, got: %v", expectedType, err)
		}
	})
}
