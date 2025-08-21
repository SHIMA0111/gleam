package series

import (
	"github.com/SHIMA0111/gleam/gleam/utils"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestSeries_Where(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	t.Run("int64 equal filter", func(t *testing.T) {
		// Create a builder for int64 values
		builder := array.NewInt64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)
		defer s.Release()

		// Filter where values equal 3
		result, err := s.Where(utils.Equal, int64(3))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.Int64)
		if resultArr.Value(0) != 3 {
			t.Errorf("expected value 3, got %d", resultArr.Value(0))
		}
	})

	t.Run("int64 greater filter", func(t *testing.T) {
		// Create a builder for int64 values
		builder := array.NewInt64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)
		defer s.Release()

		// Filter where values greater than 3
		result, err := s.Where(utils.Greater, int64(3))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 2 {
			t.Errorf("expected length 2, got %d", result.Len())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.Int64)
		if resultArr.Value(0) != 4 || resultArr.Value(1) != 5 {
			t.Errorf("expected values [4, 5], got [%d, %d]", resultArr.Value(0), resultArr.Value(1))
		}
	})

	t.Run("uint32 equal filter", func(t *testing.T) {
		// Create a builder for uint32 values
		builder := array.NewUint32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint32{1, 2, 3, 4, 5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)
		defer s.Release()

		// Filter where values equal 3
		result, err := s.Where(utils.Equal, uint32(3))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.Uint32)
		if resultArr.Value(0) != 3 {
			t.Errorf("expected value 3, got %d", resultArr.Value(0))
		}
	})

	t.Run("uint64 equal filter", func(t *testing.T) {
		// Create a builder for uint64 values
		builder := array.NewUint64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]uint64{1, 2, 3, 4, 5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)
		defer s.Release()

		// Filter where values equal 3
		result, err := s.Where(utils.Equal, uint64(3))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 1 {
			t.Errorf("expected length 1, got %d", result.Len())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.Uint64)
		if resultArr.Value(0) != 3 {
			t.Errorf("expected value 3, got %d", resultArr.Value(0))
		}
	})

	t.Run("float64 less_equal filter", func(t *testing.T) {
		// Create a builder for float64 values
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)
		defer s.Release()

		// Filter where values less than or equal to 3.3
		result, err := s.Where(utils.LessEqual, 3.3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 3 {
			t.Errorf("expected length 3, got %d", result.Len())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.Float64)
		expected := []float64{1.1, 2.2, 3.3}
		for i, v := range expected {
			if resultArr.Value(i) != v {
				t.Errorf("at index %d: expected value %f, got %f", i, v, resultArr.Value(i))
			}
		}
	})

	t.Run("string not_equal filter", func(t *testing.T) {
		// Create a builder for string values
		builder := array.NewStringBuilder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]string{"apple", "banana", "cherry", "date", "elderberry"}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)
		defer s.Release()

		// Filter where values not equal to "cherry"
		result, err := s.Where(utils.NotEqual, "cherry")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 4 {
			t.Errorf("expected length 4, got %d", result.Len())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.String)
		expected := []string{"apple", "banana", "date", "elderberry"}
		for i, v := range expected {
			if resultArr.Value(i) != v {
				t.Errorf("at index %d: expected value %s, got %s", i, v, resultArr.Value(i))
			}
		}
	})

	t.Run("boolean filter", func(t *testing.T) {
		// Create a builder for boolean values
		builder := array.NewBooleanBuilder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]bool{true, false, true, false, true}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)
		defer s.Release()

		// Filter where values equal true
		result, err := s.Where(utils.Equal, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 3 {
			t.Errorf("expected length 3, got %d", result.Len())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.Boolean)
		for i := 0; i < result.Len(); i++ {
			if !resultArr.Value(i) {
				t.Errorf("at index %d: expected value true, got false", i)
			}
		}
	})

	t.Run("with null values", func(t *testing.T) {
		// Create a builder for int32 values
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values with nulls
		values := []int32{1, 2, 3, 4, 5}
		valids := []bool{true, false, true, true, false} // 2 and 5 are null
		builder.AppendValues(values, valids)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)
		defer s.Release()

		// Filter where values greater than 2
		result, err := s.Where(utils.Greater, int32(2))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result - should only include 3 and 4 (nulls are dropped)
		if result.Len() != 2 {
			t.Errorf("expected length 2, got %d", result.Len())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.Int32)
		expected := []int32{3, 4}
		for i, v := range expected {
			if resultArr.Value(i) != v {
				t.Errorf("at index %d: expected value %d, got %d", i, v, resultArr.Value(i))
			}
		}
	})

	t.Run("type mismatch error", func(t *testing.T) {
		// Create a builder for int64 values
		builder := array.NewInt64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)
		defer s.Release()

		// Try to filter with a string value (type mismatch)
		_, err := s.Where(utils.Equal, "3")
		if err == nil {
			t.Fatalf("expected type mismatch error, got nil")
		}
	})

	t.Run("int32 greater_equal filter", func(t *testing.T) {
		// Create a builder for int32 values
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)
		defer s.Release()

		// Filter where values greater than or equal to 3
		result, err := s.Where(utils.GreaterEqual, int32(3))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 3 {
			t.Errorf("expected length 3, got %d", result.Len())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.Int32)
		expected := []int32{3, 4, 5}
		for i, v := range expected {
			if resultArr.Value(i) != v {
				t.Errorf("at index %d: expected value %d, got %d", i, v, resultArr.Value(i))
			}
		}
	})

	t.Run("float32 less filter", func(t *testing.T) {
		// Create a builder for float32 values
		builder := array.NewFloat32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]float32{1.1, 2.2, 3.3, 4.4, 5.5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)
		defer s.Release()

		// Filter where values less than 3.3
		result, err := s.Where(utils.Less, float32(3.3))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 2 {
			t.Errorf("expected length 2, got %d", result.Len())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.Float32)
		expected := []float32{1.1, 2.2}
		for i, v := range expected {
			if resultArr.Value(i) != v {
				t.Errorf("at index %d: expected value %f, got %f", i, v, resultArr.Value(i))
			}
		}
	})

	t.Run("unsupported type error", func(t *testing.T) {
		// Create a builder for int64 values
		builder := array.NewInt64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)
		defer s.Release()

		// Try to filter with a complex value (unsupported type)
		_, err := s.Where(utils.Equal, complex(1, 2))
		if err == nil {
			t.Fatalf("expected unsupported type error, got nil")
		}
	})
}

func TestMakeScalar(t *testing.T) {
	testCases := []struct {
		name     string
		input    interface{}
		expected arrow.DataType
	}{
		{"int8", int8(1), arrow.PrimitiveTypes.Int8},
		{"int16", int16(1), arrow.PrimitiveTypes.Int16},
		{"int32", int32(1), arrow.PrimitiveTypes.Int32},
		{"int64", int64(1), arrow.PrimitiveTypes.Int64},
		{"uint8", uint8(1), arrow.PrimitiveTypes.Uint8},
		{"uint16", uint16(1), arrow.PrimitiveTypes.Uint16},
		{"uint32", uint32(1), arrow.PrimitiveTypes.Uint32},
		{"uint64", uint64(1), arrow.PrimitiveTypes.Uint64},
		{"float32", float32(1.0), arrow.PrimitiveTypes.Float32},
		{"float64", float64(1.0), arrow.PrimitiveTypes.Float64},
		{"string", "test", arrow.BinaryTypes.String},
		{"bool", true, arrow.FixedWidthTypes.Boolean},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scl, err := makeScalar(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !arrow.TypeEqual(scl.DataType(), tc.expected) {
				t.Errorf("expected type %s, got %s", tc.expected, scl.DataType())
			}
		})
	}

	t.Run("unsupported type", func(t *testing.T) {
		_, err := makeScalar(complex(1, 2))
		if err == nil {
			t.Fatalf("expected unsupported type error, got nil")
		}
	})

	t.Run("int conversion", func(t *testing.T) {
		scl, err := makeScalar(1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Check if the scalar type matches the platform's int size
		if strconv.IntSize == 32 {
			if !arrow.TypeEqual(scl.DataType(), arrow.PrimitiveTypes.Int32) {
				t.Errorf("expected type Int32, got %s", scl.DataType())
			}
		} else {
			if !arrow.TypeEqual(scl.DataType(), arrow.PrimitiveTypes.Int64) {
				t.Errorf("expected type Int64, got %s", scl.DataType())
			}
		}
	})

	t.Run("uint conversion", func(t *testing.T) {
		scl, err := makeScalar(uint(1))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Check if the scalar type matches the platform's int size
		if strconv.IntSize == 32 {
			if !arrow.TypeEqual(scl.DataType(), arrow.PrimitiveTypes.Uint32) {
				t.Errorf("expected type Uint32, got %s", scl.DataType())
			}
		} else {
			if !arrow.TypeEqual(scl.DataType(), arrow.PrimitiveTypes.Uint64) {
				t.Errorf("expected type Uint64, got %s", scl.DataType())
			}
		}
	})
}
