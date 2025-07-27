package series

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestSeries_Cast(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	t.Run("int8 to int16", func(t *testing.T) {
		// Create a builder for int8 values
		builder := array.NewInt8Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int8{1, 2, 3, 4, 5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int8", arr)
		defer s.Release()

		// Cast to int16
		result, err := s.Cast(Int16)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 5 {
			t.Errorf("expected length 5, got %d", result.Len())
		}

		// Check data type
		if !arrow.TypeEqual(result.DType(), arrow.PrimitiveTypes.Int16) {
			t.Errorf("expected type Int16, got %s", result.DType())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.Int16)
		expected := []int16{1, 2, 3, 4, 5}
		for i, v := range expected {
			if resultArr.Value(i) != v {
				t.Errorf("at index %d: expected value %d, got %d", i, v, resultArr.Value(i))
			}
		}
	})

	t.Run("int32 to float32", func(t *testing.T) {
		// Create a builder for int32 values
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int32", arr)
		defer s.Release()

		// Cast to float32
		result, err := s.Cast(Float32)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 5 {
			t.Errorf("expected length 5, got %d", result.Len())
		}

		// Check data type
		if !arrow.TypeEqual(result.DType(), arrow.PrimitiveTypes.Float32) {
			t.Errorf("expected type Float32, got %s", result.DType())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.Float32)
		expected := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
		for i, v := range expected {
			if resultArr.Value(i) != v {
				t.Errorf("at index %d: expected value %f, got %f", i, v, resultArr.Value(i))
			}
		}
	})

	t.Run("float32 to int32", func(t *testing.T) {
		// Create a builder for float32 values
		builder := array.NewFloat32Builder(mem)
		defer builder.Release()

		// Append values (using whole numbers to avoid truncation errors)
		builder.AppendValues([]float32{1.0, 2.0, 3.0, 4.0, 5.0}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_float32", arr)
		defer s.Release()

		// Cast to int32
		result, err := s.Cast(Int32)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 5 {
			t.Errorf("expected length 5, got %d", result.Len())
		}

		// Check data type
		if !arrow.TypeEqual(result.DType(), arrow.PrimitiveTypes.Int32) {
			t.Errorf("expected type Int32, got %s", result.DType())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.Int32)
		expected := []int32{1, 2, 3, 4, 5}
		for i, v := range expected {
			if resultArr.Value(i) != v {
				t.Errorf("at index %d: expected value %d, got %d", i, v, resultArr.Value(i))
			}
		}
	})

	t.Run("int32 to string", func(t *testing.T) {
		// Create a builder for int32 values
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int32{1, 2, 3}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int32", arr)
		defer s.Release()

		// Cast to string
		result, err := s.Cast(String)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 3 {
			t.Errorf("expected length 3, got %d", result.Len())
		}

		// Check data type
		if !arrow.TypeEqual(result.DType(), arrow.BinaryTypes.String) {
			t.Errorf("expected type String, got %s", result.DType())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.String)
		expected := []string{"1", "2", "3"}
		for i, v := range expected {
			if resultArr.Value(i) != v {
				t.Errorf("at index %d: expected value %s, got %s", i, v, resultArr.Value(i))
			}
		}
	})

	t.Run("string to int32", func(t *testing.T) {
		// Create a builder for string values
		builder := array.NewStringBuilder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]string{"1", "2", "3"}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_string", arr)
		defer s.Release()

		// Cast to int32
		result, err := s.Cast(Int32)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 3 {
			t.Errorf("expected length 3, got %d", result.Len())
		}

		// Check data type
		if !arrow.TypeEqual(result.DType(), arrow.PrimitiveTypes.Int32) {
			t.Errorf("expected type Int32, got %s", result.DType())
		}

		// Access the underlying array to check values
		resultArr := result.array.(*array.Int32)
		expected := []int32{1, 2, 3}
		for i, v := range expected {
			if resultArr.Value(i) != v {
				t.Errorf("at index %d: expected value %d, got %d", i, v, resultArr.Value(i))
			}
		}
	})

	t.Run("same type", func(t *testing.T) {
		// Create a builder for int32 values
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int32{1, 2, 3}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int32", arr)
		defer s.Release()

		// Cast to the same type
		result, err := s.Cast(Int32)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// For same type, the original series should be returned
		if result != s {
			t.Errorf("expected the same series to be returned for same type cast")
		}
	})

	t.Run("with null values", func(t *testing.T) {
		// Create a builder for int32 values
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values with nulls
		values := []int32{1, 2, 3, 4, 5}
		valids := []bool{true, false, true, false, true} // 2nd and 4th are null
		builder.AppendValues(values, valids)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int32", arr)
		defer s.Release()

		// Cast to float32
		result, err := s.Cast(Float32)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 5 {
			t.Errorf("expected length 5, got %d", result.Len())
		}

		// Check null values are preserved
		for i := 0; i < result.Len(); i++ {
			if result.IsNull(i) != s.IsNull(i) {
				t.Errorf("at index %d: null status not preserved, expected %v, got %v", i, s.IsNull(i), result.IsNull(i))
			}
		}

		// Check non-null values are correctly converted
		resultArr := result.array.(*array.Float32)
		for i := 0; i < result.Len(); i++ {
			if !result.IsNull(i) {
				expected := float32(values[i])
				if resultArr.Value(i) != expected {
					t.Errorf("at index %d: expected value %f, got %f", i, expected, resultArr.Value(i))
				}
			}
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

		// Cast to float32
		result, err := s.Cast(Float32)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer result.Release()

		// Check result
		if result.Len() != 0 {
			t.Errorf("expected length 0, got %d", result.Len())
		}

		// Check data type
		if !arrow.TypeEqual(result.DType(), arrow.PrimitiveTypes.Float32) {
			t.Errorf("expected type Float32, got %s", result.DType())
		}
	})

	t.Run("unsupported cast", func(t *testing.T) {
		// Create a builder for int32 values
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int32{1, 2, 3}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int32", arr)
		defer s.Release()

		// Cast to unsupported type
		_, err := s.Cast(Unsupported)
		if err == nil {
			t.Fatalf("expected error for unsupported type, got nil")
		}
	})
}
