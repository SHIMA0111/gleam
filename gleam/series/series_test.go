package series

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestNewSeries(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	t.Run("create with int64 array", func(t *testing.T) {
		// Create a builder for int64 values
		builder := array.NewInt64Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_int64", arr)
		defer s.Release()

		// Check series properties
		if s.Name() != "test_int64" {
			t.Errorf("expected name 'test_int64', got '%s'", s.Name())
		}

		if s.Len() != 5 {
			t.Errorf("expected length 5, got %d", s.Len())
		}

		if !arrow.TypeEqual(s.DType(), arrow.PrimitiveTypes.Int64) {
			t.Errorf("expected type Int64, got %s", s.DType())
		}
	})

	t.Run("create with string array", func(t *testing.T) {
		// Create a builder for string values
		builder := array.NewStringBuilder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]string{"apple", "banana", "cherry"}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_string", arr)
		defer s.Release()

		// Check series properties
		if s.Name() != "test_string" {
			t.Errorf("expected name 'test_string', got '%s'", s.Name())
		}

		if s.Len() != 3 {
			t.Errorf("expected length 3, got %d", s.Len())
		}

		if !arrow.TypeEqual(s.DType(), arrow.BinaryTypes.String) {
			t.Errorf("expected type String, got %s", s.DType())
		}
	})

	t.Run("create with empty array", func(t *testing.T) {
		// Create a builder for int32 values
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Create an empty array
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("empty", arr)
		defer s.Release()

		// Check series properties
		if s.Len() != 0 {
			t.Errorf("expected length 0, got %d", s.Len())
		}

		if s.NullCount() != 0 {
			t.Errorf("expected null count 0, got %d", s.NullCount())
		}
	})
}

func TestSeries_Accessors(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	t.Run("test Len and NullCount", func(t *testing.T) {
		// Create a builder for float64 values
		builder := array.NewFloat64Builder(mem)
		defer builder.Release()

		// Append values with nulls
		values := []float64{1.1, 2.2, 3.3, 4.4, 5.5}
		valids := []bool{true, false, true, false, true} // 2 nulls
		builder.AppendValues(values, valids)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)
		defer s.Release()

		// Check length and null count
		if s.Len() != 5 {
			t.Errorf("expected length 5, got %d", s.Len())
		}

		if s.NullCount() != 2 {
			t.Errorf("expected null count 2, got %d", s.NullCount())
		}
	})

	t.Run("test IsNull and IsValid", func(t *testing.T) {
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
		s := NewSeries("test", arr)
		defer s.Release()

		// Check IsNull and IsValid
		expectedNulls := []bool{false, true, false, true, false}
		expectedValids := []bool{true, false, true, false, true}

		for i := 0; i < s.Len(); i++ {
			if s.IsNull(i) != expectedNulls[i] {
				t.Errorf("at index %d: expected IsNull=%v, got %v", i, expectedNulls[i], s.IsNull(i))
			}

			if s.IsValid(i) != expectedValids[i] {
				t.Errorf("at index %d: expected IsValid=%v, got %v", i, expectedValids[i], s.IsValid(i))
			}
		}
	})

	t.Run("test DType", func(t *testing.T) {
		testCases := []struct {
			name     string
			values   interface{}
			expected arrow.DataType
			builder  func(mem memory.Allocator) array.Builder
		}{
			{
				name:     "int32",
				values:   []int32{1, 2, 3},
				expected: arrow.PrimitiveTypes.Int32,
				builder:  func(mem memory.Allocator) array.Builder { return array.NewInt32Builder(mem) },
			},
			{
				name:     "float64",
				values:   []float64{1.1, 2.2, 3.3},
				expected: arrow.PrimitiveTypes.Float64,
				builder:  func(mem memory.Allocator) array.Builder { return array.NewFloat64Builder(mem) },
			},
			{
				name:     "string",
				values:   []string{"a", "b", "c"},
				expected: arrow.BinaryTypes.String,
				builder:  func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
			},
			{
				name:     "boolean",
				values:   []bool{true, false, true},
				expected: arrow.FixedWidthTypes.Boolean,
				builder:  func(mem memory.Allocator) array.Builder { return array.NewBooleanBuilder(mem) },
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				builder := tc.builder(mem)
				defer builder.Release()

				// Append values based on type
				switch b := builder.(type) {
				case *array.Int32Builder:
					b.AppendValues(tc.values.([]int32), nil)
				case *array.Float64Builder:
					b.AppendValues(tc.values.([]float64), nil)
				case *array.StringBuilder:
					b.AppendValues(tc.values.([]string), nil)
				case *array.BooleanBuilder:
					b.AppendValues(tc.values.([]bool), nil)
				}

				arr := builder.NewArray()
				defer arr.Release()

				s := NewSeries(tc.name, arr)
				defer s.Release()

				if !arrow.TypeEqual(s.DType(), tc.expected) {
					t.Errorf("expected type %s, got %s", tc.expected, s.DType())
				}
			})
		}
	})

	t.Run("test Name", func(t *testing.T) {
		// Create a simple series
		builder := array.NewInt32Builder(mem)
		defer builder.Release()
		builder.AppendValues([]int32{1, 2, 3}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Test with different names
		testNames := []string{"", "test", "column_1", "very_long_name_for_testing_purposes"}

		for _, name := range testNames {
			s := NewSeries(name, arr)
			defer s.Release()

			if s.Name() != name {
				t.Errorf("expected name '%s', got '%s'", name, s.Name())
			}
		}
	})
}

func TestSeries_Release(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	// Create a builder for int32 values
	builder := array.NewInt32Builder(mem)
	defer builder.Release()

	// Append values
	builder.AppendValues([]int32{1, 2, 3}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	// Create a series
	s := NewSeries("test", arr)

	// Release the series
	s.Release()

	// Check that the array field is nil
	if s.array != nil {
		t.Errorf("expected array to be nil after Release, but it's not")
	}

	// Calling Release again should not panic
	s.Release()
}

func TestSeries_String(t *testing.T) {
	// Setup memory allocator
	mem := memory.NewGoAllocator()

	t.Run("normal series", func(t *testing.T) {
		// Create a builder for int32 values
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int32{1, 2, 3}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test_series", arr)
		defer s.Release()

		// Get string representation
		str := s.String()

		// Check that it contains the series name
		if !strings.Contains(str, "Series: test_series") {
			t.Errorf("expected string to contain 'Series: test_series', got: %s", str)
		}

		// Check that it contains the values
		for _, val := range []string{"1", "2", "3"} {
			if !strings.Contains(str, val) {
				t.Errorf("expected string to contain value '%s', got: %s", val, str)
			}
		}
	})

	t.Run("released series", func(t *testing.T) {
		// Create a builder for int32 values
		builder := array.NewInt32Builder(mem)
		defer builder.Release()

		// Append values
		builder.AppendValues([]int32{1, 2, 3}, nil)
		arr := builder.NewArray()
		defer arr.Release()

		// Create a series
		s := NewSeries("test", arr)

		// Release the series
		s.Release()

		// Get string representation
		str := s.String()

		// Check that it's empty
		if str != "" {
			t.Errorf("expected empty string for released series, got: %s", str)
		}
	})
}
