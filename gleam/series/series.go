package series

import (
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Series represents a named collection of data stored as an Arrow array.
// It supports various primitive data types through the array field.
type Series struct {
	array    arrow.Array
	name     string
	datatype arrow.DataType
	mem      memory.Allocator
}

// NewSeries creates a new Series with the specified name from the given Arrow array. The array's reference count is retained.
func NewSeries(name string, array arrow.Array) *Series {
	return NewSeriesWithAllocator(name, array, memory.DefaultAllocator)
}

func NewSeriesWithAllocator(name string, array arrow.Array, mem memory.Allocator) *Series {
	array.Retain()

	return &Series{
		array:    array,
		name:     name,
		datatype: array.DataType(),
		mem:      mem,
	}
}

// Release releases the memory associated with the Series' underlying Arrow array, making it unavailable for further use.
func (s *Series) Release() {
	if s.array == nil {
		return
	}
	s.array.Release()
	s.array = nil
}

// Len returns the number of elements in the Series.
func (s *Series) Len() int {
	return s.array.Len()
}

// IsNull checks if the element at the given index i in the Series is null. Returns true if null, otherwise false.
func (s *Series) IsNull(i int) bool {
	return s.array.IsNull(i)
}

// IsValid checks if the element at the given index i in the Series is valid (not null). Returns true if valid, false otherwise.
func (s *Series) IsValid(i int) bool {
	return s.array.IsValid(i)
}

// NullCount returns the number of null (invalid) elements in the Series. It delegates to the underlying Arrow array NullN method.
func (s *Series) NullCount() int {
	return s.array.NullN()
}

// DType returns the data type of the Series.
func (s *Series) DType() arrow.DataType {
	return s.array.DataType()
}

// Name returns the name of the Series. It provides a way to identify the Series by a user-defined string.
func (s *Series) Name() string {
	return s.name
}

// String returns a string representation of the Series, including its name and data content.
func (s *Series) String() string {
	if s.array == nil {
		return ""
	}

	return fmt.Sprintf(
		"Series: %s Type: %s\n%s", s.Name(), s.datatype, s.array,
	)
}

func (s *Series) underlyingArray() arrow.Array {
	return s.array
}
