package series

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// FilterOperand defines a string type representing comparison or filtering operations for data processing.
type FilterOperand string

const (
	Equal        FilterOperand = "equal"
	NotEqual     FilterOperand = "not_equal"
	Greater      FilterOperand = "greater"
	GreaterEqual FilterOperand = "greater_equal"
	Less         FilterOperand = "less"
	LessEqual    FilterOperand = "less_equal"
)

// Where filters the Series based on the given FilterOperand and value, returning a new Series with matched elements.
func (s *Series) Where(cond FilterOperand, val interface{}) (*Series, error) {
	// Create context
	ctx := context.Background()

	filterArray, err := s.comparison(ctx, cond, val)
	if err != nil {
		return nil, err
	}
	defer filterArray.Release()

	// FilterOptions: currently using the default options
	filterOpts := compute.DefaultFilterOptions()

	// Apply the filter using the bitmap from the condition compare function
	resultArray, err := compute.FilterArray(ctx, s.array, filterArray, *filterOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to filter array: %w", err)
	}
	defer resultArray.Release()

	// Create Series by the filtered array with the input series name
	resultSeries := NewSeries(s.name, resultArray)

	return resultSeries, nil
}

// Comparison performs element-wise comparison on the Series using the specified condition and value, returning a bitmap array.
// The method takes a FilterOperand and value as parameters and returns an arrow.Array or an error if the operation fails.
func (s *Series) Comparison(cond FilterOperand, val interface{}) (arrow.Array, error) {
	ctx := context.Background()

	return s.comparison(ctx, cond, val)
}

// Performs element-wise comparison using a specified condition and value, returning a filtered Arrow array.
// It validates the provided value and ensures type compatibility with the Series, generating a mask to apply the filter.
// Returns an Arrow array or an error if the operation or type validation fails.
func (s *Series) comparison(ctx context.Context, cond FilterOperand, val interface{}) (arrow.Array, error) {
	// Make the input value to proper scalar value
	scl, err := makeScalar(val)
	if err != nil {
		return nil, fmt.Errorf("failed to create scalar: %w", err)
	}

	// If the scalar value is releasable, defer the release
	if release, ok := scl.(scalar.Releasable); ok {
		defer release.Release()
	}

	if !arrow.TypeEqual(s.DType(), scl.DataType()) {
		return nil, fmt.Errorf("type mismatch: series type is %s, but value type is %s", s.DType(), scl.DataType())
	}

	// Convert the scalar to Datum for using it on compute methods
	scalarDatum := compute.NewDatum(val)
	defer scalarDatum.Release()

	// Convert the arrow array in the series to Datum for using it on compute methods
	arrayDatum := compute.NewDatum(s.array)
	defer arrayDatum.Release()

	// Generate mask for the filterable values cond has a string mapped to arrow compute function
	filterDatum, err := compute.CallFunction(ctx, string(cond), nil, arrayDatum, scalarDatum)
	if err != nil {
		return nil, fmt.Errorf("failed to call function %s: %w", cond, err)
	}
	defer filterDatum.Release()

	filterArray, ok := filterDatum.(*compute.ArrayDatum)
	if !ok {
		return nil, fmt.Errorf("filter did not return an array datum")
	}

	return filterArray.MakeArray(), nil
}

// makeScalar translates the input-compared value to the arrow scalar
func makeScalar(val interface{}) (scalar.Scalar, error) {
	switch v := val.(type) {
	// Fixed size integers
	case int8:
		return scalar.NewInt8Scalar(v), nil
	case int16:
		return scalar.NewInt16Scalar(v), nil
	case int32:
		return scalar.NewInt32Scalar(v), nil
	case int64:
		return scalar.NewInt64Scalar(v), nil
	case uint8:
		return scalar.NewUint8Scalar(v), nil
	case uint16:
		return scalar.NewUint16Scalar(v), nil
	case uint32:
		return scalar.NewUint32Scalar(v), nil
	case uint64:
		return scalar.NewUint64Scalar(v), nil
	// Platform specific data type
	case int:
		if strconv.IntSize == 32 {
			return scalar.NewInt32Scalar(int32(v)), nil
		}
		return scalar.NewInt64Scalar(int64(v)), nil
	case uint:
		if strconv.IntSize == 32 {
			return scalar.NewUint32Scalar(uint32(v)), nil
		}
		return scalar.NewUint64Scalar(uint64(v)), nil
	// Floating number
	case float32:
		return scalar.NewFloat32Scalar(v), nil
	case float64:
		return scalar.NewFloat64Scalar(v), nil
	// string type
	case string:
		return scalar.NewStringScalar(v), nil
	// boolean type
	case bool:
		return scalar.NewBooleanScalar(v), nil
	// Unsupported type branch
	default:
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
}
