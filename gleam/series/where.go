package series

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/scalar"

	"github.com/SHIMA0111/gleam/gleam/utils"
	"github.com/SHIMA0111/gleam/internal/compute/array"
)

type ComparisonArray arrow.Array

// Where filters the Series based on the given CompareOperand and value, returning a new Series with matched elements.
func (s *Series) Where(cond utils.CompareOperand, val interface{}) (*Series, error) {
	// Create context
	ctx := context.Background()

	filterArray, err := s.Comparison(cond, val)
	if err != nil {
		return nil, err
	}
	defer filterArray.Release()

	// FilterOptions: currently using the default options
	filterOpts := compute.DefaultFilterOptions()

	resultArray, err := array.Filter(ctx, s.array, filterArray, *filterOpts)
	if err != nil {
		return nil, err
	}
	defer resultArray.Release()

	// Create Series by the filtered array with the input series name
	resultSeries := NewSeries(s.name, resultArray)

	return resultSeries, nil
}

// Comparison performs element-wise comparison on the Series using the specified condition and value, returning a bitmap array.
// The method takes a CompareOperand and value as parameters and returns an arrow.Array or an error if the operation fails.
func (s *Series) Comparison(cond utils.CompareOperand, val interface{}) (ComparisonArray, error) {
	ctx := context.Background()
	scl, err := makeScalar(val)
	if err != nil {
		return nil, err
	}

	return array.Comparison(ctx, s.array, cond, scl)
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
	// Platform-specific data type
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
