package series

import (
	"context"
	"fmt"
	array2 "github.com/SHIMA0111/gleam/internal/compute/array"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

func (s *Series) Max() (*Series, error) {
	if s.Len() == 0 {
		return nil, fmt.Errorf("cannot find max value of empty Series")
	}

	ctx := context.Background()

	droppedArray, err := array2.DropNullArray(ctx, s.array)
	if err != nil {
		return nil, err
	}

	var scl scalar.Scalar
	switch s.DType().ID() {
	case arrow.INT8:
		val := maxArray[int8](droppedArray.(*array.Int8))
		scl = scalar.NewInt8Scalar(val)
	case arrow.INT16:
		val := maxArray[int16](droppedArray.(*array.Int16))
		scl = scalar.NewInt16Scalar(val)
	case arrow.INT32:
		val := maxArray[int32](droppedArray.(*array.Int32))
		scl = scalar.NewInt32Scalar(val)
	case arrow.INT64:
		val := maxArray[int64](droppedArray.(*array.Int64))
		scl = scalar.NewInt64Scalar(val)
	case arrow.UINT8:
		val := maxArray[uint8](droppedArray.(*array.Uint8))
		scl = scalar.NewUint8Scalar(val)
	case arrow.UINT16:
		val := maxArray[uint16](droppedArray.(*array.Uint16))
		scl = scalar.NewUint16Scalar(val)
	case arrow.UINT32:
		val := maxArray[uint32](droppedArray.(*array.Uint32))
		scl = scalar.NewUint32Scalar(val)
	case arrow.UINT64:
		val := maxArray[uint64](droppedArray.(*array.Uint64))
		scl = scalar.NewUint64Scalar(val)
	case arrow.FLOAT32:
		val := maxArray[float32](droppedArray.(*array.Float32))
		scl = scalar.NewFloat32Scalar(val)
	case arrow.FLOAT64:
		val := maxArray[float64](droppedArray.(*array.Float64))
		scl = scalar.NewFloat64Scalar(val)
	default:
		return nil, fmt.Errorf("unsupported data type: %s", s.DType())
	}

	newArray, err := scalar.MakeArrayFromScalar(scl, 1, s.mem)
	if err != nil {
		return nil, err
	}
	defer newArray.Release()

	return NewSeries(s.name, newArray), nil
}

func maxArray[T Numeric](arr NumericArray[T]) T {
	maxValue := arr.Value(0)
	for i := 1; i < arr.Len(); i++ {
		if arr.Value(i) > maxValue {
			maxValue = arr.Value(i)
		}
	}

	return maxValue
}
