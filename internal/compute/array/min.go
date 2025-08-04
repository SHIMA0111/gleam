package array

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"

	"github.com/SHIMA0111/gleam/internal/utils"
)

func MinArray(ctx context.Context, arr arrow.Array, mem memory.Allocator) (arrow.Array, error) {
	sumScl, err := Min(ctx, arr)
	if err != nil {
		return nil, err
	}

	newArray, err := scalar.MakeArrayFromScalar(sumScl, 1, mem)
	if err != nil {
		return nil, err
	}

	return newArray, nil
}

func Min(ctx context.Context, arr arrow.Array) (scalar.Scalar, error) {
	if arr.Len() == 0 {
		return nil, fmt.Errorf("cannot find min value of empty Series")
	}

	droppedArray, err := DropNullArray(ctx, arr)
	if err != nil {
		return nil, err
	}

	var scl scalar.Scalar
	switch arr.DataType().ID() {
	case arrow.INT8:
		val := minArray[int8](droppedArray.(*array.Int8))
		scl = scalar.NewInt8Scalar(val)
	case arrow.INT16:
		val := minArray[int16](droppedArray.(*array.Int16))
		scl = scalar.NewInt16Scalar(val)
	case arrow.INT32:
		val := minArray[int32](droppedArray.(*array.Int32))
		scl = scalar.NewInt32Scalar(val)
	case arrow.INT64:
		val := minArray[int64](droppedArray.(*array.Int64))
		scl = scalar.NewInt64Scalar(val)
	case arrow.UINT8:
		val := minArray[uint8](droppedArray.(*array.Uint8))
		scl = scalar.NewUint8Scalar(val)
	case arrow.UINT16:
		val := minArray[uint16](droppedArray.(*array.Uint16))
		scl = scalar.NewUint16Scalar(val)
	case arrow.UINT32:
		val := minArray[uint32](droppedArray.(*array.Uint32))
		scl = scalar.NewUint32Scalar(val)
	case arrow.UINT64:
		val := minArray[uint64](droppedArray.(*array.Uint64))
		scl = scalar.NewUint64Scalar(val)
	case arrow.FLOAT32:
		val := minArray[float32](droppedArray.(*array.Float32))
		scl = scalar.NewFloat32Scalar(val)
	case arrow.FLOAT64:
		val := minArray[float64](droppedArray.(*array.Float64))
		scl = scalar.NewFloat64Scalar(val)
	default:
		return nil, fmt.Errorf("unsupported data type: %s", arr.DataType())
	}

	return scl, nil
}

func minArray[T utils.Numeric](arr utils.NumericArray[T]) T {
	minValue := arr.Value(0)
	for i := 1; i < arr.Len(); i++ {
		if arr.Value(i) < minValue {
			minValue = arr.Value(i)
		}
	}

	return minValue
}
