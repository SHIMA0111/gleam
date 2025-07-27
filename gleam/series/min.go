package series

import (
	"context"
	"fmt"
	"github.com/SHIMA0111/gleam/internal/compute"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func (s *Series) Min() (*Series, error) {
	if s.Len() == 0 {
		return nil, fmt.Errorf("cannot find min value of empty Series")
	}

	mem := memory.DefaultAllocator
	ctx := context.Background()

	droppedArray, err := compute.DropNullArray(ctx, s.array)
	if err != nil {
		return nil, err
	}

	switch s.DType().ID() {
	case arrow.INT8:
		val := minArray[int8](droppedArray.(*array.Int8))
		b := array.NewInt8Builder(mem)
		defer b.Release()

		b.Append(val)

		newArray := b.NewArray()
		defer newArray.Release()

		return NewSeries(s.name, newArray), nil
	case arrow.INT16:
		val := minArray[int16](droppedArray.(*array.Int16))
		b := array.NewInt16Builder(mem)
		defer b.Release()

		b.Append(val)

		newArray := b.NewArray()
		defer newArray.Release()

		return NewSeries(s.name, newArray), nil
	case arrow.INT32:
		val := minArray[int32](droppedArray.(*array.Int32))
		b := array.NewInt32Builder(mem)
		defer b.Release()

		b.Append(val)

		newArray := b.NewArray()
		defer newArray.Release()

		return NewSeries(s.name, newArray), nil
	case arrow.INT64:
		val := minArray[int64](droppedArray.(*array.Int64))
		b := array.NewInt64Builder(mem)
		defer b.Release()

		b.Append(val)

		newArray := b.NewArray()
		defer newArray.Release()

		return NewSeries(s.name, newArray), nil
	case arrow.UINT8:
		val := minArray[uint8](droppedArray.(*array.Uint8))
		b := array.NewUint8Builder(mem)
		defer b.Release()

		b.Append(val)

		newArray := b.NewArray()
		defer newArray.Release()

		return NewSeries(s.name, newArray), nil
	case arrow.UINT16:
		val := minArray[uint16](droppedArray.(*array.Uint16))
		b := array.NewUint16Builder(mem)
		defer b.Release()

		b.Append(val)

		newArray := b.NewArray()
		defer newArray.Release()

		return NewSeries(s.name, newArray), nil
	case arrow.UINT32:
		val := minArray[uint32](droppedArray.(*array.Uint32))
		b := array.NewUint32Builder(mem)
		defer b.Release()

		b.Append(val)

		newArray := b.NewArray()
		defer newArray.Release()

		return NewSeries(s.name, newArray), nil
	case arrow.UINT64:
		val := minArray[uint64](droppedArray.(*array.Uint64))
		b := array.NewUint64Builder(mem)
		defer b.Release()

		b.Append(val)

		newArray := b.NewArray()
		defer newArray.Release()

		return NewSeries(s.name, newArray), nil
	case arrow.FLOAT32:
		val := minArray[float32](droppedArray.(*array.Float32))
		b := array.NewFloat32Builder(mem)
		defer b.Release()

		b.Append(val)

		newArray := b.NewArray()
		defer newArray.Release()

		return NewSeries(s.name, newArray), nil
	case arrow.FLOAT64:
		val := minArray[float64](droppedArray.(*array.Float64))
		b := array.NewFloat64Builder(mem)
		defer b.Release()

		b.Append(val)

		newArray := b.NewArray()
		defer newArray.Release()

		return NewSeries(s.name, newArray), nil
	default:
		return nil, fmt.Errorf("unsupported data type: %s", s.DType())
	}
}

func minArray[T Numeric](arr NumericArray[T]) T {
	minValue := arr.Value(0)
	for i := 1; i < arr.Len(); i++ {
		if arr.Value(i) < minValue {
			minValue = arr.Value(i)
		}
	}

	return minValue
}
