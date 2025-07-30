package series

import (
	"context"
	"fmt"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
)

// DataType represents a set of integer constants used to define various primitive and complex data types.
type DataType int

const (
	Int8 DataType = iota
	Int16
	Int32
	Int64
	UInt8
	UInt16
	UInt32
	UInt64
	Float32
	Float64
	String
	Boolean
	Unsupported
)

func (dt DataType) dataType() arrow.DataType {
	switch dt {
	case Int8:
		return arrow.PrimitiveTypes.Int8
	case Int16:
		return arrow.PrimitiveTypes.Int16
	case Int32:
		return arrow.PrimitiveTypes.Int32
	case Int64:
		return arrow.PrimitiveTypes.Int64
	case UInt8:
		return arrow.PrimitiveTypes.Uint8
	case UInt16:
		return arrow.PrimitiveTypes.Uint16
	case UInt32:
		return arrow.PrimitiveTypes.Uint32
	case UInt64:
		return arrow.PrimitiveTypes.Uint64
	case Float32:
		return arrow.PrimitiveTypes.Float32
	case Float64:
		return arrow.PrimitiveTypes.Float64
	case String:
		return arrow.BinaryTypes.String
	case Boolean:
		return arrow.FixedWidthTypes.Boolean
	default:
		panic("unsupported data type")
	}
}

// Cast changes the data type of Series to the specified dtype if a valid conversion exists, returning a new Series.
func (s *Series) Cast(dtype DataType) (*Series, error) {
	if dtype == Unsupported {
		return nil, fmt.Errorf("cannot convert unsupported data type")
	}

	if s.DType() == dtype.dataType() {
		return s, nil
	}

	ctx := context.Background()

	castedArray, err := compute.CastToType(ctx, s.array, dtype.dataType())
	if err != nil {
		return nil, err
	}
	defer castedArray.Release()

	return NewSeries(s.name, castedArray), nil
}
