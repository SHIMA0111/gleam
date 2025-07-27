package utils

import "github.com/apache/arrow-go/v18/arrow"

func GetBitLenFromDataType(dataType arrow.DataType) int8 {
	switch dataType.ID() {
	case arrow.INT8:
		return 8
	case arrow.INT16:
		return 16
	case arrow.INT32:
		return 32
	case arrow.INT64:
		return 64
	case arrow.UINT8:
		return 8
	case arrow.UINT16:
		return 16
	case arrow.UINT32:
		return 32
	case arrow.UINT64:
		return 64
	case arrow.FLOAT32:
		return 32
	case arrow.FLOAT64:
		return 64
	default:
		panic("unsupported data type: " + dataType.String())
	}
}
