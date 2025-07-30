package array

import (
	"context"
	"fmt"
	"github.com/SHIMA0111/gleam/internal/utils"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/math"
)

const SumThreshold = 150_000

func Sum(ctx context.Context, arr arrow.Array) (float64, error) {
	switch arr.DataType().ID() {
	case arrow.INT8:
		if arr.Len() < SumThreshold {
			res := utils.SumInt8Array(arr.(*array.Int8))
			return utils.CheckOverflowAndConvertToFloat64(res)
		} else {
			return utils.CastSumInt(ctx, arr)
		}
	case arrow.INT16:
		if arr.Len() < SumThreshold {
			res := utils.SumInt16Array(arr.(*array.Int16))
			return utils.CheckOverflowAndConvertToFloat64(res)
		} else {
			return utils.CastSumInt(ctx, arr)
		}
	case arrow.INT32:
		if arr.Len() < SumThreshold {
			res := utils.SumInt32Array(arr.(*array.Int32))
			return utils.CheckOverflowAndConvertToFloat64(res)
		} else {
			return utils.CastSumInt(ctx, arr)
		}
	case arrow.INT64:
		i64Array, ok := arr.(*array.Int64)
		if !ok {
			return 0, fmt.Errorf("failed to cast the array to Int64 from %s", arr.DataType())
		}

		return utils.CheckOverflowAndConvertToFloat64[int64](math.Int64.Sum(i64Array))
	case arrow.UINT8:
		if arr.Len() < SumThreshold {
			res := utils.SumUInt8Array(arr.(*array.Uint8))
			return utils.CheckOverflowAndConvertToFloat64(res)
		} else {
			return utils.CastSumUInt(ctx, arr)
		}
	case arrow.UINT16:
		if arr.Len() < SumThreshold {
			res := utils.SumUInt16Array(arr.(*array.Uint16))
			return utils.CheckOverflowAndConvertToFloat64(res)
		} else {
			return utils.CastSumUInt(ctx, arr)
		}
	case arrow.UINT32:
		if arr.Len() < SumThreshold {
			res := utils.SumUInt32Array(arr.(*array.Uint32))
			return utils.CheckOverflowAndConvertToFloat64(res)
		} else {
			return utils.CastSumUInt(ctx, arr)
		}
	case arrow.UINT64:
		u64Array, ok := arr.(*array.Uint64)
		if !ok {
			return 0, fmt.Errorf("failed to cast the array to Uint64 from %s", arr.DataType())
		}

		return utils.CheckOverflowAndConvertToFloat64(math.Uint64.Sum(u64Array))
	case arrow.FLOAT32:
		if arr.Len() < SumThreshold {
			res := utils.SumFloat32Array(arr.(*array.Float32))
			return utils.CheckOverflowAndConvertToFloat64(res)
		} else {
			return utils.CastSumFloat(ctx, arr)
		}
	case arrow.FLOAT64:
		f64Array, ok := arr.(*array.Float64)
		if !ok {
			return 0, fmt.Errorf("failed to cast the array to Float64 from %s", arr.DataType())
		}

		return utils.CheckOverflowAndConvertToFloat64(math.Float64.Sum(f64Array))
	default:
		return 0, fmt.Errorf("sum is not supported for %s", arr.DataType())
	}
}
