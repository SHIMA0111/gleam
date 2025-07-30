package array

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/scalar"

	"github.com/SHIMA0111/gleam/gleam/utils"
)

func Comparison(ctx context.Context, arr arrow.Array, cond utils.CompareOperand, value scalar.Scalar) (arrow.Array, error) {
	dataDatum := compute.NewDatum(arr)
	defer dataDatum.Release()
	scalarDatum := compute.NewDatum(value)
	defer scalarDatum.Release()

	conditionDatum, err := compute.CallFunction(ctx, cond.String(), nil, dataDatum, scalarDatum)
	if err != nil {
		return nil, err
	}
	defer conditionDatum.Release()

	conditionArray, ok := conditionDatum.(*compute.ArrayDatum)
	if !ok {
		return nil, fmt.Errorf("comparison did not return an array datum")
	}

	return conditionArray.MakeArray(), nil
}
