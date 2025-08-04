package array

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
)

// Filter filters elements of the input array based on the corresponding boolean values in the filter array.
// Returns a new array with elements where the filter value is true.
// The input filter array must be of a boolean type and must not contain null values.
// Returns an error if the lengths of the input array and filter array differ.
// Also returns an error if the filter array is not of a boolean type or contains null values.
func Filter(ctx context.Context, arr arrow.Array, filterArr arrow.Array, filterOpts compute.FilterOptions) (arrow.Array, error) {
	if arr.Len() == 0 {
		return arr, nil
	}

	if filterArr.Len() == 0 {
		return arr, nil
	}

	if filterArr.DataType().ID() != arrow.BOOL {
		return nil, fmt.Errorf("filter array must be boolean array and cannot have null values: %s (null: %d)", filterArr.DataType().String(), filterArr.NullN())
	}

	if arr.Len() != filterArr.Len() {
		return nil, fmt.Errorf("array length is not equal to filter array length: %d != %d", arr.Len(), filterArr.Len())
	}

	arrDatum := compute.NewDatum(arr)
	defer arrDatum.Release()

	filterDatum := compute.NewDatum(filterArr)
	defer filterDatum.Release()

	filteredDatum, err := compute.Filter(ctx, arrDatum, filterDatum, filterOpts)
	if err != nil {
		return nil, err
	}
	defer filteredDatum.Release()

	filteredArray, ok := filteredDatum.(*compute.ArrayDatum)
	if !ok {
		return nil, fmt.Errorf("filter did not return an array datum")
	}

	return filteredArray.MakeArray(), nil
}
