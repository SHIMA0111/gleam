package array

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
)

// DropNullArray removes null elements from an Arrow array and returns a new array with only non-null elements.
func DropNullArray(ctx context.Context, arrayData arrow.Array) (arrow.Array, error) {
	if arrayData.NullN() == 0 {
		arrayData.Retain()
		return arrayData, nil
	}

	// Convert Array to Datum
	arrayDatum := compute.NewDatum(arrayData)

	// Get the validity boolean slice from the arrayData
	validityFilterDatum, err := compute.CallFunction(ctx, "is_not_null", nil, arrayDatum)
	if err != nil {
		return nil, fmt.Errorf("failed to call function is_not_null: %w", err)
	}
	defer validityFilterDatum.Release()

	// TODO: Consider if the default filter option is good or not for this
	filterOpts := compute.DefaultFilterOptions()

	// Apply the validity filter to the original array
	validValuesArray, err := compute.Filter(ctx, arrayDatum, validityFilterDatum, *filterOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to filter array: %w", err)
	}
	defer validValuesArray.Release()

	// Convert to the filtered Datum to Array and return it
	validArray, ok := validValuesArray.(*compute.ArrayDatum)
	if !ok {
		return nil, fmt.Errorf("filter did not return an array datum")
	}

	return validArray.MakeArray(), nil
}
