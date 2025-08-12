package dataframe

import (
	"fmt"
	"testing"

	"github.com/SHIMA0111/gleam/gleam/dataframe"
	"github.com/SHIMA0111/gleam/gleam/series"
	"github.com/SHIMA0111/gleam/gleam/utils"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	gotaDataframe "github.com/go-gota/gota/dataframe"
	gotaSeries "github.com/go-gota/gota/series"
)

// DataFrame where operation benchmarks
// This file contains benchmarks comparing Gleam DataFrame where operations with Gota DataFrame filtering
// Benchmarks are organized by data type and dataset size (Small: 10k, Medium: 1M, Large: 100M rows)

// Dataset size constants
const smallDatasetSize = 10000
const mediumDatasetSize = 1000000
const largeDatasetSize = 100000000

// Data generation functions for Gleam DataFrames

// generateInt8DataFrame creates a DataFrame with 3 int8 columns and n rows for benchmarking
func generateInt8DataFrame(n int) (*dataframe.DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Create 3 columns of int8 data
	columns := make([]arrow.Array, 3)
	columnNames := []string{"col1", "col2", "col3"}

	// Ensure cleanup of columns if error occurs
	defer func() {
		if columns != nil {
			for _, col := range columns {
				if col != nil {
					col.Release()
				}
			}
		}
	}()

	for colIdx := 0; colIdx < 3; colIdx++ {
		builder := array.NewInt8Builder(mem)
		if builder == nil {
			return nil, fmt.Errorf("failed to create int8 builder for column %d", colIdx)
		}

		builder.Reserve(n)

		// Add values with different patterns for each column
		for i := 0; i < n; i++ {
			builder.Append(int8((i + colIdx*10) % 100))
		}

		columns[colIdx] = builder.NewArray()
		builder.Release()
	}

	df, err := dataframe.NewDataFrame(columns, columnNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	// Clear columns slice to prevent cleanup in defer
	columns = nil
	return df, nil
}

// generateInt16DataFrame creates a DataFrame with 3 int16 columns and n rows for benchmarking
func generateInt16DataFrame(n int) (*dataframe.DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Create 3 columns of int16 data
	columns := make([]arrow.Array, 3)
	columnNames := []string{"col1", "col2", "col3"}

	// Ensure cleanup of columns if error occurs
	defer func() {
		if columns != nil {
			for _, col := range columns {
				if col != nil {
					col.Release()
				}
			}
		}
	}()

	for colIdx := 0; colIdx < 3; colIdx++ {
		builder := array.NewInt16Builder(mem)
		if builder == nil {
			return nil, fmt.Errorf("failed to create int16 builder for column %d", colIdx)
		}

		builder.Reserve(n)

		// Add values with different patterns for each column
		for i := 0; i < n; i++ {
			builder.Append(int16((i + colIdx*10) % 100))
		}

		columns[colIdx] = builder.NewArray()
		builder.Release()
	}

	df, err := dataframe.NewDataFrame(columns, columnNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	// Clear columns slice to prevent cleanup in defer
	columns = nil
	return df, nil
}

// generateInt32DataFrame creates a DataFrame with 3 int32 columns and n rows for benchmarking
func generateInt32DataFrame(n int) (*dataframe.DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Create 3 columns of int32 data
	columns := make([]arrow.Array, 3)
	columnNames := []string{"col1", "col2", "col3"}

	// Ensure cleanup of columns if error occurs
	defer func() {
		if columns != nil {
			for _, col := range columns {
				if col != nil {
					col.Release()
				}
			}
		}
	}()

	for colIdx := 0; colIdx < 3; colIdx++ {
		builder := array.NewInt32Builder(mem)
		if builder == nil {
			return nil, fmt.Errorf("failed to create int32 builder for column %d", colIdx)
		}

		builder.Reserve(n)

		// Add values with different patterns for each column
		for i := 0; i < n; i++ {
			builder.Append(int32((i + colIdx*10) % 100))
		}

		columns[colIdx] = builder.NewArray()
		builder.Release()
	}

	df, err := dataframe.NewDataFrame(columns, columnNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	// Clear columns slice to prevent cleanup in defer
	columns = nil
	return df, nil
}

// generateInt64DataFrame creates a DataFrame with 3 int64 columns and n rows for benchmarking
func generateInt64DataFrame(n int) (*dataframe.DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Create 3 columns of int64 data
	columns := make([]arrow.Array, 3)
	columnNames := []string{"col1", "col2", "col3"}

	// Ensure cleanup of columns if error occurs
	defer func() {
		if columns != nil {
			for _, col := range columns {
				if col != nil {
					col.Release()
				}
			}
		}
	}()

	for colIdx := 0; colIdx < 3; colIdx++ {
		builder := array.NewInt64Builder(mem)
		if builder == nil {
			return nil, fmt.Errorf("failed to create int64 builder for column %d", colIdx)
		}

		builder.Reserve(n)

		// Add values with different patterns for each column
		for i := 0; i < n; i++ {
			builder.Append(int64((i + colIdx*10) % 100))
		}

		columns[colIdx] = builder.NewArray()
		builder.Release()
	}

	df, err := dataframe.NewDataFrame(columns, columnNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	// Clear columns slice to prevent cleanup in defer
	columns = nil
	return df, nil
}

// generateUint8DataFrame creates a DataFrame with 3 uint8 columns and n rows for benchmarking
func generateUint8DataFrame(n int) (*dataframe.DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Create 3 columns of uint8 data
	columns := make([]arrow.Array, 3)
	columnNames := []string{"col1", "col2", "col3"}

	// Ensure cleanup of columns if error occurs
	defer func() {
		if columns != nil {
			for _, col := range columns {
				if col != nil {
					col.Release()
				}
			}
		}
	}()

	for colIdx := 0; colIdx < 3; colIdx++ {
		builder := array.NewUint8Builder(mem)
		if builder == nil {
			return nil, fmt.Errorf("failed to create uint8 builder for column %d", colIdx)
		}

		builder.Reserve(n)

		// Add values with different patterns for each column
		for i := 0; i < n; i++ {
			builder.Append(uint8((i + colIdx*10) % 100))
		}

		columns[colIdx] = builder.NewArray()
		builder.Release()
	}

	df, err := dataframe.NewDataFrame(columns, columnNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	// Clear columns slice to prevent cleanup in defer
	columns = nil
	return df, nil
}

// generateUint16DataFrame creates a DataFrame with 3 uint16 columns and n rows for benchmarking
func generateUint16DataFrame(n int) (*dataframe.DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Create 3 columns of uint16 data
	columns := make([]arrow.Array, 3)
	columnNames := []string{"col1", "col2", "col3"}

	// Ensure cleanup of columns if error occurs
	defer func() {
		if columns != nil {
			for _, col := range columns {
				if col != nil {
					col.Release()
				}
			}
		}
	}()

	for colIdx := 0; colIdx < 3; colIdx++ {
		builder := array.NewUint16Builder(mem)
		if builder == nil {
			return nil, fmt.Errorf("failed to create uint16 builder for column %d", colIdx)
		}

		builder.Reserve(n)

		// Add values with different patterns for each column
		for i := 0; i < n; i++ {
			builder.Append(uint16((i + colIdx*10) % 100))
		}

		columns[colIdx] = builder.NewArray()
		builder.Release()
	}

	df, err := dataframe.NewDataFrame(columns, columnNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	// Clear columns slice to prevent cleanup in defer
	columns = nil
	return df, nil
}

// generateUint32DataFrame creates a DataFrame with 3 uint32 columns and n rows for benchmarking
func generateUint32DataFrame(n int) (*dataframe.DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Create 3 columns of uint32 data
	columns := make([]arrow.Array, 3)
	columnNames := []string{"col1", "col2", "col3"}

	// Ensure cleanup of columns if error occurs
	defer func() {
		if columns != nil {
			for _, col := range columns {
				if col != nil {
					col.Release()
				}
			}
		}
	}()

	for colIdx := 0; colIdx < 3; colIdx++ {
		builder := array.NewUint32Builder(mem)
		if builder == nil {
			return nil, fmt.Errorf("failed to create uint32 builder for column %d", colIdx)
		}

		builder.Reserve(n)

		// Add values with different patterns for each column
		for i := 0; i < n; i++ {
			builder.Append(uint32((i + colIdx*10) % 100))
		}

		columns[colIdx] = builder.NewArray()
		builder.Release()
	}

	df, err := dataframe.NewDataFrame(columns, columnNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	// Clear columns slice to prevent cleanup in defer
	columns = nil
	return df, nil
}

// generateUint64DataFrame creates a DataFrame with 3 uint64 columns and n rows for benchmarking
func generateUint64DataFrame(n int) (*dataframe.DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Create 3 columns of uint64 data
	columns := make([]arrow.Array, 3)
	columnNames := []string{"col1", "col2", "col3"}

	// Ensure cleanup of columns if error occurs
	defer func() {
		if columns != nil {
			for _, col := range columns {
				if col != nil {
					col.Release()
				}
			}
		}
	}()

	for colIdx := 0; colIdx < 3; colIdx++ {
		builder := array.NewUint64Builder(mem)
		if builder == nil {
			return nil, fmt.Errorf("failed to create uint64 builder for column %d", colIdx)
		}

		builder.Reserve(n)

		// Add values with different patterns for each column
		for i := 0; i < n; i++ {
			builder.Append(uint64((i + colIdx*10) % 100))
		}

		columns[colIdx] = builder.NewArray()
		builder.Release()
	}

	df, err := dataframe.NewDataFrame(columns, columnNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	// Clear columns slice to prevent cleanup in defer
	columns = nil
	return df, nil
}

// generateFloat32DataFrame creates a DataFrame with 3 float32 columns and n rows for benchmarking
func generateFloat32DataFrame(n int) (*dataframe.DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Create 3 columns of float32 data
	columns := make([]arrow.Array, 3)
	columnNames := []string{"col1", "col2", "col3"}

	// Ensure cleanup of columns if error occurs
	defer func() {
		if columns != nil {
			for _, col := range columns {
				if col != nil {
					col.Release()
				}
			}
		}
	}()

	for colIdx := 0; colIdx < 3; colIdx++ {
		builder := array.NewFloat32Builder(mem)
		if builder == nil {
			return nil, fmt.Errorf("failed to create float32 builder for column %d", colIdx)
		}

		builder.Reserve(n)

		// Add values with different patterns for each column
		for i := 0; i < n; i++ {
			builder.Append(float32((i + colIdx*10) % 100))
		}

		columns[colIdx] = builder.NewArray()
		builder.Release()
	}

	df, err := dataframe.NewDataFrame(columns, columnNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	// Clear columns slice to prevent cleanup in defer
	columns = nil
	return df, nil
}

// generateFloat64DataFrame creates a DataFrame with 3 float64 columns and n rows for benchmarking
func generateFloat64DataFrame(n int) (*dataframe.DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Create 3 columns of float64 data
	columns := make([]arrow.Array, 3)
	columnNames := []string{"col1", "col2", "col3"}

	// Ensure cleanup of columns if error occurs
	defer func() {
		if columns != nil {
			for _, col := range columns {
				if col != nil {
					col.Release()
				}
			}
		}
	}()

	for colIdx := 0; colIdx < 3; colIdx++ {
		builder := array.NewFloat64Builder(mem)
		if builder == nil {
			return nil, fmt.Errorf("failed to create float64 builder for column %d", colIdx)
		}

		builder.Reserve(n)

		// Add values with different patterns for each column
		for i := 0; i < n; i++ {
			builder.Append(float64((i + colIdx*10) % 100))
		}

		columns[colIdx] = builder.NewArray()
		builder.Release()
	}

	df, err := dataframe.NewDataFrame(columns, columnNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	// Clear columns slice to prevent cleanup in defer
	columns = nil
	return df, nil
}

// Data generation functions for Gota DataFrames

// generateGotaInt8DataFrame creates a Gota DataFrame with 3 int8 columns and n rows for benchmarking
func generateGotaInt8DataFrame(n int) gotaDataframe.DataFrame {
	// Create 3 columns of int8 data
	col1Data := make([]int8, n)
	col2Data := make([]int8, n)
	col3Data := make([]int8, n)

	// Fill the columns with different patterns
	for i := 0; i < n; i++ {
		col1Data[i] = int8((i + 0*10) % 100)
		col2Data[i] = int8((i + 1*10) % 100)
		col3Data[i] = int8((i + 2*10) % 100)
	}

	// Create Gota Series for each column
	col1 := gotaSeries.Ints(col1Data)
	col2 := gotaSeries.Ints(col2Data)
	col3 := gotaSeries.Ints(col3Data)

	// Set column names
	col1.Name = "col1"
	col2.Name = "col2"
	col3.Name = "col3"

	return gotaDataframe.New(col1, col2, col3)
}

// generateGotaInt16DataFrame creates a Gota DataFrame with 3 int16 columns and n rows for benchmarking
func generateGotaInt16DataFrame(n int) gotaDataframe.DataFrame {
	// Create 3 columns of int16 data
	col1Data := make([]int16, n)
	col2Data := make([]int16, n)
	col3Data := make([]int16, n)

	// Fill the columns with different patterns
	for i := 0; i < n; i++ {
		col1Data[i] = int16((i + 0*10) % 100)
		col2Data[i] = int16((i + 1*10) % 100)
		col3Data[i] = int16((i + 2*10) % 100)
	}

	// Create Gota Series for each column
	col1 := gotaSeries.Ints(col1Data)
	col2 := gotaSeries.Ints(col2Data)
	col3 := gotaSeries.Ints(col3Data)

	// Set column names
	col1.Name = "col1"
	col2.Name = "col2"
	col3.Name = "col3"

	return gotaDataframe.New(col1, col2, col3)
}

// generateGotaInt32DataFrame creates a Gota DataFrame with 3 int32 columns and n rows for benchmarking
func generateGotaInt32DataFrame(n int) gotaDataframe.DataFrame {
	// Create 3 columns of int32 data
	col1Data := make([]int32, n)
	col2Data := make([]int32, n)
	col3Data := make([]int32, n)

	// Fill the columns with different patterns
	for i := 0; i < n; i++ {
		col1Data[i] = int32((i + 0*10) % 100)
		col2Data[i] = int32((i + 1*10) % 100)
		col3Data[i] = int32((i + 2*10) % 100)
	}

	// Create Gota Series for each column
	col1 := gotaSeries.Ints(col1Data)
	col2 := gotaSeries.Ints(col2Data)
	col3 := gotaSeries.Ints(col3Data)

	// Set column names
	col1.Name = "col1"
	col2.Name = "col2"
	col3.Name = "col3"

	return gotaDataframe.New(col1, col2, col3)
}

// generateGotaInt64DataFrame creates a Gota DataFrame with 3 int64 columns and n rows for benchmarking
func generateGotaInt64DataFrame(n int) gotaDataframe.DataFrame {
	// Create 3 columns of int64 data
	col1Data := make([]int64, n)
	col2Data := make([]int64, n)
	col3Data := make([]int64, n)

	// Fill the columns with different patterns
	for i := 0; i < n; i++ {
		col1Data[i] = int64((i + 0*10) % 100)
		col2Data[i] = int64((i + 1*10) % 100)
		col3Data[i] = int64((i + 2*10) % 100)
	}

	// Create Gota Series for each column
	col1 := gotaSeries.Ints(col1Data)
	col2 := gotaSeries.Ints(col2Data)
	col3 := gotaSeries.Ints(col3Data)

	// Set column names
	col1.Name = "col1"
	col2.Name = "col2"
	col3.Name = "col3"

	return gotaDataframe.New(col1, col2, col3)
}

// generateGotaUint8DataFrame creates a Gota DataFrame with 3 uint8 columns and n rows for benchmarking
func generateGotaUint8DataFrame(n int) gotaDataframe.DataFrame {
	// Create 3 columns of uint8 data
	col1Data := make([]uint8, n)
	col2Data := make([]uint8, n)
	col3Data := make([]uint8, n)

	// Fill the columns with different patterns
	for i := 0; i < n; i++ {
		col1Data[i] = uint8((i + 0*10) % 100)
		col2Data[i] = uint8((i + 1*10) % 100)
		col3Data[i] = uint8((i + 2*10) % 100)
	}

	// Create Gota Series for each column
	col1 := gotaSeries.Ints(col1Data)
	col2 := gotaSeries.Ints(col2Data)
	col3 := gotaSeries.Ints(col3Data)

	// Set column names
	col1.Name = "col1"
	col2.Name = "col2"
	col3.Name = "col3"

	return gotaDataframe.New(col1, col2, col3)
}

// generateGotaUint16DataFrame creates a Gota DataFrame with 3 uint16 columns and n rows for benchmarking
func generateGotaUint16DataFrame(n int) gotaDataframe.DataFrame {
	// Create 3 columns of uint16 data
	col1Data := make([]uint16, n)
	col2Data := make([]uint16, n)
	col3Data := make([]uint16, n)

	// Fill the columns with different patterns
	for i := 0; i < n; i++ {
		col1Data[i] = uint16((i + 0*10) % 100)
		col2Data[i] = uint16((i + 1*10) % 100)
		col3Data[i] = uint16((i + 2*10) % 100)
	}

	// Create Gota Series for each column
	col1 := gotaSeries.Ints(col1Data)
	col2 := gotaSeries.Ints(col2Data)
	col3 := gotaSeries.Ints(col3Data)

	// Set column names
	col1.Name = "col1"
	col2.Name = "col2"
	col3.Name = "col3"

	return gotaDataframe.New(col1, col2, col3)
}

// generateGotaUint32DataFrame creates a Gota DataFrame with 3 uint32 columns and n rows for benchmarking
func generateGotaUint32DataFrame(n int) gotaDataframe.DataFrame {
	// Create 3 columns of uint32 data
	col1Data := make([]uint32, n)
	col2Data := make([]uint32, n)
	col3Data := make([]uint32, n)

	// Fill the columns with different patterns
	for i := 0; i < n; i++ {
		col1Data[i] = uint32((i + 0*10) % 100)
		col2Data[i] = uint32((i + 1*10) % 100)
		col3Data[i] = uint32((i + 2*10) % 100)
	}

	// Create Gota Series for each column
	col1 := gotaSeries.Ints(col1Data)
	col2 := gotaSeries.Ints(col2Data)
	col3 := gotaSeries.Ints(col3Data)

	// Set column names
	col1.Name = "col1"
	col2.Name = "col2"
	col3.Name = "col3"

	return gotaDataframe.New(col1, col2, col3)
}

// generateGotaUint64DataFrame creates a Gota DataFrame with 3 uint64 columns and n rows for benchmarking
func generateGotaUint64DataFrame(n int) gotaDataframe.DataFrame {
	// Create 3 columns of uint64 data
	col1Data := make([]uint64, n)
	col2Data := make([]uint64, n)
	col3Data := make([]uint64, n)

	// Fill the columns with different patterns
	for i := 0; i < n; i++ {
		col1Data[i] = uint64((i + 0*10) % 100)
		col2Data[i] = uint64((i + 1*10) % 100)
		col3Data[i] = uint64((i + 2*10) % 100)
	}

	// Create Gota Series for each column
	col1 := gotaSeries.Ints(col1Data)
	col2 := gotaSeries.Ints(col2Data)
	col3 := gotaSeries.Ints(col3Data)

	// Set column names
	col1.Name = "col1"
	col2.Name = "col2"
	col3.Name = "col3"

	return gotaDataframe.New(col1, col2, col3)
}

// generateGotaFloat32DataFrame creates a Gota DataFrame with 3 float32 columns and n rows for benchmarking
func generateGotaFloat32DataFrame(n int) gotaDataframe.DataFrame {
	// Create 3 columns of float32 data
	col1Data := make([]float32, n)
	col2Data := make([]float32, n)
	col3Data := make([]float32, n)

	// Fill the columns with different patterns
	for i := 0; i < n; i++ {
		col1Data[i] = float32((i + 0*10) % 100)
		col2Data[i] = float32((i + 1*10) % 100)
		col3Data[i] = float32((i + 2*10) % 100)
	}

	// Create Gota Series for each column
	col1 := gotaSeries.Floats(col1Data)
	col2 := gotaSeries.Floats(col2Data)
	col3 := gotaSeries.Floats(col3Data)

	// Set column names
	col1.Name = "col1"
	col2.Name = "col2"
	col3.Name = "col3"

	return gotaDataframe.New(col1, col2, col3)
}

// generateGotaFloat64DataFrame creates a Gota DataFrame with 3 float64 columns and n rows for benchmarking
func generateGotaFloat64DataFrame(n int) gotaDataframe.DataFrame {
	// Create 3 columns of float64 data
	col1Data := make([]float64, n)
	col2Data := make([]float64, n)
	col3Data := make([]float64, n)

	// Fill the columns with different patterns
	for i := 0; i < n; i++ {
		col1Data[i] = float64((i + 0*10) % 100)
		col2Data[i] = float64((i + 1*10) % 100)
		col3Data[i] = float64((i + 2*10) % 100)
	}

	// Create Gota Series for each column
	col1 := gotaSeries.Floats(col1Data)
	col2 := gotaSeries.Floats(col2Data)
	col3 := gotaSeries.Floats(col3Data)

	// Set column names
	col1.Name = "col1"
	col2.Name = "col2"
	col3.Name = "col3"

	return gotaDataframe.New(col1, col2, col3)
}

// Data generation functions for non-numeric types

// generateStringDataFrame creates a DataFrame with 3 string columns and n rows for benchmarking
func generateStringDataFrame(n int) (*dataframe.DataFrame, error) {
	mem := memory.NewGoAllocator()

	// String patterns as defined in the design document
	patterns := []string{"apple", "banana", "cherry", "date", "elderberry"}

	// Create 3 columns of string data
	columns := make([]arrow.Array, 3)
	columnNames := []string{"col1", "col2", "col3"}

	// Ensure cleanup of columns if error occurs
	defer func() {
		if columns != nil {
			for _, col := range columns {
				if col != nil {
					col.Release()
				}
			}
		}
	}()

	for colIdx := 0; colIdx < 3; colIdx++ {
		builder := array.NewStringBuilder(mem)
		if builder == nil {
			return nil, fmt.Errorf("failed to create string builder for column %d", colIdx)
		}

		builder.Reserve(n)

		// Add values with different patterns for each column
		for i := 0; i < n; i++ {
			patternIdx := (i + colIdx*10) % len(patterns)
			builder.Append(patterns[patternIdx])
		}

		columns[colIdx] = builder.NewArray()
		builder.Release()
	}

	df, err := dataframe.NewDataFrame(columns, columnNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	// Clear columns slice to prevent cleanup in defer
	columns = nil
	return df, nil
}

// generateBooleanDataFrame creates a DataFrame with 3 boolean columns and n rows for benchmarking
func generateBooleanDataFrame(n int) (*dataframe.DataFrame, error) {
	mem := memory.NewGoAllocator()

	// Create 3 columns of boolean data
	columns := make([]arrow.Array, 3)
	columnNames := []string{"col1", "col2", "col3"}

	// Ensure cleanup of columns if error occurs
	defer func() {
		if columns != nil {
			for _, col := range columns {
				if col != nil {
					col.Release()
				}
			}
		}
	}()

	for colIdx := 0; colIdx < 3; colIdx++ {
		builder := array.NewBooleanBuilder(mem)
		if builder == nil {
			return nil, fmt.Errorf("failed to create boolean builder for column %d", colIdx)
		}

		builder.Reserve(n)

		// Add values with 50% true and 50% false, different patterns for each column
		for i := 0; i < n; i++ {
			value := ((i + colIdx*10) % 2) == 0
			builder.Append(value)
		}

		columns[colIdx] = builder.NewArray()
		builder.Release()
	}

	df, err := dataframe.NewDataFrame(columns, columnNames)
	if err != nil {
		return nil, fmt.Errorf("failed to create DataFrame: %w", err)
	}

	// Clear columns slice to prevent cleanup in defer
	columns = nil
	return df, nil
}

// generateGotaStringDataFrame creates a Gota DataFrame with 3 string columns and n rows for benchmarking
func generateGotaStringDataFrame(n int) gotaDataframe.DataFrame {
	// String patterns as defined in the design document
	patterns := []string{"apple", "banana", "cherry", "date", "elderberry"}

	// Create 3 columns of string data
	col1Data := make([]string, n)
	col2Data := make([]string, n)
	col3Data := make([]string, n)

	// Fill the columns with different patterns
	for i := 0; i < n; i++ {
		col1Data[i] = patterns[(i+0*10)%len(patterns)]
		col2Data[i] = patterns[(i+1*10)%len(patterns)]
		col3Data[i] = patterns[(i+2*10)%len(patterns)]
	}

	// Create Gota Series for each column
	col1 := gotaSeries.Strings(col1Data)
	col2 := gotaSeries.Strings(col2Data)
	col3 := gotaSeries.Strings(col3Data)

	// Set column names
	col1.Name = "col1"
	col2.Name = "col2"
	col3.Name = "col3"

	return gotaDataframe.New(col1, col2, col3)
}

// generateGotaBooleanDataFrame creates a Gota DataFrame with 3 boolean columns and n rows for benchmarking
func generateGotaBooleanDataFrame(n int) gotaDataframe.DataFrame {
	// Create 3 columns of boolean data
	col1Data := make([]bool, n)
	col2Data := make([]bool, n)
	col3Data := make([]bool, n)

	// Fill the columns with 50% true and 50% false, different patterns for each column
	for i := 0; i < n; i++ {
		col1Data[i] = ((i + 0*10) % 2) == 0
		col2Data[i] = ((i + 1*10) % 2) == 0
		col3Data[i] = ((i + 2*10) % 2) == 0
	}

	// Create Gota Series for each column
	col1 := gotaSeries.Bools(col1Data)
	col2 := gotaSeries.Bools(col2Data)
	col3 := gotaSeries.Bools(col3Data)

	// Set column names
	col1.Name = "col1"
	col2.Name = "col2"
	col3.Name = "col3"

	return gotaDataframe.New(col1, col2, col3)
}

// Benchmark functions for small dataset numeric types

// BenchmarkWhere_Small_Int8 benchmarks Gleam DataFrame where operation on int8 data with small dataset
func BenchmarkWhere_Small_Int8(b *testing.B) {
	// Generate test data
	df, err := generateInt8DataFrame(smallDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 1)) // 3 columns * 1 byte per int8

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, int8(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Small_Int8 benchmarks Gota DataFrame where operation on int8 data with small dataset
func BenchmarkGotaWhere_Small_Int8(b *testing.B) {
	// Generate test data
	df := generateGotaInt8DataFrame(smallDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 1)) // 3 columns * 1 byte per int8

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Small_Int16 benchmarks Gleam DataFrame where operation on int16 data with small dataset
func BenchmarkWhere_Small_Int16(b *testing.B) {
	// Generate test data
	df, err := generateInt16DataFrame(smallDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 2)) // 3 columns * 2 bytes per int16

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, int16(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Small_Int16 benchmarks Gota DataFrame where operation on int16 data with small dataset
func BenchmarkGotaWhere_Small_Int16(b *testing.B) {
	// Generate test data
	df := generateGotaInt16DataFrame(smallDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 2)) // 3 columns * 2 bytes per int16

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Small_Int32 benchmarks Gleam DataFrame where operation on int32 data with small dataset
func BenchmarkWhere_Small_Int32(b *testing.B) {
	// Generate test data
	df, err := generateInt32DataFrame(smallDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 4)) // 3 columns * 4 bytes per int32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, int32(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Small_Int32 benchmarks Gota DataFrame where operation on int32 data with small dataset
func BenchmarkGotaWhere_Small_Int32(b *testing.B) {
	// Generate test data
	df := generateGotaInt32DataFrame(smallDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 4)) // 3 columns * 4 bytes per int32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Small_Int64 benchmarks Gleam DataFrame where operation on int64 data with small dataset
func BenchmarkWhere_Small_Int64(b *testing.B) {
	// Generate test data
	df, err := generateInt64DataFrame(smallDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 8)) // 3 columns * 8 bytes per int64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, int64(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Small_Int64 benchmarks Gota DataFrame where operation on int64 data with small dataset
func BenchmarkGotaWhere_Small_Int64(b *testing.B) {
	// Generate test data
	df := generateGotaInt64DataFrame(smallDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 8)) // 3 columns * 8 bytes per int64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Small_Uint8 benchmarks Gleam DataFrame where operation on uint8 data with small dataset
func BenchmarkWhere_Small_Uint8(b *testing.B) {
	// Generate test data
	df, err := generateUint8DataFrame(smallDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 1)) // 3 columns * 1 byte per uint8

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, uint8(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Small_Uint8 benchmarks Gota DataFrame where operation on uint8 data with small dataset
func BenchmarkGotaWhere_Small_Uint8(b *testing.B) {
	// Generate test data
	df := generateGotaUint8DataFrame(smallDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 1)) // 3 columns * 1 byte per uint8

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Small_Uint16 benchmarks Gleam DataFrame where operation on uint16 data with small dataset
func BenchmarkWhere_Small_Uint16(b *testing.B) {
	// Generate test data
	df, err := generateUint16DataFrame(smallDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 2)) // 3 columns * 2 bytes per uint16

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, uint16(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Small_Uint16 benchmarks Gota DataFrame where operation on uint16 data with small dataset
func BenchmarkGotaWhere_Small_Uint16(b *testing.B) {
	// Generate test data
	df := generateGotaUint16DataFrame(smallDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 2)) // 3 columns * 2 bytes per uint16

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Small_Uint32 benchmarks Gleam DataFrame where operation on uint32 data with small dataset
func BenchmarkWhere_Small_Uint32(b *testing.B) {
	// Generate test data
	df, err := generateUint32DataFrame(smallDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 4)) // 3 columns * 4 bytes per uint32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, uint32(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Small_Uint32 benchmarks Gota DataFrame where operation on uint32 data with small dataset
func BenchmarkGotaWhere_Small_Uint32(b *testing.B) {
	// Generate test data
	df := generateGotaUint32DataFrame(smallDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 4)) // 3 columns * 4 bytes per uint32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Small_Uint64 benchmarks Gleam DataFrame where operation on uint64 data with small dataset
func BenchmarkWhere_Small_Uint64(b *testing.B) {
	// Generate test data
	df, err := generateUint64DataFrame(smallDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 8)) // 3 columns * 8 bytes per uint64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, uint64(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Small_Uint64 benchmarks Gota DataFrame where operation on uint64 data with small dataset
func BenchmarkGotaWhere_Small_Uint64(b *testing.B) {
	// Generate test data
	df := generateGotaUint64DataFrame(smallDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 8)) // 3 columns * 8 bytes per uint64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Small_Float32 benchmarks Gleam DataFrame where operation on float32 data with small dataset
func BenchmarkWhere_Small_Float32(b *testing.B) {
	// Generate test data
	df, err := generateFloat32DataFrame(smallDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 4)) // 3 columns * 4 bytes per float32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, float32(50.0))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Small_Float32 benchmarks Gota DataFrame where operation on float32 data with small dataset
func BenchmarkGotaWhere_Small_Float32(b *testing.B) {
	// Generate test data
	df := generateGotaFloat32DataFrame(smallDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 4)) // 3 columns * 4 bytes per float32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50.0
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50.0})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Small_Float64 benchmarks Gleam DataFrame where operation on float64 data with small dataset
func BenchmarkWhere_Small_Float64(b *testing.B) {
	// Generate test data
	df, err := generateFloat64DataFrame(smallDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 8)) // 3 columns * 8 bytes per float64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, float64(50.0))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Small_Float64 benchmarks Gota DataFrame where operation on float64 data with small dataset
func BenchmarkGotaWhere_Small_Float64(b *testing.B) {
	// Generate test data
	df := generateGotaFloat64DataFrame(smallDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 8)) // 3 columns * 8 bytes per float64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50.0
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50.0})
		_ = result // Use result to prevent optimization
	}
}

// Benchmark functions for medium dataset numeric types

// BenchmarkWhere_Medium_Int8 benchmarks Gleam DataFrame where operation on int8 data with medium dataset
func BenchmarkWhere_Medium_Int8(b *testing.B) {
	// Generate test data
	df, err := generateInt8DataFrame(mediumDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 1)) // 3 columns * 1 byte per int8

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, int8(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Medium_Int8 benchmarks Gota DataFrame where operation on int8 data with medium dataset
func BenchmarkGotaWhere_Medium_Int8(b *testing.B) {
	// Generate test data
	df := generateGotaInt8DataFrame(mediumDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 1)) // 3 columns * 1 byte per int8

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Medium_Int16 benchmarks Gleam DataFrame where operation on int16 data with medium dataset
func BenchmarkWhere_Medium_Int16(b *testing.B) {
	// Generate test data
	df, err := generateInt16DataFrame(mediumDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 2)) // 3 columns * 2 bytes per int16

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, int16(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Medium_Int16 benchmarks Gota DataFrame where operation on int16 data with medium dataset
func BenchmarkGotaWhere_Medium_Int16(b *testing.B) {
	// Generate test data
	df := generateGotaInt16DataFrame(mediumDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 2)) // 3 columns * 2 bytes per int16

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Medium_Int32 benchmarks Gleam DataFrame where operation on int32 data with medium dataset
func BenchmarkWhere_Medium_Int32(b *testing.B) {
	// Generate test data
	df, err := generateInt32DataFrame(mediumDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 4)) // 3 columns * 4 bytes per int32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, int32(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Medium_Int32 benchmarks Gota DataFrame where operation on int32 data with medium dataset
func BenchmarkGotaWhere_Medium_Int32(b *testing.B) {
	// Generate test data
	df := generateGotaInt32DataFrame(mediumDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 4)) // 3 columns * 4 bytes per int32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Medium_Int64 benchmarks Gleam DataFrame where operation on int64 data with medium dataset
func BenchmarkWhere_Medium_Int64(b *testing.B) {
	// Generate test data
	df, err := generateInt64DataFrame(mediumDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 8)) // 3 columns * 8 bytes per int64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, int64(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Medium_Int64 benchmarks Gota DataFrame where operation on int64 data with medium dataset
func BenchmarkGotaWhere_Medium_Int64(b *testing.B) {
	// Generate test data
	df := generateGotaInt64DataFrame(mediumDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 8)) // 3 columns * 8 bytes per int64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Medium_Uint8 benchmarks Gleam DataFrame where operation on uint8 data with medium dataset
func BenchmarkWhere_Medium_Uint8(b *testing.B) {
	// Generate test data
	df, err := generateUint8DataFrame(mediumDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 1)) // 3 columns * 1 byte per uint8

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, uint8(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Medium_Uint8 benchmarks Gota DataFrame where operation on uint8 data with medium dataset
func BenchmarkGotaWhere_Medium_Uint8(b *testing.B) {
	// Generate test data
	df := generateGotaUint8DataFrame(mediumDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 1)) // 3 columns * 1 byte per uint8

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Medium_Uint16 benchmarks Gleam DataFrame where operation on uint16 data with medium dataset
func BenchmarkWhere_Medium_Uint16(b *testing.B) {
	// Generate test data
	df, err := generateUint16DataFrame(mediumDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 2)) // 3 columns * 2 bytes per uint16

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, uint16(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Medium_Uint16 benchmarks Gota DataFrame where operation on uint16 data with medium dataset
func BenchmarkGotaWhere_Medium_Uint16(b *testing.B) {
	// Generate test data
	df := generateGotaUint16DataFrame(mediumDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 2)) // 3 columns * 2 bytes per uint16

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Medium_Uint32 benchmarks Gleam DataFrame where operation on uint32 data with medium dataset
func BenchmarkWhere_Medium_Uint32(b *testing.B) {
	// Generate test data
	df, err := generateUint32DataFrame(mediumDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 4)) // 3 columns * 4 bytes per uint32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, uint32(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Medium_Uint32 benchmarks Gota DataFrame where operation on uint32 data with medium dataset
func BenchmarkGotaWhere_Medium_Uint32(b *testing.B) {
	// Generate test data
	df := generateGotaUint32DataFrame(mediumDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 4)) // 3 columns * 4 bytes per uint32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Medium_Uint64 benchmarks Gleam DataFrame where operation on uint64 data with medium dataset
func BenchmarkWhere_Medium_Uint64(b *testing.B) {
	// Generate test data
	df, err := generateUint64DataFrame(mediumDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 8)) // 3 columns * 8 bytes per uint64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, uint64(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Medium_Uint64 benchmarks Gota DataFrame where operation on uint64 data with medium dataset
func BenchmarkGotaWhere_Medium_Uint64(b *testing.B) {
	// Generate test data
	df := generateGotaUint64DataFrame(mediumDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 8)) // 3 columns * 8 bytes per uint64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Medium_Float32 benchmarks Gleam DataFrame where operation on float32 data with medium dataset
func BenchmarkWhere_Medium_Float32(b *testing.B) {
	// Generate test data
	df, err := generateFloat32DataFrame(mediumDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 4)) // 3 columns * 4 bytes per float32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, float32(50.0))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Medium_Float32 benchmarks Gota DataFrame where operation on float32 data with medium dataset
func BenchmarkGotaWhere_Medium_Float32(b *testing.B) {
	// Generate test data
	df := generateGotaFloat32DataFrame(mediumDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 4)) // 3 columns * 4 bytes per float32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50.0
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50.0})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Medium_Float64 benchmarks Gleam DataFrame where operation on float64 data with medium dataset
func BenchmarkWhere_Medium_Float64(b *testing.B) {
	// Generate test data
	df, err := generateFloat64DataFrame(mediumDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 8)) // 3 columns * 8 bytes per float64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, float64(50.0))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Medium_Float64 benchmarks Gota DataFrame where operation on float64 data with medium dataset
func BenchmarkGotaWhere_Medium_Float64(b *testing.B) {
	// Generate test data
	df := generateGotaFloat64DataFrame(mediumDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 8)) // 3 columns * 8 bytes per float64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50.0
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50.0})
		_ = result // Use result to prevent optimization
	}
}

// Benchmark functions for large dataset numeric types

// BenchmarkWhere_Large_Int8 benchmarks Gleam DataFrame where operation on int8 data with large dataset
func BenchmarkWhere_Large_Int8(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df, err := generateInt8DataFrame(largeDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 1)) // 3 columns * 1 byte per int8

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, int8(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Large_Int8 benchmarks Gota DataFrame where operation on int8 data with large dataset
func BenchmarkGotaWhere_Large_Int8(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df := generateGotaInt8DataFrame(largeDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 1)) // 3 columns * 1 byte per int8

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Large_Int16 benchmarks Gleam DataFrame where operation on int16 data with large dataset
func BenchmarkWhere_Large_Int16(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df, err := generateInt16DataFrame(largeDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 2)) // 3 columns * 2 bytes per int16

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, int16(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Large_Int16 benchmarks Gota DataFrame where operation on int16 data with large dataset
func BenchmarkGotaWhere_Large_Int16(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df := generateGotaInt16DataFrame(largeDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 2)) // 3 columns * 2 bytes per int16

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Large_Int32 benchmarks Gleam DataFrame where operation on int32 data with large dataset
func BenchmarkWhere_Large_Int32(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df, err := generateInt32DataFrame(largeDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 4)) // 3 columns * 4 bytes per int32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, int32(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Large_Int32 benchmarks Gota DataFrame where operation on int32 data with large dataset
func BenchmarkGotaWhere_Large_Int32(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df := generateGotaInt32DataFrame(largeDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 4)) // 3 columns * 4 bytes per int32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Large_Int64 benchmarks Gleam DataFrame where operation on int64 data with large dataset
func BenchmarkWhere_Large_Int64(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df, err := generateInt64DataFrame(largeDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 8)) // 3 columns * 8 bytes per int64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, int64(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Large_Int64 benchmarks Gota DataFrame where operation on int64 data with large dataset
func BenchmarkGotaWhere_Large_Int64(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df := generateGotaInt64DataFrame(largeDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 8)) // 3 columns * 8 bytes per int64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Large_Uint8 benchmarks Gleam DataFrame where operation on uint8 data with large dataset
func BenchmarkWhere_Large_Uint8(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df, err := generateUint8DataFrame(largeDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 1)) // 3 columns * 1 byte per uint8

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, uint8(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Large_Uint8 benchmarks Gota DataFrame where operation on uint8 data with large dataset
func BenchmarkGotaWhere_Large_Uint8(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df := generateGotaUint8DataFrame(largeDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 1)) // 3 columns * 1 byte per uint8

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Large_Uint16 benchmarks Gleam DataFrame where operation on uint16 data with large dataset
func BenchmarkWhere_Large_Uint16(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df, err := generateUint16DataFrame(largeDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 2)) // 3 columns * 2 bytes per uint16

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, uint16(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Large_Uint16 benchmarks Gota DataFrame where operation on uint16 data with large dataset
func BenchmarkGotaWhere_Large_Uint16(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df := generateGotaUint16DataFrame(largeDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 2)) // 3 columns * 2 bytes per uint16

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Large_Uint32 benchmarks Gleam DataFrame where operation on uint32 data with large dataset
func BenchmarkWhere_Large_Uint32(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df, err := generateUint32DataFrame(largeDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 4)) // 3 columns * 4 bytes per uint32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, uint32(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Large_Uint32 benchmarks Gota DataFrame where operation on uint32 data with large dataset
func BenchmarkGotaWhere_Large_Uint32(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df := generateGotaUint32DataFrame(largeDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 4)) // 3 columns * 4 bytes per uint32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Large_Uint64 benchmarks Gleam DataFrame where operation on uint64 data with large dataset
func BenchmarkWhere_Large_Uint64(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df, err := generateUint64DataFrame(largeDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 8)) // 3 columns * 8 bytes per uint64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, uint64(50))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Large_Uint64 benchmarks Gota DataFrame where operation on uint64 data with large dataset
func BenchmarkGotaWhere_Large_Uint64(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df := generateGotaUint64DataFrame(largeDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 8)) // 3 columns * 8 bytes per uint64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Large_Float32 benchmarks Gleam DataFrame where operation on float32 data with large dataset
func BenchmarkWhere_Large_Float32(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df, err := generateFloat32DataFrame(largeDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 4)) // 3 columns * 4 bytes per float32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, float32(50.0))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Large_Float32 benchmarks Gota DataFrame where operation on float32 data with large dataset
func BenchmarkGotaWhere_Large_Float32(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df := generateGotaFloat32DataFrame(largeDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 4)) // 3 columns * 4 bytes per float32

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50.0
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50.0})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Large_Float64 benchmarks Gleam DataFrame where operation on float64 data with large dataset
func BenchmarkWhere_Large_Float64(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df, err := generateFloat64DataFrame(largeDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 8)) // 3 columns * 8 bytes per float64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array
		filter, err := s.Comparison(utils.Greater, float64(50.0))
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Large_Float64 benchmarks Gota DataFrame where operation on float64 data with large dataset
func BenchmarkGotaWhere_Large_Float64(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df := generateGotaFloat64DataFrame(largeDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 8)) // 3 columns * 8 bytes per float64

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Greater comparison and threshold 50.0
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Greater, Comparando: 50.0})
		_ = result // Use result to prevent optimization
	}
}

// Benchmark functions for string types

// BenchmarkWhere_Small_String benchmarks Gleam DataFrame where operation on string data with small dataset
func BenchmarkWhere_Small_String(b *testing.B) {
	// Generate test data
	df, err := generateStringDataFrame(smallDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation (approximate 6 bytes per string on average)
	b.SetBytes(int64(smallDatasetSize * 3 * 6)) // 3 columns * ~6 bytes per string

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array with Equal comparison and "banana" value
		filter, err := s.Comparison(utils.Equal, "banana")
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Small_String benchmarks Gota DataFrame where operation on string data with small dataset
func BenchmarkGotaWhere_Small_String(b *testing.B) {
	// Generate test data
	df := generateGotaStringDataFrame(smallDatasetSize)

	// Set bytes processed for throughput calculation (approximate 6 bytes per string on average)
	b.SetBytes(int64(smallDatasetSize * 3 * 6)) // 3 columns * ~6 bytes per string

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Equal comparison and "banana" value
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Eq, Comparando: "banana"})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Medium_String benchmarks Gleam DataFrame where operation on string data with medium dataset
func BenchmarkWhere_Medium_String(b *testing.B) {
	// Generate test data
	df, err := generateStringDataFrame(mediumDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation (approximate 6 bytes per string on average)
	b.SetBytes(int64(mediumDatasetSize * 3 * 6)) // 3 columns * ~6 bytes per string

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array with Equal comparison and "banana" value
		filter, err := s.Comparison(utils.Equal, "banana")
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Medium_String benchmarks Gota DataFrame where operation on string data with medium dataset
func BenchmarkGotaWhere_Medium_String(b *testing.B) {
	// Generate test data
	df := generateGotaStringDataFrame(mediumDatasetSize)

	// Set bytes processed for throughput calculation (approximate 6 bytes per string on average)
	b.SetBytes(int64(mediumDatasetSize * 3 * 6)) // 3 columns * ~6 bytes per string

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Equal comparison and "banana" value
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Eq, Comparando: "banana"})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Large_String benchmarks Gleam DataFrame where operation on string data with large dataset
func BenchmarkWhere_Large_String(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df, err := generateStringDataFrame(largeDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation (approximate 6 bytes per string on average)
	b.SetBytes(int64(largeDatasetSize * 3 * 6)) // 3 columns * ~6 bytes per string

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array with Equal comparison and "banana" value
		filter, err := s.Comparison(utils.Equal, "banana")
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Large_String benchmarks Gota DataFrame where operation on string data with large dataset
func BenchmarkGotaWhere_Large_String(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df := generateGotaStringDataFrame(largeDatasetSize)

	// Set bytes processed for throughput calculation (approximate 6 bytes per string on average)
	b.SetBytes(int64(largeDatasetSize * 3 * 6)) // 3 columns * ~6 bytes per string

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Equal comparison and "banana" value
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Eq, Comparando: "banana"})
		_ = result // Use result to prevent optimization
	}
}

// Benchmark functions for boolean types

// BenchmarkWhere_Small_Boolean benchmarks Gleam DataFrame where operation on boolean data with small dataset
func BenchmarkWhere_Small_Boolean(b *testing.B) {
	// Generate test data
	df, err := generateBooleanDataFrame(smallDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 1)) // 3 columns * 1 byte per boolean

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array with Equal comparison and true value
		filter, err := s.Comparison(utils.Equal, true)
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Small_Boolean benchmarks Gota DataFrame where operation on boolean data with small dataset
func BenchmarkGotaWhere_Small_Boolean(b *testing.B) {
	// Generate test data
	df := generateGotaBooleanDataFrame(smallDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(smallDatasetSize * 3 * 1)) // 3 columns * 1 byte per boolean

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Equal comparison and true value
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Eq, Comparando: true})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Medium_Boolean benchmarks Gleam DataFrame where operation on boolean data with medium dataset
func BenchmarkWhere_Medium_Boolean(b *testing.B) {
	// Generate test data
	df, err := generateBooleanDataFrame(mediumDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 1)) // 3 columns * 1 byte per boolean

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array with Equal comparison and true value
		filter, err := s.Comparison(utils.Equal, true)
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Medium_Boolean benchmarks Gota DataFrame where operation on boolean data with medium dataset
func BenchmarkGotaWhere_Medium_Boolean(b *testing.B) {
	// Generate test data
	df := generateGotaBooleanDataFrame(mediumDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(mediumDatasetSize * 3 * 1)) // 3 columns * 1 byte per boolean

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Equal comparison and true value
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Eq, Comparando: true})
		_ = result // Use result to prevent optimization
	}
}

// BenchmarkWhere_Large_Boolean benchmarks Gleam DataFrame where operation on boolean data with large dataset
func BenchmarkWhere_Large_Boolean(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df, err := generateBooleanDataFrame(largeDatasetSize)
	if err != nil {
		b.Fatal(err)
	}
	defer df.Release()

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 1)) // 3 columns * 1 byte per boolean

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Get first column
		col, err := df.ColumnByName("col1")
		if err != nil {
			b.Fatal(err)
		}

		// Create Series from first column
		s := series.NewSeries("col1", col)

		// Create comparison array with Equal comparison and true value
		filter, err := s.Comparison(utils.Equal, true)
		if err != nil {
			s.Release()
			b.Fatal(err)
		}

		// Perform where operation
		result, err := df.Where(filter)
		if err != nil {
			s.Release()
			filter.Release()
			b.Fatal(err)
		}

		// Clean up
		s.Release()
		filter.Release()
		result.Release()
	}
}

// BenchmarkGotaWhere_Large_Boolean benchmarks Gota DataFrame where operation on boolean data with large dataset
func BenchmarkGotaWhere_Large_Boolean(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate test data
	df := generateGotaBooleanDataFrame(largeDatasetSize)

	// Set bytes processed for throughput calculation
	b.SetBytes(int64(largeDatasetSize * 3 * 1)) // 3 columns * 1 byte per boolean

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Perform where operation with Equal comparison and true value
		result := df.Filter(gotaDataframe.F{Colname: "col1", Comparator: gotaSeries.Eq, Comparando: true})
		_ = result // Use result to prevent optimization
	}
}
