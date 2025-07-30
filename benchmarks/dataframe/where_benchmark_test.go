package dataframe

import (
	"testing"

	dataframe2 "github.com/SHIMA0111/gleam/gleam/dataframe"
	series2 "github.com/SHIMA0111/gleam/gleam/series"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

// Helper functions to generate test data

// generateDataFrame creates a Gleam DataFrame with n rows for benchmarking
func generateDataFrame(n int) *dataframe2.DataFrame {
	mem := memory.NewGoAllocator()

	// Create slices to hold the data
	intData := make([]int64, n)
	floatData := make([]float64, n)
	stringData := make([]string, n)
	boolData := make([]bool, n)

	// Fill the slices with data
	for i := 0; i < n; i++ {
		intData[i] = int64(i % 100)
		floatData[i] = float64(i % 100)
		stringData[i] = "val" + string(rune('0'+i%10))
		boolData[i] = i%2 == 0
	}

	// Create a DataFrame from a map with Go native slices
	df, err := dataframe2.NewDataFrameFromMapWithMemory(mem, map[string]interface{}{
		"int_col":    intData,
		"float_col":  floatData,
		"string_col": stringData,
		"bool_col":   boolData,
	})
	if err != nil {
		panic(err)
	}

	return df
}

// generateGotaDataFrame creates a Gota DataFrame with n rows for benchmarking
func generateGotaDataFrame(n int) dataframe.DataFrame {
	// Create slices to hold the data
	intData := make([]int64, n)
	floatData := make([]float64, n)
	stringData := make([]string, n)
	boolData := make([]bool, n)

	// Fill the slices with the same values as the Gleam DataFrame
	for i := 0; i < n; i++ {
		intData[i] = int64(i % 100)
		floatData[i] = float64(i % 100)
		stringData[i] = "val" + string(rune('0'+i%10))
		boolData[i] = i%2 == 0
	}

	// Create and return a Gota DataFrame
	return dataframe.New(
		series.New(intData, series.Int, "int_col"),
		series.New(floatData, series.Float, "float_col"),
		series.New(stringData, series.String, "string_col"),
		series.New(boolData, series.Bool, "bool_col"),
	)
}

// Benchmark functions for Small size (10,000 rows)

func BenchmarkWhere_Small_Int(b *testing.B) {
	// Generate a small dataset (10,000 rows)
	data := generateDataFrame(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where int_col > 50
		result, err := data.WhereBy("int_col", series2.Greater, int64(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaWhere_Small_Int(b *testing.B) {
	// Generate a small dataset (10,000 rows)
	data := generateGotaDataFrame(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where int_col > 50
		_ = data.Filter(
			dataframe.F{
				Colname:    "int_col",
				Comparator: series.Greater,
				Comparando: 50,
			},
		)
	}
}

func BenchmarkWhere_Small_Float(b *testing.B) {
	// Generate a small dataset (10,000 rows)
	data := generateDataFrame(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where float_col > 50.0
		result, err := data.WhereBy("float_col", series2.Greater, 50.0)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaWhere_Small_Float(b *testing.B) {
	// Generate a small dataset (10,000 rows)
	data := generateGotaDataFrame(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where float_col > 50.0
		_ = data.Filter(
			dataframe.F{
				Colname:    "float_col",
				Comparator: series.Greater,
				Comparando: 50.0,
			},
		)
	}
}

func BenchmarkWhere_Small_String(b *testing.B) {
	// Generate a small dataset (10,000 rows)
	data := generateDataFrame(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * 8)) // Assuming 8 bytes per string pointer

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where string_col = "val5"
		result, err := data.WhereBy("string_col", series2.Equal, "val5")
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaWhere_Small_String(b *testing.B) {
	// Generate a small dataset (10,000 rows)
	data := generateGotaDataFrame(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * 8)) // Assuming 8 bytes per string pointer

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where string_col = "val5"
		_ = data.Filter(
			dataframe.F{
				Colname:    "string_col",
				Comparator: series.Eq,
				Comparando: "val5",
			},
		)
	}
}

func BenchmarkWhere_Small_Bool(b *testing.B) {
	// Generate a small dataset (10,000 rows)
	data := generateDataFrame(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 / 8)) // 1 bit per boolean, 8 booleans per byte

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where bool_col = true
		result, err := data.WhereBy("bool_col", series2.Equal, true)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaWhere_Small_Bool(b *testing.B) {
	// Generate a small dataset (10,000 rows)
	data := generateGotaDataFrame(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 / 8)) // 1 bit per boolean, 8 booleans per byte

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where bool_col = true
		_ = data.Filter(
			dataframe.F{
				Colname:    "bool_col",
				Comparator: series.Eq,
				Comparando: true,
			},
		)
	}
}

// Benchmark functions for Medium size (1,000,000 rows)

func BenchmarkWhere_Medium_Int(b *testing.B) {
	// Generate a medium dataset (1,000,000 rows)
	data := generateDataFrame(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where int_col > 50
		result, err := data.WhereBy("int_col", series2.Greater, int64(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaWhere_Medium_Int(b *testing.B) {
	// Generate a medium dataset (1,000,000 rows)
	data := generateGotaDataFrame(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where int_col > 50
		_ = data.Filter(
			dataframe.F{
				Colname:    "int_col",
				Comparator: series.Greater,
				Comparando: 50,
			},
		)
	}
}

func BenchmarkWhere_Medium_Float(b *testing.B) {
	// Generate a medium dataset (1,000,000 rows)
	data := generateDataFrame(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where float_col > 50.0
		result, err := data.WhereBy("float_col", series2.Greater, 50.0)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaWhere_Medium_Float(b *testing.B) {
	// Generate a medium dataset (1,000,000 rows)
	data := generateGotaDataFrame(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where float_col > 50.0
		_ = data.Filter(
			dataframe.F{
				Colname:    "float_col",
				Comparator: series.Greater,
				Comparando: 50.0,
			},
		)
	}
}

func BenchmarkWhere_Medium_String(b *testing.B) {
	// Generate a medium dataset (1,000,000 rows)
	data := generateDataFrame(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * 8)) // Assuming 8 bytes per string pointer

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where string_col = "val5"
		result, err := data.WhereBy("string_col", series2.Equal, "val5")
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaWhere_Medium_String(b *testing.B) {
	// Generate a medium dataset (1,000,000 rows)
	data := generateGotaDataFrame(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * 8)) // Assuming 8 bytes per string pointer

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where string_col = "val5"
		_ = data.Filter(
			dataframe.F{
				Colname:    "string_col",
				Comparator: series.Eq,
				Comparando: "val5",
			},
		)
	}
}

func BenchmarkWhere_Medium_Bool(b *testing.B) {
	// Generate a medium dataset (1,000,000 rows)
	data := generateDataFrame(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 / 8)) // 1 bit per boolean, 8 booleans per byte

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where bool_col = true
		result, err := data.WhereBy("bool_col", series2.Equal, true)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaWhere_Medium_Bool(b *testing.B) {
	// Generate a medium dataset (1,000,000 rows)
	data := generateGotaDataFrame(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 / 8)) // 1 bit per boolean, 8 booleans per byte

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where bool_col = true
		_ = data.Filter(
			dataframe.F{
				Colname:    "bool_col",
				Comparator: series.Eq,
				Comparando: true,
			},
		)
	}
}

// Benchmark functions for Large size (100,000,000 rows)

func BenchmarkWhere_Large_Int(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 rows)
	data := generateDataFrame(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where int_col > 50
		result, err := data.WhereBy("int_col", series2.Greater, int64(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaWhere_Large_Int(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 rows)
	data := generateGotaDataFrame(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where int_col > 50
		_ = data.Filter(
			dataframe.F{
				Colname:    "int_col",
				Comparator: series.Greater,
				Comparando: 50,
			},
		)
	}
}

func BenchmarkWhere_Large_Float(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 rows)
	data := generateDataFrame(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where float_col > 50.0
		result, err := data.WhereBy("float_col", series2.Greater, 50.0)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaWhere_Large_Float(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 rows)
	data := generateGotaDataFrame(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where float_col > 50.0
		_ = data.Filter(
			dataframe.F{
				Colname:    "float_col",
				Comparator: series.Greater,
				Comparando: 50.0,
			},
		)
	}
}

func BenchmarkWhere_Large_String(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 rows)
	data := generateDataFrame(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * 8)) // Assuming 8 bytes per string pointer

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where string_col = "val5"
		result, err := data.WhereBy("string_col", series2.Equal, "val5")
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaWhere_Large_String(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 rows)
	data := generateGotaDataFrame(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * 8)) // Assuming 8 bytes per string pointer

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where string_col = "val5"
		_ = data.Filter(
			dataframe.F{
				Colname:    "string_col",
				Comparator: series.Eq,
				Comparando: "val5",
			},
		)
	}
}

func BenchmarkWhere_Large_Bool(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 rows)
	data := generateDataFrame(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 / 8)) // 1 bit per boolean, 8 booleans per byte

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where bool_col = true
		result, err := data.WhereBy("bool_col", series2.Equal, true)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaWhere_Large_Bool(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 rows)
	data := generateGotaDataFrame(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 / 8)) // 1 bit per boolean, 8 booleans per byte

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter rows where bool_col = true
		_ = data.Filter(
			dataframe.F{
				Colname:    "bool_col",
				Comparator: series.Eq,
				Comparando: true,
			},
		)
	}
}
