package series

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-gota/gota/series"
	"testing"
)

// generateInt32Data creates a Series with n int32 elements for benchmarking
func generateInt32Data(n int) *Series {
	mem := memory.NewGoAllocator()
	builder := array.NewInt32Builder(mem)

	// Pre-allocate space for better performance
	builder.Reserve(n)

	// Add values from 1 to n
	for i := 0; i < n; i++ {
		builder.Append(int32(i % 100)) // Use modulo to avoid overflow and keep values small
	}

	arr := builder.NewArray()
	// Release the builder after creating the array
	builder.Release()

	// Don't release the array here, the Series will own it
	return NewSeries("benchmark_int32", arr)
}

// generateInt64Data creates a Series with n int64 elements for benchmarking
func generateInt64Data(n int) *Series {
	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)

	// Pre-allocate space for better performance
	builder.Reserve(n)

	// Add values from 1 to n
	for i := 0; i < n; i++ {
		builder.Append(int64(i % 100)) // Use modulo to avoid overflow and keep values small
	}

	arr := builder.NewArray()
	// Release the builder after creating the array
	builder.Release()

	// Don't release the array here, the Series will own it
	return NewSeries("benchmark_int64", arr)
}

// generateFloat32Data creates a Series with n float32 elements for benchmarking
func generateFloat32Data(n int) *Series {
	mem := memory.NewGoAllocator()
	builder := array.NewFloat32Builder(mem)

	// Pre-allocate space for better performance
	builder.Reserve(n)

	// Add values from 1 to n
	for i := 0; i < n; i++ {
		builder.Append(float32(i % 100)) // Use modulo to avoid overflow and keep values small
	}

	arr := builder.NewArray()
	// Release the builder after creating the array
	builder.Release()

	// Don't release the array here, the Series will own it
	return NewSeries("benchmark_float32", arr)
}

// generateFloat64Data creates a Series with n float64 elements for benchmarking
func generateFloat64Data(n int) *Series {
	mem := memory.NewGoAllocator()
	builder := array.NewFloat64Builder(mem)

	// Pre-allocate space for better performance
	builder.Reserve(n)

	// Add values from 1 to n
	for i := 0; i < n; i++ {
		builder.Append(float64(i % 100)) // Use modulo to avoid overflow and keep values small
	}

	arr := builder.NewArray()
	// Release the builder after creating the array
	builder.Release()

	// Don't release the array here, the Series will own it
	return NewSeries("benchmark_float64", arr)
}

// generateGotaInt32Data creates a Gota Series with n int32 elements for benchmarking
func generateGotaInt32Data(n int) series.Series {
	// Create a slice to hold the data
	data := make([]int32, n)

	// Fill the slice with the same values as the gleam Series
	for i := 0; i < n; i++ {
		data[i] = int32(i % 100) // Use modulo to avoid overflow and keep values small
	}

	// Create and return a Gota Series
	return series.Ints(data)
}

// generateGotaInt64Data creates a Gota Series with n int64 elements for benchmarking
func generateGotaInt64Data(n int) series.Series {
	// Create a slice to hold the data
	data := make([]int64, n)

	// Fill the slice with the same values as the gleam Series
	for i := 0; i < n; i++ {
		data[i] = int64(i % 100) // Use modulo to avoid overflow and keep values small
	}

	// Create and return a Gota Series
	return series.Ints(data)
}

// generateGotaFloat32Data creates a Gota Series with n float32 elements for benchmarking
func generateGotaFloat32Data(n int) series.Series {
	// Create a slice to hold the data
	data := make([]float32, n)

	// Fill the slice with the same values as the gleam Series
	for i := 0; i < n; i++ {
		data[i] = float32(i % 100) // Use modulo to avoid overflow and keep values small
	}

	// Create and return a Gota Series
	return series.Floats(data)
}

// generateGotaFloat64Data creates a Gota Series with n float64 elements for benchmarking
func generateGotaFloat64Data(n int) series.Series {
	// Create a slice to hold the data
	data := make([]float64, n)

	// Fill the slice with the same values as the gleam Series
	for i := 0; i < n; i++ {
		data[i] = float64(i % 100) // Use modulo to avoid overflow and keep values small
	}

	// Create and return a Gota Series
	return series.Floats(data)
}

// Benchmark Sum vs NaiveSum vs Gota Sum with different data types and sizes

// Int32 benchmarks
func BenchmarkSum_Int32_Small(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateInt32Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Sum()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Gota Series Sum benchmarks
// Int32 benchmarks
func BenchmarkGotaSum_Int32_Small(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaInt32Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Sum()
	}
}

func BenchmarkSum_Int32_Medium(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateInt32Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Sum()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaSum_Int32_Medium(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaInt32Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Sum()
	}
}

func BenchmarkSum_Int32_Large(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateInt32Data(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Int32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Sum()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaSum_Int32_Large(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateGotaInt32Data(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Int32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Sum()
	}
}

// Int64 benchmarks
func BenchmarkSum_Int64_Small(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateInt64Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Sum()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaSum_Int64_Small(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaInt64Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Sum()
	}
}

func BenchmarkSum_Int64_Medium(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateInt64Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Sum()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaSum_Int64_Medium(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaInt64Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Sum()
	}
}

func BenchmarkSum_Int64_Large(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateInt64Data(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Sum()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaSum_Int64_Large(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateGotaInt64Data(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Sum()
	}
}

// Float32 benchmarks
func BenchmarkSum_Float32_Small(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateFloat32Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Float32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Sum()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Float32 benchmarks
func BenchmarkGotaSum_Float32_Small(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaFloat32Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Float32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Sum()
	}
}

func BenchmarkSum_Float32_Medium(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateFloat32Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Float32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Sum()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaSum_Float32_Medium(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaFloat32Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Float32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Sum()
	}
}

func BenchmarkSum_Float32_Large(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateFloat32Data(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Float32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Sum()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaSum_Float32_Large(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateGotaFloat32Data(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Float32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Sum()
	}
}

// Float64 benchmarks
func BenchmarkSum_Float64_Small(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateFloat64Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Sum()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Float64 benchmarks
func BenchmarkGotaSum_Float64_Small(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaFloat64Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Sum()
	}
}

func BenchmarkSum_Float64_Medium(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateFloat64Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Sum()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaSum_Float64_Medium(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaFloat64Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Sum()
	}
}

func BenchmarkSum_Float64_Large(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateFloat64Data(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Sum()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaSum_Float64_Large(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateGotaFloat64Data(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Sum()
	}
}
