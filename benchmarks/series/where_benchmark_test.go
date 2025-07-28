package series

import (
	series2 "github.com/SHIMA0111/gleam/gleam/series"
	"github.com/apache/arrow-go/v18/arrow"
	"testing"
)

// Where benchmarks for Small size
// Int8
func BenchmarkWhere_Small_Int8(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateInt8Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, int8(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Medium_Int8(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateInt8Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, int8(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Where benchmarks for Large size
// Int8
func BenchmarkWhere_Large_Int8(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateInt8Data(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Int8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, int8(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Int16
func BenchmarkWhere_Small_Int16(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateInt16Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, int16(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Medium_Int16(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateInt16Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, int16(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Large_Int16(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateInt16Data(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Int16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, int16(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Int32
func BenchmarkWhere_Small_Int32(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateInt32Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, int32(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Medium_Int32(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateInt32Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, int32(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Large_Int32(b *testing.B) {
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
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, int32(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Int64
func BenchmarkWhere_Small_Int64(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateInt64Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, int64(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Medium_Int64(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateInt64Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, int64(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Large_Int64(b *testing.B) {
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
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, int64(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Uint8
func BenchmarkWhere_Small_Uint8(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateUint8Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Uint8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, uint8(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Medium_Uint8(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateUint8Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Uint8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, uint8(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Large_Uint8(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateUint8Data(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Uint8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, uint8(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Uint16
func BenchmarkWhere_Small_Uint16(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateUint16Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Uint16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, uint16(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Medium_Uint16(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateUint16Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Uint16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, uint16(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Large_Uint16(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateUint16Data(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Uint16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, uint16(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Uint32
func BenchmarkWhere_Small_Uint32(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateUint32Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Uint32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, uint32(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Medium_Uint32(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateUint32Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Uint32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, uint32(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Large_Uint32(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateUint32Data(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Uint32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, uint32(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Uint64
func BenchmarkWhere_Small_Uint64(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateUint64Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Uint64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, uint64(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Medium_Uint64(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateUint64Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Uint64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, uint64(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Large_Uint64(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateUint64Data(100_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Uint64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, uint64(50))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Float32
func BenchmarkWhere_Small_Float32(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateFloat32Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Float32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, float32(50.0))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Medium_Float32(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateFloat32Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Float32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, float32(50.0))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Large_Float32(b *testing.B) {
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
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, float32(50.0))
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

// Float64
func BenchmarkWhere_Small_Float64(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateFloat64Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, 50.0)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Medium_Float64(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateFloat64Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, 50.0)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkWhere_Large_Float64(b *testing.B) {
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
		// Filter where values greater than 50
		result, err := data.Where(series2.Greater, 50.0)
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}
