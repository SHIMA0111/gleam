package series

import (
	"github.com/apache/arrow-go/v18/arrow"
	"testing"
)

// Mean benchmarks for Small size
// Int8
func BenchmarkMean_Small_Int8(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateInt8Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Small_Int8(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaInt8Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Int16
func BenchmarkMean_Small_Int16(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateInt16Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Small_Int16(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaInt16Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Int32
func BenchmarkMean_Small_Int32(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateInt32Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Small_Int32(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaInt32Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Int64
func BenchmarkMean_Small_Int64(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateInt64Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Small_Int64(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaInt64Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Uint8
func BenchmarkMean_Small_Uint8(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateUint8Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Uint8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Small_Uint8(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaUint8Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Uint8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Uint16
func BenchmarkMean_Small_Uint16(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateUint16Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Uint16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Small_Uint16(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaUint16Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Uint16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Uint32
func BenchmarkMean_Small_Uint32(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateUint32Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Uint32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Small_Uint32(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaUint32Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Uint32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Uint64
func BenchmarkMean_Small_Uint64(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateUint64Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Uint64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Small_Uint64(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaUint64Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Uint64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Float32
func BenchmarkMean_Small_Float32(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateFloat32Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Float32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Small_Float32(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaFloat32Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Float32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Float64
func BenchmarkMean_Small_Float64(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateFloat64Data(10_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Small_Float64(b *testing.B) {
	// Generate a small dataset (10,000 elements)
	data := generateGotaFloat64Data(10_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(10_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Mean benchmarks for Medium size
// Int8
func BenchmarkMean_Medium_Int8(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateInt8Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Medium_Int8(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaInt8Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Int16
func BenchmarkMean_Medium_Int16(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateInt16Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Medium_Int16(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaInt16Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Int32
func BenchmarkMean_Medium_Int32(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateInt32Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Medium_Int32(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaInt32Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Int64
func BenchmarkMean_Medium_Int64(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateInt64Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Medium_Int64(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaInt64Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Int64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Uint8
func BenchmarkMean_Medium_Uint8(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateUint8Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Uint8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Medium_Uint8(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaUint8Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Uint8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Uint16
func BenchmarkMean_Medium_Uint16(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateUint16Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Uint16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Medium_Uint16(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaUint16Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Uint16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Uint32
func BenchmarkMean_Medium_Uint32(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateUint32Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Uint32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Medium_Uint32(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaUint32Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Uint32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Uint64
func BenchmarkMean_Medium_Uint64(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateUint64Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Uint64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Medium_Uint64(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaUint64Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Uint64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Float32
func BenchmarkMean_Medium_Float32(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateFloat32Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Float32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Medium_Float32(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaFloat32Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Float32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Float64
func BenchmarkMean_Medium_Float64(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateFloat64Data(1_000_000)
	defer data.Release()

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Medium_Float64(b *testing.B) {
	// Generate a medium dataset (1,000,000 elements)
	data := generateGotaFloat64Data(1_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(1_000_000 * arrow.Float64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Mean benchmarks for Large size
// Int8
func BenchmarkMean_Large_Int8(b *testing.B) {
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
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Large_Int8(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateGotaInt8Data(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Int8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Int16
func BenchmarkMean_Large_Int16(b *testing.B) {
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
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Large_Int16(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateGotaInt16Data(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Int16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Int32
func BenchmarkMean_Large_Int32(b *testing.B) {
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
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Large_Int32(b *testing.B) {
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
		_ = data.Mean()
	}
}

// Int64
func BenchmarkMean_Large_Int64(b *testing.B) {
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
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Large_Int64(b *testing.B) {
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
		_ = data.Mean()
	}
}

// Uint8
func BenchmarkMean_Large_Uint8(b *testing.B) {
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
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Large_Uint8(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateGotaUint8Data(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Uint8SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Uint16
func BenchmarkMean_Large_Uint16(b *testing.B) {
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
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Large_Uint16(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateGotaUint16Data(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Uint16SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Uint32
func BenchmarkMean_Large_Uint32(b *testing.B) {
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
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Large_Uint32(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateGotaUint32Data(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Uint32SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Uint64
func BenchmarkMean_Large_Uint64(b *testing.B) {
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
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Large_Uint64(b *testing.B) {
	// Skip this benchmark in short mode
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	// Generate a large dataset (100,000,000 elements)
	data := generateGotaUint64Data(100_000_000)

	// Set the number of bytes processed for throughput calculation
	b.SetBytes(int64(100_000_000 * arrow.Uint64SizeBytes))

	// Reset the timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = data.Mean()
	}
}

// Float32
func BenchmarkMean_Large_Float32(b *testing.B) {
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
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Large_Float32(b *testing.B) {
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
		_ = data.Mean()
	}
}

// Float64
func BenchmarkMean_Large_Float64(b *testing.B) {
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
		result, err := data.Mean()
		if err != nil {
			b.Fatal(err)
		}
		result.Release()
	}
}

func BenchmarkGotaMean_Large_Float64(b *testing.B) {
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
		_ = data.Mean()
	}
}
