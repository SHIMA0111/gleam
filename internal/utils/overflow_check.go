package utils

import (
	"math"
	"strconv"
)

func IsIntOverflow(value int64, distTypeBits int8) bool {
	switch distTypeBits {
	case 8:
		if value > math.MaxInt8 || value < math.MinInt8 {
			return true
		}

		return false
	case 16:
		if value > math.MaxInt16 || value < math.MinInt16 {
			return true
		}

		return false
	case 32:
		if value > math.MaxInt32 || value < math.MinInt32 {
			return true
		}

		return false
	case 64:
		return false
	default:
		panic("unsupported int bit width: " + strconv.Itoa(int(distTypeBits)))
	}
}

func IsUintOverflow(value uint64, distTypeBits int8) bool {
	switch distTypeBits {
	case 8:
		if value > math.MaxUint8 {
			return true
		}

		return false
	case 16:
		if value > math.MaxUint16 {
			return true
		}

		return false
	case 32:
		if value > math.MaxUint32 {
			return true
		}

		return false
	case 64:
		return false
	default:
		panic("unsupported uint bit width: " + strconv.Itoa(int(distTypeBits)))
	}
}

func IsFloatOverflow(value float64, distTypeBits int8) bool {
	switch distTypeBits {
	case 32:
		if value > math.MaxFloat32 {
			return true
		}

		return false
	case 64:
		return false
	default:
		panic("unsupported float bit width: " + strconv.Itoa(int(distTypeBits)))
	}
}
