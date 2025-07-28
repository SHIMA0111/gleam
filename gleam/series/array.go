package series

import "github.com/apache/arrow-go/v18/arrow"

type Numeric interface {
	~int8 | ~int16 | ~int32 | ~int64 |
		~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

type NumericArray[T Numeric] interface {
	arrow.Array
	Value(i int) T
}

type Integer interface {
	~int8 | ~int16 | ~int32 | ~int64
}

type IntegerArray[T Integer] interface {
	arrow.Array
	Value(i int) T
}

type UnsignedInteger interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

type UnsignedIntegerArray[T UnsignedInteger] interface {
	arrow.Array
	Value(i int) T
}

type Float interface {
	~float32 | ~float64
}

type FloatArray[T Float] interface {
	arrow.Array
	Value(i int) T
}
