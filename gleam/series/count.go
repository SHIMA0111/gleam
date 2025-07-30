package series

import (
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

func (s *Series) Count() (*Series, error) {
	count, err := s.count()
	if err != nil {
		return nil, err
	}

	scl := scalar.NewInt64Scalar(count)
	newArray, err := scalar.MakeArrayFromScalar(scl, 1, s.mem)
	if err != nil {
		return nil, err
	}
	defer newArray.Release()

	return NewSeries(s.name, newArray), nil
}

func (s *Series) count() (int64, error) {
	return int64(s.array.Len() - s.array.NullN()), nil
}
