package utils

type CompareOperand string

const (
	Equal        CompareOperand = "equal"
	NotEqual     CompareOperand = "not_equal"
	Greater      CompareOperand = "greater"
	GreaterEqual CompareOperand = "greater_equal"
	Less         CompareOperand = "less"
	LessEqual    CompareOperand = "less_equal"
)

func (o CompareOperand) String() string {
	return string(o)
}
