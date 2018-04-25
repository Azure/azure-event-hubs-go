package ehmath

// Max provides an integer function for math.Max
func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
