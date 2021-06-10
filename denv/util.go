package denv

func Max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func Min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}