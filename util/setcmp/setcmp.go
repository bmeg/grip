package setcmp

func ArrayEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func ArrayIntEq(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func ContainsString(a []string, v string) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}

func ContainsInt(a []int, v int) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}

func ContainsUint(a []uint64, v uint64) bool {
	for _, i := range a {
		if i == v {
			return true
		}
	}
	return false
}
