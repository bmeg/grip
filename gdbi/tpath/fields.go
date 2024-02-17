package tpath

func IsGraphField(f string) bool {
	return f == "_gid" || f == "_label" || f == "_to" || f == "_from"
}
