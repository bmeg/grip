package merge

type GraphConfig struct {
	Prefix string
	Graph  string
}

type Config struct {
	Graphs map[string]GraphConfig
}
