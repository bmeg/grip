package gripper

// Config is the federation entry in the global GRIP config file. It builds a
// graph out of the resource nodes listed in the top level `Sources` map.
type Config struct {
	// Graph is the name the federated graph is served under.
	Graph string
	// Sources lists the names of the sources that make up the graph. Empty means all registered sources.
	Sources []string
}
