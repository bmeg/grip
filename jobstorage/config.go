package jobstorage

type OpenSearchConfig struct {
	Address  string
	Username string
	Password string
}

type JobsConfig struct {
	File       string
	OpenSearch *OpenSearchConfig
}
