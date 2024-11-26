package provider

type QueryExecutor interface {
	QueryKeys(query string) ([]string, error)
	Execute(query string, reads map[string]string) (string, map[string]string, error)
}
