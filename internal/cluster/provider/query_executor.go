package provider

import "github.com/eqimd/accord/internal/common"

type QueryExecutor interface {
	QueryKeys(query string) (common.Set[string], error)
	Execute(query string, reads map[string]string) (string, map[string]string, error)
}
