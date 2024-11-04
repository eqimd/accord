package provider

type Storage interface {
	Get(key string) (string, error)
	GetBatch(keys []string) ([]string, error)
	Set(key, value string) error
	SetBatch(kv map[string]string) error
	Snapshot() (map[string]string, error)
}
