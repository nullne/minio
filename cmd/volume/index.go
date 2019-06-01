package volume

type Index interface {
	Get(key string) (fi FileInfo, err error)
	Set(key string, fi FileInfo) error
	Delete(key string) error
	StatDir(key string) (fi FileInfo, err error)
	ListN(key string, count int) ([]string, error)
	Close() error
	Remove() error
}
