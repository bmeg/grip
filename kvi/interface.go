package kvi

// KVBuilder is function implemented by the various key/value storage drivers
// that returns an initialized KVInterface given a file/path argument
type KVBuilder func(path string) (KVInterface, error)

// KVInterface is the base interface for key/value based graph driver
type KVInterface interface {
	HasKey(key []byte) bool
	Set(key, value []byte) error
	DeletePrefix(prefix []byte) error
	Delete(key []byte) error

	View(func(it KVIterator) error) error
	Update(func(tx KVTransaction) error) error
	Close() error
}

// KVIterator is a genetic interface used by KVInterface.View to allow the
// KVGraph to scan the values stored in the key value driver
type KVIterator interface {
	Seek(k []byte) error
	Valid() bool
	Key() []byte
	Value() ([]byte, error)
	Next() error

	Get(key []byte) ([]byte, error)
}

// KVTransaction is a generic interface used by KVInterface.Update to allow the
// KVGraph to alter the values stored in the key value driver
type KVTransaction interface {
	HasKey(key []byte) bool
	Set(key, value []byte) error
	Delete(key []byte) error
}
