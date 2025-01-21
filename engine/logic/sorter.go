package logic

type SortConf[SortType any] interface {
	FromBytes([]byte) SortType
	ToBytes(a SortType) []byte // ToBytes used for marshaling with gob
	Compare(a, b SortType) int
}

type Sorter[T any] interface {
	Add(T)
	Sorted() chan T
	Close() error
}
