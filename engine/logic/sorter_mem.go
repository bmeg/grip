package logic

import "slices"

type MemSorter[T any] struct {
	data []T
	conf SortConf[T]
}

// Add implements Sorter.
func (m *MemSorter[T]) Add(value T) {
	m.data = append(m.data, value)
}

// Close implements Sorter.
func (m *MemSorter[T]) Close() error {
	return nil
}

// Sorted implements Sorter.
func (m *MemSorter[T]) Sorted() chan T {
	slices.SortFunc(m.data, m.conf.Compare)
	out := make(chan T)
	go func() {
		defer close(out)
		for _, i := range m.data {
			out <- i
		}
	}()
	return out
}

func NewMemSorter[T any](conf SortConf[T]) Sorter[T] {
	o := make([]T, 0, 10)
	return &MemSorter[T]{o, conf}
}
