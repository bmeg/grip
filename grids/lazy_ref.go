package grids

import (
	"sync"

	"github.com/bmeg/grip/gdbi"
)

// lazyElementRef defers row hydration until Get() is called.
type lazyElementRef struct {
	meta   gdbi.DataElement
	loadFn func() *gdbi.DataElement

	once sync.Once
	elem *gdbi.DataElement
}

func (l *lazyElementRef) Identity() *gdbi.DataElement {
	m := l.meta
	return &m
}

func (l *lazyElementRef) Get() *gdbi.DataElement {
	l.once.Do(func() {
		if l.loadFn != nil {
			l.elem = l.loadFn()
		}
	})
	if l.elem != nil {
		return l.elem
	}
	m := l.meta
	return &m
}

func (l *lazyElementRef) Copy() gdbi.DataRef {
	return l
}
