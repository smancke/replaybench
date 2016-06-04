package main

import (
	"fmt"
	"io"
	"sync"
)

type CountProcessor struct {
	mutex  *sync.Mutex
	counts map[string]int
}

func NewCountProcessor() *CountProcessor {
	return &CountProcessor{
		mutex:  &sync.Mutex{},
		counts: make(map[string]int),
	}
}

func (cp *CountProcessor) Process(l *LogEntry) error {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if l.ContentType != "ignore" {
		cp.counts[l.ContentType+" "+l.Verb+" "+l.Path]++
	}
	return nil
}

func (cp *CountProcessor) PrintResults(w io.Writer) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	for k, v := range cp.counts {
		fmt.Fprintf(w, "%v %v\n", v, k)
	}
}
