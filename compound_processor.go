package main

import (
	"errors"
	"io"
	"time"
)

type ResultPrinter interface {
	PrintResults(w io.Writer)
}

type Finisher interface {
	Finish() chan bool
}

type CompoundProcessor []Processor

func (cp CompoundProcessor) Process(l *LogEntry) error {
	for _, p := range cp {
		if err := p.Process(l); err != nil {
			return err
		}
	}
	return nil
}

func (cp CompoundProcessor) PrintResults(w io.Writer) {
	for _, p := range cp {
		printer, ok := p.(ResultPrinter)
		if ok {
			printer.PrintResults(w)
		}
	}
}

func (cp CompoundProcessor) Finish(timeout time.Duration) error {
	finisher := make([]chan bool, 0)
	for _, p := range cp {
		fi, ok := p.(Finisher)
		if ok {
			finisher = append(finisher, fi.Finish())
		}
	}

	timer := time.After(timeout)
	for _, okChan := range finisher {
		select {
		case <-okChan:
			// terminated, fine!
		case <-timer:
			return errors.New("error: not all jobs terminated")
		}
	}
	return nil
}
