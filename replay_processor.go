package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type ReplayProcessor struct {
	baseURL        string
	fanout         chan *LogEntry
	userSimulation map[string]*UserSimulation
	mux            *sync.Mutex
	shouldFinish   chan bool
	workerDone     chan bool
	log            Processor
}

func NewReplayProcessor(baseURL string, log Processor) *ReplayProcessor {
	rp := &ReplayProcessor{
		baseURL:        baseURL,
		fanout:         make(chan *LogEntry, 100),
		userSimulation: make(map[string]*UserSimulation),
		mux:            &sync.Mutex{},
		shouldFinish:   make(chan bool),
		workerDone:     make(chan bool),
		log:            log,
	}
	go rp.startWorker()
	return rp
}

func (rp *ReplayProcessor) Process(l *LogEntry) error {
	rp.fanout <- l
	return nil
}

func (rp *ReplayProcessor) startWorker() {
loop:
	for {
		select {
		case l := <-rp.fanout:
			if l.ContentType == "ignore" {
				continue
			}
			us := rp.getUserSimulation(l.Clientip)
			us.Process(l)
		case <-rp.shouldFinish:
			break loop
		}
	}
	rp.workerDone <- true
}

func (rp *ReplayProcessor) getUserSimulation(clientIp string) *UserSimulation {
	rp.mux.Lock()
	defer rp.mux.Unlock()

	us, exist := rp.userSimulation[clientIp]
	if !exist {
		fmt.Fprintf(os.Stderr, "started user simulation %v\n", clientIp)
		us = newUserSimulation(rp.baseURL, rp.log)
		rp.userSimulation[clientIp] = us
		// cleanup old
		for k, v := range rp.userSimulation {
			if !v.IsActive() {
				v.Finish()
				fmt.Fprintf(os.Stderr, "closed user simulation %v\n", k)
				delete(rp.userSimulation, k)
			} // maybe do some statistics, here?
		}
	}
	return us
}

func (rp *ReplayProcessor) Finish() chan bool {
	done := make(chan bool)
	go func() {
		for len(rp.fanout) > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		close(rp.shouldFinish) // close does a broadcast
		for _, v := range rp.userSimulation {
			simC := v.Finish()
			<-simC
		}
		done <- true
	}()
	return done
}
