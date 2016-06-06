package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

type ElasticsearchIndexer struct {
	baseurl       string
	fanout        chan *LogEntry
	shouldFinishC chan bool
	workerDone    []chan bool
}

func NewElasticsearchIndexer(baseurl string) *ElasticsearchIndexer {
	for strings.HasSuffix(baseurl, "/") {
		baseurl = baseurl[:len(baseurl)-1]
	}
	ei := &ElasticsearchIndexer{
		baseurl:       baseurl,
		fanout:        make(chan *LogEntry, 100),
		shouldFinishC: make(chan bool),
		workerDone:    make([]chan bool, 4),
	}
	for i, _ := range ei.workerDone {
		ei.workerDone[i] = make(chan bool)
		go ei.startWorker(ei.shouldFinishC, ei.workerDone[i])
	}
	return ei
}

func (ei *ElasticsearchIndexer) Process(l *LogEntry) error {
	ei.fanout <- l
	return nil
}

func (ei *ElasticsearchIndexer) startWorker(shouldFinishC, done chan bool) {
	shouldFinish := false
	for {
		bulkTimeout := time.After(100 * time.Millisecond)
		documentCount := 0
		buff := bytes.NewBufferString("")
	bulkaggregate:
		for documentCount < 1000 {
			select {
			case l := <-ei.fanout:
				if l.ContentType == "ignore" {
					continue
				}

				documentCount++
				if js, err := json.Marshal(l); err != nil {
					fmt.Fprintf(os.Stderr, err.Error())
					continue
				} else {
					buff.WriteString(fmt.Sprintf(`{"index":{"_index": "logstash-%v", "_type": "log"}}`, l.Timestamp.Format("2006-01-02")))
					buff.WriteString("\n")
					buff.WriteString(string(js))
					buff.WriteString("\n")
				}
			case <-bulkTimeout:
				break bulkaggregate
			case <-shouldFinishC:
				shouldFinish = true
				break bulkaggregate
			}
		}
		if documentCount > 0 {
			//start := time.Now()
			resp, err := http.Post(ei.baseurl+"/_bulk", "application/json", buff)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err.Error())
				continue
			}
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				fmt.Fprintf(os.Stderr, "http error %v\n", resp.StatusCode)
				continue
			}
			//fmt.Fprintf(os.Stderr, "bulk with %v documents(took %v)\n", documentCount, time.Since(start))
		}
		if shouldFinish {
			done <- true
			return
		}
	}
}

func (ei *ElasticsearchIndexer) Finish() chan bool {
	done := make(chan bool)
	go func() {
		for len(ei.fanout) > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		close(ei.shouldFinishC) // close does a broadcast
		for _, c := range ei.workerDone {
			<-c
		}
		done <- true
	}()
	return done
}
