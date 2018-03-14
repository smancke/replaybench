package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type UserSimulation struct {
	baseURL      string
	username     string
	password     string
	fanout       chan *LogEntry
	mux          *sync.Mutex
	shouldFinish chan bool
	workerDone   []chan bool
	log          Processor
	lastAction   time.Time
}

var redirectError = errors.New("redirect")

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func newUserSimulation(baseURL string, log Processor, username, password string) *UserSimulation {
	us := &UserSimulation{
		baseURL:      baseURL,
		username:     username,
		password:     password,
		fanout:       make(chan *LogEntry, 10),
		mux:          &sync.Mutex{},
		shouldFinish: make(chan bool),
		workerDone:   make([]chan bool, 6),
		log:          log,
		lastAction:   time.Now(),
	}
	for i, _ := range us.workerDone {
		us.workerDone[i] = make(chan bool)
		go us.startWorker(us.shouldFinish, us.workerDone[i])
	}
	return us
}

func (us *UserSimulation) Process(l *LogEntry) error {
	if l.Verb != "GET" {
		l.wg.Done()
		return nil
	}
	us.fanout <- l
	return nil
}

func (us *UserSimulation) IsActive() bool {
	us.mux.Lock()
	defer us.mux.Unlock()
	return len(us.fanout) > 0 || time.Since(us.lastAction) < time.Second*30
}

func (us *UserSimulation) doCall(client *http.Client, l *LogEntry) {
	us.UpdateLastAction()

	l.Timestamp = time.Now()
	l.CorrelationId = "rep-" + randStringBytes(10)

	url := us.baseURL + l.Request
	request, err := http.NewRequest("GET", url, nil)
	request.Header.Set("X-Correlation-Id", l.CorrelationId)
	if us.username != "" {
		request.SetBasicAuth(us.username, us.password)
	}
	if err != nil {
		l.Replay.Error = true
		l.Replay.ErrorMessage = err.Error()
		return
	}
	resp, err := client.Do(request)
	if err != nil && !(err == redirectError && (l.Response == 301 || l.Response == 302 || l.Response == 303)) {
		l.Replay.Error = true
		l.Replay.ErrorMessage = fmt.Sprintf("expected %v, but got redirect: %e", l.Response, err)
		return
	}
	ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	defer us.UpdateLastAction()

	l.Replay.ErrorMessage = fmt.Sprintf("%v", resp.StatusCode)
	l.Replay.DurationMs = int(time.Since(l.Timestamp).Nanoseconds() / 1000000)
	if resp.StatusCode != l.Response {
		l.Replay.Error = true
		l.Replay.ErrorMessage = fmt.Sprintf("Wrong status returned: %v (expected: %v)", resp.StatusCode, l.Response)
		return
	}
}

func (us *UserSimulation) startWorker(shouldFinishC, done chan bool) {
	client := &http.Client{Timeout: time.Second * 10}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return redirectError
	}
loop:
	for {
		select {
		case l := <-us.fanout:
			if l.ContentType == "ignore" {
				l.wg.Done()
				continue
			}
			us.doCall(client, l)
			l.wg.Done()
		case <-shouldFinishC:
			break loop
		}
	}
	done <- true
}

func (us *UserSimulation) UpdateLastAction() {
	us.mux.Lock()
	defer us.mux.Unlock()
	us.lastAction = time.Now()
}

func (us *UserSimulation) Finish() chan bool {
	done := make(chan bool)
	go func() {
		for len(us.fanout) > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		close(us.shouldFinish) // close does a broadcast
		for _, c := range us.workerDone {
			<-c
		}
		done <- true
	}()
	return done
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
