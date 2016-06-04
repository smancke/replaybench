package main

import (
	"encoding/json"
	"net"
)

type LogstashProcessor struct {
	conn net.Conn
}

func NewLogstashProcessor(hostPort string) (*LogstashProcessor, error) {
	conn, err := net.Dial("udp", hostPort)
	return &LogstashProcessor{
		conn: conn,
	}, err
}

func (cp *LogstashProcessor) Process(l *LogEntry) error {
	if l.ContentType != "ignore" {
		if js, err := json.Marshal(l); err != nil {
			return err
		} else {
			if _, err := cp.conn.Write(js); err != nil {
				return err
			}
		}
	}
	return nil
}
