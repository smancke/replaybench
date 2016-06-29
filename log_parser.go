package main

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type LogEntry struct {
	Clientip      string
	Verb          string
	Request       string
	Httpversion   string
	Response      int
	ContentType   string
	CorrelationId string
	Timestamp     time.Time `json:"@timestamp"`
	Replay        struct {
		DurationMs   int
		Error        bool
		ErrorMessage string
		Offset       time.Duration
	}
}

var positionRegexp = map[string]string{
	"Clientip":    `^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$`,
	"Verb":        `^HEAD|GET|POST|PUT|PATCH|UPGRADE$`,
	"Request":     `^(http(s?):\/\/[.:a-zA-Z0-9-]*)?/.*`,
	"Httpversion": `^HTTP\/[0-9]\.[0-9]$`,
	"Response":    `^[2-5][0-9][0-9]$`,
}

var timePatterns = []string{
	"02/Jan/2006:15:04:05 -0700",
	"2006-01-02T15:04:05-0700",
	"02/Jan/2006:15:04:05",
}

var trimChars = `"[]`

type LogParser struct {
	positions   map[string]int
	timePattern string
}

func NewLogParser() *LogParser {
	return &LogParser{
		positions: make(map[string]int),
	}
}

func (parser *LogParser) ConfigureByExample(line string) error {
	fields := strings.Split(line, " ")
	for i, v := range fields {
		fields[i] = strings.Trim(v, trimChars)
	}

	for attribute, regex := range positionRegexp {
		var err error
		parser.positions[attribute], err = getPosFor(fields, regex)
		if err != nil {
			return fmt.Errorf("can not find position for %q in ine %v: %v", attribute, line, err)
		}
	}

	var err error
	parser.positions["Timestamp"], parser.timePattern, err = getPosAndPatternForTime(fields)
	if err != nil {
		return fmt.Errorf("can not find position for Timestamp in ine %v: %v", line, err)
	}

	return nil
}

func (parser *LogParser) ParseEntry(line string) (*LogEntry, error) {
	fields := strings.Split(line, " ")

	l := &LogEntry{}
	for field, pos := range parser.positions {
		if pos >= len(fields) {
			return nil, fmt.Errorf("line does not have index %v for field %v: %v", pos, field, line)
		}
		value := strings.Trim(fields[pos], trimChars)

		lV := reflect.ValueOf(l).Elem()
		fieldV := lV.FieldByName(field)
		if !fieldV.IsValid() {
			return nil, fmt.Errorf("can not set field %v in struct LogEntry", field)
		}
		switch fieldV.Kind() {
		case reflect.Int:
			i, err := strconv.Atoi(value)
			if err != nil {
				return nil, err
			}
			fieldV.SetInt(int64(i))
		case reflect.String:
			fieldV.SetString(value)
		case reflect.Struct:
			if fieldV.Type() == reflect.TypeOf(time.Time{}) {
				if t, err := time.Parse(parser.timePattern, value); err != nil {
					return nil, fmt.Errorf("error parsing timestamp in %e: %err", line, err)
				} else {
					fieldV.Set(reflect.ValueOf(t))
				}
			} else {
				return nil, fmt.Errorf("unsupported type %q (%v) for %q", fieldV.Kind(), fieldV.Type(), field)
			}
		default:
			return nil, fmt.Errorf("unsupported type %q for %q", fieldV.Kind(), field)
		}
	}

	return l, nil
}

func getPosFor(fields []string, regex string) (int, error) {
	r := regexp.MustCompile(regex)
	for i, v := range fields {
		if r.MatchString(v) {
			return i, nil
		}
	}
	return -1, errors.New("no field found for " + regex)
}

func getPosAndPatternForTime(fields []string) (int, string, error) {
	for i, v := range fields {
		for _, timePattern := range timePatterns {
			if _, err := time.Parse(timePattern, v); err == nil {
				return i, timePattern, nil
			}
		}
	}
	return -1, "", fmt.Errorf("no time field found for %q", timePatterns)
}
