package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var textLogLine = `www.example.org 42.24.424.24 2016-05-29T13:00:00+0200 "GET http://www.example.org/foo/bar/bazz.pdf HTTP/1.1" 206 65536 "https://www.google.de" "Mozilla/5.0 (Windows NT 6.1; rv:46.0) Gecko/20100101 Firefox/46.0" 0.000142 hit  hit`

func Test_ParseTest(t *testing.T) {
	a := assert.New(t)

	parser := NewLogParser()
	err := parser.ConfigureByExample(textLogLine)
	a.NoError(err)

	l, err := parser.ParseEntry(textLogLine)
	a.NoError(err)

	a.Equal("42.24.424.24", l.Clientip)
	a.Equal("2016-05-29T13:00:00+0200", l.Timestamp.Format("2006-01-02T15:04:05-0700"))
	a.Equal("GET", l.Verb)
	a.Equal("http://www.example.org/foo/bar/bazz.pdf", l.Request)
	a.Equal("HTTP/1.1", l.Httpversion)
	a.Equal(206, l.Response)
}

func Test_getPosAndPatternForTime(t *testing.T) {
	a := assert.New(t)

	i, _, err := getPosAndPatternForTime([]string{"foo", "2016-05-29T13:00:00+0200", "bar"})
	a.NoError(err)
	a.Equal(1, i)

	i, _, err = getPosAndPatternForTime([]string{"foo", "2018-02-22T08:27:14Z", "bar"})
	a.NoError(err)
	a.Equal(1, i)

	i, _, err = getPosAndPatternForTime([]string{"foo", "29/May/2016:16:23:08 +0200", "bar"})
	a.NoError(err)
	a.Equal(1, i)
}
