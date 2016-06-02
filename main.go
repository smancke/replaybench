package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/blakesmith/go-grok/grok"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
)

type Args struct {
	LogFiles    []string `arg:"positional,help: The logfiles to replay"`
	Verbose     bool     `arg:"-v,help: More verbose output"`
	PatternFile string   `arg:"--pattern-file,help: The configuration file with grok patterns"`
	PatternName string   `arg:"--pattern-name,help: The name of the pattern to parse the log lines"`
	RegexIgnore string   `arg:"--regex-ignore,help: Pattern for lines to ignore (matched against the request)"`
	RegexAssets string   `arg:"--regex-asset,help: Pattern for lines of type asset (matched against the request)"`
	RegexAjax   string   `arg:"--regex-ajax,help: Pattern for lines of type ajax (matched against the request)"`
	RegexSearch string   `arg:"--regex-search,help: Pattern for lines of type search (matched against the request)"`
}

type LogEntry struct {
	host         string
	loadbalancer string
	clientip     string
	ident        string
	auth         string
	timestamp    string
	verb         string
	request      string
	httpversion  string
	response     string
	bytes        string
	referrer     string
	agent        string
	content_type string
	path         string
}

var g *grok.Grok
var args *Args
var RegexIgnore, RegexAssets, RegexAjax, RegexSearch *regexp.Regexp

func main() {
	args = &Args{
		PatternFile: "./patterns",
		PatternName: "COMBINEDAPACHELOG",
		RegexIgnore: `healthcheck`,
		RegexAssets: `\.jpg|\.jpeg|\.png|\.ico|\.css|\.js|\.svg|\.gif|\.pdf`,
		RegexAjax:   `jsonp_callback|\.json`,
		RegexSearch: `\?q=|\&q=`,
	}
	arg.MustParse(args)

	RegexIgnore = regexp.MustCompile(args.RegexIgnore)
	RegexAssets = regexp.MustCompile(args.RegexAssets)
	RegexAjax = regexp.MustCompile(args.RegexAjax)
	RegexSearch = regexp.MustCompile(args.RegexSearch)

	g = grok.New()
	defer g.Free()
	if err := g.AddPatternsFromFile(args.PatternFile); err != nil {
		panic(err)
	}
	if err := g.Compile("%{" + args.PatternName + "}"); err != nil {
		panic(err)
	}

	counts := make(map[string]int)
	countProcessor := func(l *LogEntry) {
		if l.content_type != "ignore" {
			counts[l.content_type+" "+l.verb+" "+l.path]++
			if l.content_type == "ajax" {
				fmt.Printf("%v %v %v\n", l.verb, l.content_type, l.path)
			}

		}
	}
	if len(args.LogFiles) > 0 {
		for _, fileName := range args.LogFiles {
			file, err := os.Open(fileName)
			if err != nil {
				panic(err)
			}
			defer file.Close()
			read(file, countProcessor)
		}
	} else {
		read(os.Stdin, countProcessor)
	}

	for k, v := range counts {
		fmt.Printf("%v %v\n", v, k)
	}
}

func read(reader io.Reader, processor func(*LogEntry)) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()

		l, err := parseEntry(line)
		if err != nil {
			if args.Verbose {
				log.Println(err)
			}
			continue
		}
		calculateFields(l)
		if args.Verbose {
			fmt.Printf("\n%v\n%+v\n", line, l)
		}
		//fmt.Printf("%v %v %v\n", l.verb, l.content_type, l.path)
		processor(l)
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

func parseEntry(line string) (*LogEntry, error) {
	m := g.Match(line)
	if m == nil {
		return nil, errors.New("can not parse: " + line)
	}
	c := m.Captures()

	l := &LogEntry{
		host:         getFirst(c, "host"),
		loadbalancer: getFirst(c, "loadbalancer"),
		clientip:     getFirst(c, "clientip"),
		ident:        getFirst(c, "ident"),
		auth:         getFirst(c, "auth"),
		timestamp:    getFirst(c, "timestamp"),
		verb:         getFirst(c, "verb"),
		request:      getFirst(c, "request"),
		httpversion:  getFirst(c, "httpversion"),
		response:     getFirst(c, "response"),
		bytes:        getFirst(c, "bytes"),
		referrer:     getFirst(c, "referrer"),
		agent:        getFirst(c, "agent"),
	}
	return l, nil
}

func calculateFields(l *LogEntry) {
	parts := strings.SplitN(l.request, "?", 2)
	l.path = parts[0]

	if RegexIgnore.MatchString(l.request) {
		l.content_type = "ignore"
	} else if RegexAssets.MatchString(l.request) {
		l.content_type = "asset"
	} else if RegexSearch.MatchString(l.request) {
		l.content_type = "search"
	} else if RegexAjax.MatchString(l.request) {
		l.content_type = "ajax"
	} else {
		l.content_type = "page"
	}

}

func getFirst(captures map[string][]string, key string) string {
	for k, v := range captures {
		if strings.HasSuffix(k, ":"+key) && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}
