package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/blakesmith/go-grok/grok"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"strings"
	"time"
)

type Args struct {
	LogFiles    []string `arg:"positional,help: The logfiles to replay"`
	Verbose     bool     `arg:"-v,help: More verbose output"`
	ShowErrors  bool     `arg:"--show-errors,help: Show errors"`
	Limit       int      `arg:"--limit,help: Only process the first LIMIT lines"`
	PatternFile string   `arg:"--pattern-file,help: The configuration file with grok patterns"`
	PatternName string   `arg:"--pattern-name,help: The name of the pattern to parse the log lines"`
	RegexIgnore string   `arg:"--regex-ignore,help: Pattern for lines to ignore (matched against the request)"`
	RegexAssets string   `arg:"--regex-asset,help: Pattern for lines of type asset (matched against the request)"`
	RegexAjax   string   `arg:"--regex-ajax,help: Pattern for lines of type ajax (matched against the request)"`
	RegexSearch string   `arg:"--regex-search,help: Pattern for lines of type search (matched against the request)"`
}

type LogEntry struct {
	Host         string
	Loadbalancer string
	Clientip     string
	Ident        string
	Auth         string
	Time         string
	Verb         string
	Request      string
	Httpversion  string
	Response     string
	Bytes        string
	Referrer     string
	Agent        string
	ContentType  string
	Path         string
	Timestamp    time.Time `json:"@timestamp"`
	//Timestamp    int64 `json:"@timestamp"`
}

type Processor interface {
	Process(l *LogEntry) error
}

var g *grok.Grok
var args *Args
var RegexIgnore, RegexAssets, RegexAjax, RegexSearch *regexp.Regexp

func main() {
	args = &Args{
		ShowErrors:  false,
		Limit:       math.MaxInt32,
		PatternFile: "./patterns",
		PatternName: "LOG",
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

	processors := CompoundProcessor{
		NewElasticsearchIndexer("http://127.0.0.1:9200"),
		//NewCountProcessor(),
	}

	count, ignoreCount, errorCount := 0, 0, 0
	if len(args.LogFiles) > 0 {
		for _, fileName := range args.LogFiles {
			file, err := os.Open(fileName)
			if err != nil {
				panic(err)
			}
			defer file.Close()

			var in io.Reader

			if strings.HasSuffix(fileName, ".gz") {
				gz, err := gzip.NewReader(file)
				if err != nil {
					panic(err)
				}
				defer gz.Close()
				in = gz
			} else {
				in = file
			}
			fmt.Fprintf(os.Stderr, "reading from: %v\n", fileName)
			c, ic, ec := read(in, processors)
			count += c
			ignoreCount += ic
			errorCount += ec
		}
	} else {
		fmt.Fprintf(os.Stderr, "reading from stdin\n")

		c, ic, ec := read(os.Stdin, processors)
		count += c
		ignoreCount += ic
		errorCount += ec
	}
	processors.PrintResults(os.Stdout)

	fmt.Fprintf(os.Stderr, "Processed: %v\n", count)
	fmt.Fprintf(os.Stderr, "Ignored: %v\n", ignoreCount)
	fmt.Fprintf(os.Stderr, "Errors: %v\n", errorCount)

	if err := processors.Finish(time.Second * 10); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err.Error())
	} else {
		fmt.Fprintf(os.Stderr, "done.\n")
	}

}

func read(reader io.Reader, processor Processor) (count, ignoreCount, errorCount int) {

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		if count+ignoreCount+errorCount >= args.Limit {
			return count, ignoreCount, errorCount
		}
		line := scanner.Text()

		l, err := parseEntry(line)
		if err != nil {
			if args.ShowErrors {
				log.Println(err)
			}
			errorCount++
			continue
		}
		err = calculateFields(l)
		if err != nil {
			if args.ShowErrors {
				log.Println(err)
			}
			errorCount++
			continue
		}
		if l.ContentType == "ignore" {
			ignoreCount++
			continue
		}
		if args.Verbose {
			fmt.Printf("\n%v\n%+v\n", line, l)
		}
		//fmt.Printf("%v %v %v\n", l.verb, l.ContentType, l.path)
		if err := processor.Process(l); err != nil {
			panic(err)
		}
		count++
		total := count + ignoreCount + errorCount
		if total%10000 == 0 {
			fmt.Fprintf(os.Stderr, "%v entries\n", total)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
	return count, ignoreCount, errorCount
}

func parseEntry(line string) (*LogEntry, error) {
	m := g.Match(line)
	if m == nil {
		return nil, errors.New("can not parse: " + line)
	}
	c := m.Captures()

	l := &LogEntry{
		Host:         getFirst(c, "host"),
		Loadbalancer: getFirst(c, "loadbalancer"),
		Clientip:     getFirst(c, "clientip"),
		Ident:        getFirst(c, "ident"),
		Auth:         getFirst(c, "auth"),
		Time:         getFirst(c, "time"),
		Verb:         getFirst(c, "verb"),
		Request:      getFirst(c, "request"),
		Httpversion:  getFirst(c, "httpversion"),
		Response:     getFirst(c, "response"),
		Bytes:        getFirst(c, "bytes"),
		Referrer:     getFirst(c, "referrer"),
		Agent:        getFirst(c, "agent"),
	}
	return l, nil
}

func calculateFields(l *LogEntry) error {
	parts := strings.SplitN(l.Request, "?", 2)
	l.Path = parts[0]

	if RegexIgnore.MatchString(l.Request) {
		l.ContentType = "ignore"
	} else if RegexAssets.MatchString(l.Request) {
		l.ContentType = "asset"
	} else if RegexSearch.MatchString(l.Request) {
		l.ContentType = "search"
	} else if RegexAjax.MatchString(l.Request) {
		l.ContentType = "ajax"
	} else {
		l.ContentType = "page"
	}

	if time, err := time.Parse("02/Jan/2006:15:04:05 -0700", l.Time); err == nil {
		l.Timestamp = time
	} else {
		return err
	}
	return nil
}

func getFirst(captures map[string][]string, key string) string {
	for k, v := range captures {
		if strings.HasSuffix(k, ":"+key) && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}
