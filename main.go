package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"github.com/alexflint/go-arg"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Args struct {
	LogFiles    []string `arg:"positional,help: The logfiles to replay"`
	Verbose     bool     `arg:"-v,help: More verbose output"`
	ShowErrors  bool     `arg:"--show-errors,help: Show errors"`
	Limit       int      `arg:"--limit,help: Only process the first LIMIT lines"`
	RegexIgnore string   `arg:"--regex-ignore,help: Pattern for lines to ignore (matched against the request)"`
	RegexAssets string   `arg:"--regex-asset,help: Pattern for lines of type asset (matched against the request)"`
	RegexAjax   string   `arg:"--regex-ajax,help: Pattern for lines of type ajax (matched against the request)"`
	RegexSearch string   `arg:"--regex-search,help: Pattern for lines of type search (matched against the request)"`
	BaseUrl     string   `arg:"--base-url,help: The base url to call"`
	Username    string   `arg:"--username,help: Http Basic Auth Username"`
	Password    string   `arg:"--password,help: Http Basic Auth Password"`
	EsURL       string   `arg:"--es-url,help: The url of elasticsearch"`
}

var urlHostRegexp = regexp.MustCompile(`http(s?):\/\/[.:a-zA-Z0-9-]*`)

type Processor interface {
	Process(l *LogEntry) error
}

var args *Args
var RegexIgnore, RegexAssets, RegexAjax, RegexSearch *regexp.Regexp

func main() {
	args = &Args{
		ShowErrors:  false,
		Limit:       math.MaxInt32,
		RegexIgnore: `healthcheck`,
		RegexAssets: `\.jpg|\.jpeg|\.png|\.ico|\.css|\.js|\.svg|\.gif|\.pdf|\.xml|\.woff|\.eot`,
		RegexAjax:   `jsonp_callback|\.json`,
		RegexSearch: `\?q=|\&q=`,
		BaseUrl:     "http://127.0.0.1",
		Username:    "",
		Password:    "",
		EsURL:       "http://127.0.0.1:9200",
	}
	arg.MustParse(args)

	RegexIgnore = regexp.MustCompile(args.RegexIgnore)
	RegexAssets = regexp.MustCompile(args.RegexAssets)
	RegexAjax = regexp.MustCompile(args.RegexAjax)
	RegexSearch = regexp.MustCompile(args.RegexSearch)

	indexer := NewElasticsearchIndexer(args.EsURL)
	processors := CompoundProcessor{
		NewReplayProcessor(args.BaseUrl, indexer, args.Username, args.Password),
		indexer,
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

	if err := processors.Finish(time.Second * 100); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err.Error())
	} else {
		fmt.Fprintf(os.Stderr, "done.\n")
	}
}

func read(reader io.Reader, processor Processor) (count, ignoreCount, errorCount int) {
	parser := NewLogParser()
	initialized := false

	offset := time.Duration(0)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		if count+ignoreCount+errorCount >= args.Limit {
			return count, ignoreCount, errorCount
		}
		line := scanner.Text()

		if !initialized {
			err := parser.ConfigureByExample(line)
			if err != nil {
				panic(err)
			}
			initialized = true
		}

		l, err := parser.ParseEntry(line)
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

		if offset == time.Duration(0) {
			offset = time.Since(l.Timestamp)
		}
		// don't be fastster than the log
		for time.Since(l.Timestamp) < offset {
			time.Sleep(time.Millisecond * 100)
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

func calculateFields(l *LogEntry) error {
	l.Request = urlHostRegexp.ReplaceAllString(l.Request, "")

	if RegexIgnore.MatchString(l.Request) || l.Response != 200 {
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

func getFirstInt(captures map[string][]string, key string) int {
	for k, v := range captures {
		if strings.HasSuffix(k, ":"+key) && len(v) > 0 {
			i, _ := strconv.Atoi(v[0])
			return i
		}
	}
	return 0
}
