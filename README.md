replaybench
=============
Golang http benchmark and statistic tool replaying access log files.

WORK in progress: Do not expect a ready solution, here!

Build
---------
Before compile, you have to install the grok build requirements, e.g.:
```
apt-get install libgrok1 libgrok-dev bison ctags flex gperf libevent-dev libpcre3-dev libtokyocabinet-dev
```

Features
---------
The tool should answer the following questions:

- How much user sessions per timeframe
- Number of Requests per user session, split by GET of assets, GET of html, POST
- Call count per timeframe

Reporting in: average, max, 99%
