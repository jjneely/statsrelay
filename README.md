StatsRelay
==========

StatsRelay is a Golang implementation of a proxy or load balancer for Etsy's
excellent nodejs statsd utility.  This utility will listen for UDP metrics
in the same format as Etsy's statsd and forward them to one statsd service
from a pool of available host:port combinations.  Each metric is hashed with
a consistent hashing algorithm so that the same metric is always sent to
the same statsd server.

This is written in Go with a simple goal: Be fast.  Current statsd
distributions come with a nodejs proxy tool that does much the same thing.
However, that proxy daemon is unable to keep up in high-traffic situations.

You can layer statsrelay as needed to build a tree, or run multiple versions
behind a generic UDP load balancer, like IPVS.  Provided the configuration is
the same all statsrelay daemons will route metrics to the same statsd server.

Usage
-----

Command synopsis:

    statsrelay [options] HOST:PORT:INSTANCE [HOST:PORT:INSTANCE ...]

    -b="0.0.0.0": IP Address to listen on
    -bind="0.0.0.0": IP Address to listen on
    -p=9125: Port to listen on
    -port=9125: Port to listen on
    -prefix="statsrelay": The prefix to use with self generated stats
    -metrics-prefix="": The prefix to use with metrics passed through statsrelay
    -metrics-tags="": Metrics tags added at the end of each relayed metric
    -bufsize="32768": Read buffer size
    -packetlen="1400": Max packet length. Must be lower than MTU plus IPv4 and UDP headers to avoid fragmentation.
    -sendproto="UDP": IP Protocol for sending data: TCP, UDP, or TEST
    -tcptimeout="1s": Timeout for TCP client remote connections
    -backoff-retries="3": Maximum number of retries in backoff for TCP dial when sendproto set to TCP
    -backoff-min="50ms": Backoff minimal (integer) time in Millisecond
    -backoff-max="1s": Backoff maximal (integer) time in Millisecond
    -backoff-factor="1.5": Backoff factor (float)
    -pprof=false: Golang profiling support
    -pprof-bind=":8080": Listen host:port for HTTP pprof data
    -dnscache=false: Enable in app DNS cache for resolved TCP sendout sharded endpoints
    -dnscache-time="1s": Time we cache resolved adresses of sharded endpoint
    -dnscache-purge="5s": Time purge stale elements in cache
    -verbose=false: Verbose output

You must specify at least one HOST:PORT combination.  The INSTANCE can be
used to further populate or weight the consistent hashing ring as you see fit.
The instance is stripped before using the HOST and PORT to create a UDP
socket or TCP connection.

Algorithms and Performance
---------------------------

Use Go 1.3 or better.  Go 1.5 is quite a bit faster.

This used to depend on an external consistent hashing algorithm which has
been replaced with an internal implementation of Google's Jump Hash.

   http://arxiv.org/pdf/1406.2294.pdf

FNV1a hashing is used to hash the string values into a long integer.

Combined with Go 1.5 this spreads out metrics to the underlying statsd daemons
much more evenly than before and is quite a bit faster.  I'm easily seeing
700,000 packets per second processed by StatsRelay on my test machine.  Using
only 11 MB of RAM.

My Use Case
-----------

I run a Statsd service for a large collection of in-house web apps.  There are
metrics generated per host -- where you would usually run a local statsd daemon
to deal with high load.  But most of my metrics are application specific and
not host specific.  So running local statsd daemons means they would each
submit the same metrics to Graphite resulting in corrupt data in Graphite or
very large and time consuming aggregations on the Graphite side.  Instead, I
run a single Statsd service scaled to handle more than 1,000,000 incoming
statsd metrics per second.

I do this using LVS/IPVS to create a UDP load balancer.  You'll want to use
a new enough kernel and ipvsadm tool to have the --ops command which treats
each incoming UDP packet as its own "connection" and routes each independently.

    ipvsadm -A -u 10.0.0.222:9125 -s wlc -o

Then add your real servers that run identically configured StatsRelay daemons
to the LVS service:

    ipvsadm -a -u 10.0.0.222:9125 -r 10.0.0.156:9125 -g -w 100
    ...

I use [Keepalved][1] for the details here.

I run [Etsy's statsd][2] daemon on a pool of machines and StatsRelay on a much
smaller pool.  I can simply add more machines to the pools for more
throughput.  My incantation for StatsRelay looks something like this upstart
job.  (A Puppet ERB template.)

    description "StatsRelay statsd proxy on port 9125"
    author      "Jack Neely <jjneely@42lines.net>"

    start on startup
    stop on shutdown

    setuid nobody

    exec /usr/bin/statsrelay --port 9125 --bind 0.0.0.0 \
        --prefix statsrelay.<%= @hostname %> \
	<%= @dest.join(" \\\n    ") %>


Go Profiling Support
--------------------

Heap profile example:

    go tool pprof http://localhost:8080/debug/pprof/heap

Or to look at a 30-second CPU profile example:

    go tool pprof http://localhost:8080/debug/pprof/profile

Goroutine blocking profile example:

    go tool pprof http://localhost:6060/debug/pprof/block

For longer profiling and saving Graphviz in png/svg/pdf:

```
go tool pprof http://localhost:8080/debug/pprof/profile?seconds=180
Fetching profile from http://localhost:8080/debug/pprof/profile?seconds=180
Please wait... (3m0s)
Saved profile in /tmp/pprof/pprof.statsrelay.localhost:8080.samples.cpu.007.pb.gz
Entering interactive mode (type "help" for commands)
(pprof) top
19890ms of 37300ms total (53.32%)
Dropped 343 nodes (cum <= 186.50ms)
Showing top 10 nodes out of 176 (cum >= 1860ms)
      flat  flat%   sum%        cum   cum%
    9880ms 26.49% 26.49%    10570ms 28.34%  syscall.Syscall
    3130ms  8.39% 34.88%     3130ms  8.39%  runtime.futex
    1320ms  3.54% 38.42%     2590ms  6.94%  runtime.mallocgc
    1180ms  3.16% 41.58%     1180ms  3.16%  syscall.RawSyscall
    1020ms  2.73% 44.32%     1020ms  2.73%  runtime._ExternalCode
    1020ms  2.73% 47.05%     1020ms  2.73%  runtime.epollctl
     970ms  2.60% 49.65%      970ms  2.60%  runtime.epollwait
     570ms  1.53% 51.18%      570ms  1.53%  runtime.heapBitsSetType
     430ms  1.15% 52.33%      430ms  1.15%  runtime.memmove
     370ms  0.99% 53.32%     1860ms  4.99%  runtime.newobject
(pprof) png > graph.png
Generating report in graph.png
(pprof) svg > graph.svg                                                                                                                                                                                                                      Generating report in graph.svg
```

[1]: http://keepalived.org/
[2]: https://github.com/etsy/statsd
