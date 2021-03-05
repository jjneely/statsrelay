package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
        "github.com/patrickmn/go-cache"
  "github.com/dropbox/godropbox/net2"
)

const VERSION string = "0.0.10"

// BUFFERSIZE controls the size of the [...]byte array used to read UDP data
// off the wire and into local memory.  Metrics are separated by \n
// characters.  This buffer is passed to a handler to proxy out the metrics
// to the real statsd daemons.
const BUFFERSIZE int = 1 * 1024 * 1024 // 1MiB

// prefix is the string that will be prefixed onto self generated stats.
// Such as <prefix>.statsProcessed.  Default is "statsrelay"
var prefix string

// metricsPrefix is the string that will be prefixed onto each metric passed
// through statsrelay. This is especialy usefull with docker passing
// env, appname automatic from docker environment to app metri
// Such as <metricsPrefix>.mytest.service.metric.count  Default is empty
var metricsPrefix string

// metricTags is the string that will be used as tags into each metric passed
// through statsrelay.
// This is especialy usefull with datadog statsd metrics passing.
// Such as my.prefix.myinc:1|c|@1.000000|#baz,foo:bar
// Default is empty
var metricTags string

// udpAddr is a mapping of HOST:PORT:INSTANCE to a UDPAddr object
var udpAddr = make(map[string]*net.UDPAddr)

// tcpAddr is a mapping of HOST:PORT:INSTANCE to a TCPAddr object
var tcpAddr = make(map[string]*net.TCPAddr)

// tcpPool is our dropbox TCP connection pool object, representing
// all statsd backend destinations
var tcpPool net2.ConnectionPool

// hashRing is our consistent hashing ring.
var hashRing = NewJumpHashRing(1)

// totalMetrics tracks the totall number of metrics processed
var totalMetrics int = 0

// totalMetricsLock is a mutex gaurding totalMetrics
var totalMetricsLock sync.Mutex

// Time we began
var epochTime int64

// Verbose/Debug output
var verbose bool

// dnscache for caching resolved domains in sendout
var dnscache bool

// IP protocol set for sending data target
var sendproto string

// packetLen is the size in bytes of data we stuff into one packet before
// sending it to statsd. This must be lower than the MTU, IPv4 header size
// and UDP header size to avoid fragmentation and data loss.
var packetLen int

// Maximum size of buffer
var bufferMaxSize int

// Timeout value for remote TCP connection
var TCPtimeout time.Duration

// profiling bool value to enable disable http endpoint for profiling
var profiling bool

// profilingBind string value for pprof http host:port data
var profilingBind string

// maxprocs int value to set GOMAXPROCS
var maxprocs int

// The maximum number of connections that can be active per host at any
// given time (A non-positive value indicates the number of connections
// is unbounded).
var TCPMaxActive int

// The maximum number of idle connections per host that are kept alive by
// the connection pool.
var TCPMaxIdle int

// The maximum amount of time an idle connection can alive (if specified).
var TCPMaxTimeout time.Duration

// This limits the number of concurrent Dial calls (there's no limit when
// DialMaxConcurrency is non-positive).
var TCPMaxNewConnections int

// dnscacheTime TTL of cached resolved k/v
var dnscacheTime time.Duration

// dnscachePurge after this time all stale objects will be deleted
var dnscachePurge time.Duration

// dnscacheExp custom expiration when setting new object
var dnscacheExp time.Duration

// c cache definition for resolved endpoint
var c = cache.New(dnscacheTime, dnscachePurge)

// ctarget cached target used for resolving
var ctarget string

// sockBufferMaxSize() returns the maximum size that the UDP receive buffer
// in the kernel can be set to.  In bytes.
func getSockBufferMaxSize() (int, error) {

	// XXX: This is Linux-only most likely
	data, err := ioutil.ReadFile("/proc/sys/net/core/rmem_max")
	if err != nil {
		return -1, err
	}

	data = bytes.TrimRight(data, "\n\r")
	i, err := strconv.Atoi(string(data))
	if err != nil {
		log.Printf("Could not parse /proc/sys/net/core/rmem_max\n")
		return -1, err
	}

	return i, nil
}

// getMetricName() parses the given []byte metric as a string, extracts
// the metric key name and returns it as a string.
func getMetricName(metric []byte) (string, error) {
	// statsd metrics are of the form:
	//    KEY:VALUE|TYPE|RATE or KEY:VALUE|TYPE|RATE|#tags
	length := bytes.IndexByte(metric, byte(':'))
	if length == -1 {
		return "error", errors.New("Length of -1, must be invalid StatsD data")
	}
	if len(metricsPrefix) != 0 {
		return genPrefix(metric[:length], metricsPrefix), nil
	}
	return string(metric[:length]), nil
}

// genPrefix() combine metric []byte with metricsPrefix string and return as string
func genPrefix(metric []byte, metricsPrefix string) string {
	if len(metricsPrefix) != 0 {
		return fmt.Sprintf("%s.%s", metricsPrefix, string(metric))
	}
	return string(metric)
}

// extendMetric() add prefix to []byte metric as a string, extracts metric
// key name and add metrics prefixs, then returning new key as []byte.
func extendMetric(metric []byte, metricsPrefix string, metricTags string) ([]byte, error) {
	// statsd metrics are of the form:
	// KEY:VALUE|TYPE|RATE or KEY:VALUE|TYPE|RATE|#tags
	length := bytes.IndexByte(metric, byte(':'))
	if length == -1 {
		return nil, errors.New("Length of -1, must be invalid StatsD data in adding prefix")
	}
	if len(metricTags) != 0 {
		return []byte(genTags(genPrefix(metric, metricsPrefix), metricTags)), nil
	}
	return []byte(genPrefix(metric, metricsPrefix)), nil
}

// genTags() add metric []byte and metricTags string, return string
// of metrics with additional tags
func genTags(metric, metricTags string) string {
	// statsd metrics are of the form:
	// KEY:VALUE|TYPE|RATE or KEY:VALUE|TYPE|RATE|#tags
	// This function add or extend #tags in metric
	if strings.Contains((metric), "|#") {
		return fmt.Sprintf("%s,%s", metric, metricTags)
	}
	return fmt.Sprintf("%s|#%s", metric, metricTags)
}

// write to the TCP connection pool, recovering errors and retrying
func connectionPoolWrite(buff []byte, target string) {
  // get an active connection from the pool
  conn, err := tcpPool.Get("tcp", target)
  if err != nil {
    log.Printf("TCP Error in Pool.Get(): %s", err)
    // Return on error to ignore this packet
    return
  }
  _, err = conn.Write(buff)
  if err != nil {
    log.Printf("TCP Error writing to target pool %s: %s", target, err)
    // Return on error to ignore this packet
    return
  }
  // finished using the connection, release back to the pool 
  conn.ReleaseConnection()
}

// sendPacket takes a []byte and writes that directly to a UDP socket
// that was assigned for target.
func sendPacket(buff []byte, target string, sendproto string) {
  switch sendproto {
  case "UDP":
    conn, err := net.ListenUDP("udp", nil)
    if err != nil {
      log.Panicln(err)
    }
    conn.WriteToUDP(buff, udpAddr[target])
    conn.Close()
  case "TCP":
    if verbose {
      log.Printf("Sending to target: %s => %s", target, buff)
    }
    connectionPoolWrite(buff, target)
    break
  case "TEST":
    if verbose {
      log.Printf("Debug: Would have sent packet of %d bytes to %s",
      len(buff), target)
    }
  default:
    log.Fatalf("Illegal send protocol %s", sendproto)
  }
}

// buildPacketMap() is a helper function to initialize a map that represents
// a UDP packet currently being built for each destination we proxy to.  As
// Go forbids taking the address of an object in a map or array so the
// bytes.Buffer object must be stored in the map as a pointer rather than
// a direct object in order to call the pointer methods on it.
func buildPacketMap() map[string]*bytes.Buffer {
	members := hashRing.Nodes()
	hash := make(map[string]*bytes.Buffer, len(members))

	for _, n := range members {
		hash[n.Server] = new(bytes.Buffer)
	}

	return hash
}

// handleBuff() sorts through a full buffer of metrics and batches metrics
// to remote statsd daemons using a consistent hash.
func handleBuff(buff []byte) {
  packets := buildPacketMap()
  sep := []byte("\n")
  numMetrics := 0
  statsMetric := prefix + ".statsProcessed"

  for offset := 0; offset < len(buff); {
    loop:
    for offset < len(buff) {
      // Find our next value
      switch buff[offset] {
      case '\n':
        offset++
      case '\r':
        offset++
      case 0:
        offset++
      default:
        break loop
      }
    }

    size := bytes.IndexByte(buff[offset:], '\n')
    if size == -1 {
      // last metric in buffer
      size = len(buff) - offset
    }
    if size == 0 {
      // no more metrics
      break
    }

    // Check to ensure we get a metric, and not an invalid Byte sequence
    metric, err := getMetricName(buff[offset : offset+size])

    if err == nil {

      target := hashRing.GetNode(metric).Server
      ctarget := target

      // resolve and cache
      if dnscache {
        gettarget, found := c.Get(target)
        if found {
          ctarget = gettarget.(string)
          if verbose {
            log.Printf("Found in cache target %s (%s)", target, ctarget)
          }
        } else {
          targetaddr, err := net.ResolveUDPAddr("udp", target)
          if verbose {
            log.Printf("Not found in cache adding target %s (%s)", target, ctarget)
          }
          if err != nil {
            log.Printf("Error resolving target %s", target)
          }
          c.Set(target, targetaddr.String(), dnscacheExp)
          ctarget = targetaddr.String()
          // Register the destination IP:PORT combo in the TCP pool 
          if sendproto == "TCP" {
            tcpPool.Register("tcp", ctarget)
          }
        }
      }
      // check built packet size and send if metric doesn't fit
      if packets[target].Len()+size > packetLen {
        sendPacket(packets[target].Bytes(), ctarget, sendproto)
        packets[target].Reset()
      }
      // add to packet
      if len(metricsPrefix) != 0 || len(metricTags) != 0 {
        buffPrefix, err := extendMetric(buff[offset:offset+size], metricsPrefix, metricTags)
        if verbose {
          log.Printf("Sending %s to %s (%s)", buffPrefix, target, ctarget)
        }
        if err != nil {
          if len(metricsPrefix) != 0 {
            log.Printf("Error %s when adding prefix %s", err, metricsPrefix)
            break
          }
          if len(metricTags) != 0 {
            log.Printf("Error %s when adding tag %s", err, metricTags)
            break
          }
        }
        packets[target].Write(buffPrefix)
      } else {
        if verbose {
          log.Printf("Sending %s to %s (%s)", metric, target, ctarget)
        }
        packets[target].Write(buff[offset : offset+size])
      }
      packets[target].Write(sep)
      numMetrics++
    }

    offset = offset + size + 1
  }

  if numMetrics == 0 {
    // if we haven't handled any metrics, then don't update counters/stats
    // or send packets
    return
  }

  // Update internal counter
  totalMetricsLock.Lock()
  totalMetrics = totalMetrics + numMetrics
  totalMetricsLock.Unlock()

  // Handle reporting our own stats
  stats := fmt.Sprintf("%s:%d|c\n", statsMetric, numMetrics)
  target := hashRing.GetNode(statsMetric).Server
  if packets[target].Len()+len(stats) > packetLen {
    sendPacket(packets[target].Bytes(), target, sendproto)
    packets[target].Reset()
  }
  packets[target].Write([]byte(stats))

  // Empty out any remaining data
  for _, target := range hashRing.Nodes() {
    if packets[target.Server].Len() > 0 {
      sendPacket(packets[target.Server].Bytes(), target.Server, sendproto)
    }
  }

  if verbose && time.Now().Unix()-epochTime > 0 {
    log.Printf("Processed %d metrics. Running total: %d. Metrics/sec: %d\n",
    numMetrics, totalMetrics,
    int64(totalMetrics)/(time.Now().Unix()-epochTime))
    if sendproto == "TCP" {
      log.Printf("TCP Pool:  Active: %d   Max: %d  Idle: %d", 
        tcpPool.NumActive(), tcpPool.ActiveHighWaterMark(), tcpPool.NumIdle())
    }
  }
}

// readUDP() a goroutine that just reads data off of a UDP socket and fills
// buffers.  Once a buffer is full, it passes it to handleBuff().
func readUDP(ip string, port int, c chan []byte) {
	var buff *[BUFFERSIZE]byte
	var offset int
	var timeout bool
	var addr = net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(ip),
	}

	log.Printf("Starting version %s", VERSION)
	log.Printf("Listening on %s:%d\n", ip, port)
	sock, err := net.ListenUDP("udp", &addr)
	if err != nil {
		log.Printf("Error opening UDP socket.\n")
		log.Fatalln(err)
	}
	defer sock.Close()

	log.Printf("Setting socket read buffer size to: %d\n", bufferMaxSize)
	err = sock.SetReadBuffer(bufferMaxSize)
	if err != nil {
		log.Printf("Unable to set read buffer size on socket.  Non-fatal.")
		log.Println(err)
	}
	err = sock.SetDeadline(time.Now().Add(time.Second))
	if err != nil {
		log.Printf("Unable to set timeout on socket.\n")
		log.Fatalln(err)
	}

	if sendproto == "TCP" {
		log.Printf("TCP send timeout set to %s", TCPtimeout)
    log.Printf("TCP Pool maximum active connections: %d", TCPMaxActive)
    log.Printf("TCP Pool maximum idle connections: %d", TCPMaxIdle)
    log.Printf("TCP Pool maximum idle timeout: %v", TCPMaxTimeout)
    log.Printf("TCP Pool maximum new concurrent connections: %d", TCPMaxNewConnections)
	}

	if len(metricsPrefix) != 0 {
		log.Printf("Metrics prefix set to %s", metricsPrefix)
	}

	if len(metricTags) != 0 {
		log.Printf("Metrics tags set to %s", metricTags)
	}

	if verbose {
		log.Printf("Rock and Roll!\n")
	}

	for {
		if buff == nil {
			buff = new([BUFFERSIZE]byte)
			offset = 0
			timeout = false
		}

		i, err := sock.Read(buff[offset : BUFFERSIZE-1])
		if err == nil {
			buff[offset+i] = '\n'
			offset = offset + i + 1
		} else if err.(net.Error).Timeout() {
			timeout = true
			err = sock.SetDeadline(time.Now().Add(time.Second))
			if err != nil {
				log.Panicln(err)
			}
		} else {
			log.Printf("Read Error: %s\n", err)
			continue
		}

		if offset > BUFFERSIZE-4096 || timeout {
			// Approaching make buff size
			// we use a 4KiB margin
			c <- buff[:offset]
			buff = nil
		}
	}
}

// runServer() runs and manages this daemon, deals with OS signals, and handles
// communication channels.
func runServer(host string, port int) {
	var c chan []byte = make(chan []byte, 256)
	// Set up channel on which to send signal notifications.
	// We must use a buffered channel or risk missing the signal
	// if we're not ready to receive when the signal is sent.
	var sig chan os.Signal = make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGTERM)

	// read incoming UDP packets
	go readUDP(host, port, c)

	for {
		select {
		case buff := <-c:
			//fmt.Printf("Handling %d length buffer...\n", len(buff))
			go handleBuff(buff)
		case <-sig:
			log.Printf("Signal received.  Shutting down...\n")
			log.Printf("Received %d metrics.\n", totalMetrics)
			return
		}
	}
}

// wrapper function for connecting that we pass to the TCP pool
func DialFunc(network string, address string) (net.Conn, error) {
  return net.DialTimeout(network, address, TCPtimeout)
}

func main() {
	var bindAddress string
	var port int

	flag.IntVar(&port, "port", 9125, "Port to listen on")
	flag.IntVar(&port, "p", 9125, "Port to listen on")

	flag.StringVar(&bindAddress, "bind", "0.0.0.0", "IP Address to listen on")
	flag.StringVar(&bindAddress, "b", "0.0.0.0", "IP Address to listen on")

	flag.StringVar(&prefix, "prefix", "statsrelay", "The prefix to use with self generated stats")
	flag.StringVar(&metricsPrefix, "metrics-prefix", "", "The prefix to use with metrics passed through statsrelay")

	flag.StringVar(&metricTags, "metrics-tags", "", "Comma separated tags for each relayed metric. Example: foo:bar,test,test2:bar")

	flag.IntVar(&maxprocs, "maxprocs", 0, "Set GOMAXPROCS in runtime. If not defined then Golang defaults.")

	flag.BoolVar(&verbose, "verbose", false, "Verbose output")
	flag.BoolVar(&verbose, "v", false, "Verbose output")

        flag.BoolVar(&dnscache, "dnscache", false, "Enable in app DNS cache for resolved TCP sendout sharded endpoints")
        flag.DurationVar(&dnscacheTime, "dnscache-time", 1*time.Second, "Time we cache resolved adresses of sharded endpoint")
        flag.DurationVar(&dnscachePurge, "dnscache-purge", 5*time.Second, "When we purge stale elements in cache")
        flag.DurationVar(&dnscacheExp, "dnscache-expiration", 1*time.Second, "When set new object after resolv then use this expiration time in cache")

	flag.StringVar(&sendproto, "sendproto", "UDP", "IP Protocol for sending data: TCP, UDP, or TEST")
	flag.IntVar(&packetLen, "packetlen", 1400, "Max packet length. Must be lower than MTU plus IPv4 and UDP headers to avoid fragmentation.")

	flag.DurationVar(&TCPtimeout, "tcptimeout", 1*time.Second, "Timeout for TCP client remote connections")
	flag.DurationVar(&TCPtimeout, "t", 1*time.Second, "Timeout for TCP client remote connections")

	flag.BoolVar(&profiling, "pprof", false, "Enable HTTP endpoint for pprof")
	flag.StringVar(&profilingBind, "pprof-bind", ":8080", "Bind for pprof HTTP endpoint")

  flag.IntVar(&TCPMaxActive, "tcpmaxactive", 500, "Maximum number of connections that can be active per host")
  flag.IntVar(&TCPMaxIdle, "tcpmaxidle", 10, "Maximum number of idle connections per host that are kept alive")
  flag.DurationVar(&TCPMaxTimeout, "tcpmaxtimeout", 5*time.Minute, "Maximum amount of time an idle connection can live")
  flag.IntVar(&TCPMaxNewConnections, "tcpmaxnew", 100, "Maximum concurrent new connections created")

	defaultBufferSize, err := getSockBufferMaxSize()
	if err != nil {
		defaultBufferSize = 32 * 1024
	}

	flag.IntVar(&bufferMaxSize, "bufsize", defaultBufferSize, "Read buffer size")

	flag.Parse()

	if len(flag.Args()) == 0 {
		log.Fatalf("One or more host specifications are needed to locate statsd daemons.\n")
	}

	if maxprocs != 0 {
		log.Printf("Using GOMAXPROCS %d", maxprocs)
		runtime.GOMAXPROCS(maxprocs)
	}

	if profiling {
		go func() {
			log.Println(http.ListenAndServe(profilingBind, nil))
		}()
	}

  tcpPool = net2.NewMultiConnectionPool(net2.ConnectionOptions{
    MaxActiveConnections: int32(TCPMaxActive),
    MaxIdleConnections: uint32(TCPMaxIdle),
    MaxIdleTime: &TCPMaxTimeout,
    DialMaxConcurrency: TCPMaxNewConnections,
    Dial: DialFunc,
    ReadTimeout: TCPtimeout,
    WriteTimeout: TCPtimeout,
  })

	for _, v := range flag.Args() {
		var addr *net.UDPAddr
		var err error
		host := strings.Split(v, ":")

		switch len(host) {
		case 1:
			log.Printf("Invalid statsd location: %s\n", v)
			log.Fatalf("Must be of the form HOST:PORT or HOST:PORT:INSTANCE\n")
		case 2:
			addr, err = net.ResolveUDPAddr("udp", v)
			if err != nil {
				log.Printf("Error parsing HOST:PORT \"%s\"\n", v)
				log.Fatalf("%s\n", err.Error())
			}
		case 3:
			addr, err = net.ResolveUDPAddr("udp", host[0]+":"+host[1])
			if err != nil {
				log.Printf("Error parsing HOST:PORT:INSTANCE \"%s\"\n", v)
				log.Fatalf("%s\n", err.Error())
			}
		default:
			log.Fatalf("Unrecongnized host specification: %s\n", v)
		}

		if addr != nil {
			udpAddr[v] = addr
			hashRing.AddNode(Node{v, ""})
      // Register the destination DNS:PORT combo in the TCP pool
      if sendproto == "TCP" {
        tcpPool.Register("tcp", v)
      }
		}
	}

	epochTime = time.Now().Unix()
	runServer(bindAddress, port)

	log.Printf("Normal shutdown.\n")

}
