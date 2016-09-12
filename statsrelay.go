package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const VERSION string = "0.0.3"

// BUFFERSIZE controls the size of the [...]byte array used to read UDP data
// off the wire and into local memory.  Metrics are separated by \n
// characters.  This buffer is passed to a handler to proxy out the metrics
// to the real statsd daemons.
const BUFFERSIZE int = 1 * 1024 * 1024 // 1MiB

// prefix is the string that will be prefixed onto self generated stats.
// Such as <prefix>.statsProcessed.  Default is "statsrelay"
var prefix string

// udpAddr is a mapping of HOST:PORT:INSTANCE to a UDPAddr object
var udpAddr = make(map[string]*net.UDPAddr)

// tcpAddr is a mapping of HOST:PORT:INSTANCE to a TCPAddr object
var tcpAddr = make(map[string]*net.TCPAddr)

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

// IP protocol set for sending data target
var sendproto string

// packetLen is the size in bytes of data we stuff into one packet before
// sending it to statsd. This must be lower than the MTU, IPv4 header size
// and UDP header size to avoid fragmentation and data loss.
var packetLen int

// Maximum size of buffer
var bufferMaxSize int

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
	//    KEY:VALUE|TYPE|RATE
	length := bytes.IndexByte(metric, byte(':'))
	if length == -1 {
		return "error", errors.New("Length of -1, must be invalid StatsD data")
	}
	return string(metric[:length]), nil
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
		tcpAddr, err := net.ResolveTCPAddr("tcp", target)
		if err != nil {
			log.Fatalf("ResolveTCPAddr Failed %s\n", err.Error())
		}
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			log.Panicln(err)
		}
		conn.Write(buff)
		conn.Close()
	case "TEST":
		// A test no-op
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
			if verbose {
				log.Printf("Sending %s to %s", metric, target)
			}

			// check built packet size and send if metric doesn't fit
			if packets[target].Len()+size > packetLen {
				sendPacket(packets[target].Bytes(), target, sendproto)
				packets[target].Reset()
			}
			// add to packet
			packets[target].Write(buff[offset : offset+size])
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
		log.Printf("Procssed %d metrics. Running total: %d. Metrics/sec: %d\n",
			numMetrics, totalMetrics,
			int64(totalMetrics)/(time.Now().Unix()-epochTime))
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

func main() {
	var bindAddress string
	var port int

	flag.IntVar(&port, "port", 9125, "Port to listen on")
	flag.IntVar(&port, "p", 9125, "Port to listen on")

	flag.StringVar(&bindAddress, "bind", "0.0.0.0", "IP Address to listen on")
	flag.StringVar(&bindAddress, "b", "0.0.0.0", "IP Address to listen on")

	flag.StringVar(&prefix, "prefix", "statsrelay", "The prefix to use with self generated stats")

	flag.BoolVar(&verbose, "verbose", false, "Verbose output")
	flag.BoolVar(&verbose, "v", false, "Verbose output")

	flag.StringVar(&sendproto, "sendproto", "UDP", "IP Protocol for sending data - TCP or UDP")
	flag.IntVar(&packetLen, "packetlen", 1400, "Max packet length. Must be lower than MTU plus IPv4 and UDP headers to avoid fragmentation.")

	defaultBufferSize, err := getSockBufferMaxSize()
	if err != nil {
		defaultBufferSize = 32 * 1024
	}

	flag.IntVar(&bufferMaxSize, "bufsize", defaultBufferSize, "Read buffer size")

	flag.Parse()

	if len(flag.Args()) == 0 {
		log.Fatalf("One or more host specifications are needed to locate statsd daemons.\n")
	}

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
		}
	}

	epochTime = time.Now().Unix()
	runServer(bindAddress, port)

	log.Printf("Normal shutdown.\n")
}
