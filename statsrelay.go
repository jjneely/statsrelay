package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"
)

import "github.com/stathat/consistent"

// BUFFERSIZE controls the size of the [...]byte array used to read UDP data
// off the wire and into local memory.  Metrics are separated by \n
// characters.  This buffer is passed to a handler to proxy out the metrics
// to the real statsd daemons.
const BUFFERSIZE int = 1 * 1024 * 1024 // 1MiB

// packetLen is the size in bytes of data we stuff into one packet before
// sending it to statsd.  This must be lower than the MTU, IPv4 header size
// and UDP header size.
const packetLen int = 1000

// prefix is the string that will be prefixed onto self generated stats.
// Such as <prefix>.statsProcessed.  Default is "statsrelay"
var prefix string

// udpAddr is a mapping of HOST:PORT:INSTANCE to a UDPAddr object
var udpAddr = make(map[string]*net.UDPAddr)

// hashRing is our consistent hashing ring.
var hashRing *consistent.Consistent

// totalMetrics tracks the totall number of metrics processed
var totalMetrics int = 0

// totalMetricsLock is a mutex gaurding totalMetrics
var totalMetricsLock sync.Mutex

// sockBufferMaxSize() returns the maximum size that the UDP receive buffer
// in the kernel can be set to.  In bytes.
func sockBufferMaxSize() int {

	// XXX: This is Linux-only most likely
	data, err := ioutil.ReadFile("/proc/sys/net/core/rmem_max")
	if err != nil {
		panic(err)
	}

	data = bytes.TrimRight(data, "\n\r")
	i, err := strconv.Atoi(string(data))
	if err != nil {
		fmt.Printf("Could not parse /proc/sys/net/core/rmem_max\n")
		fmt.Println(err)
		os.Exit(2)
	}

	return i
}

// getMetricName() parses the given []byte metric as a string, extracts
// the metric key name and returns it as a string.
func getMetricName(metric []byte) string {
	// statsd metrics are of the form:
	//    KEY:VALUE|TYPE|RATE
	length := bytes.IndexByte(metric, byte(':'))
	return string(metric[:length])
}

// sendPacket takes a []byte and writes that directly to a UDP socket
// that was assigned for target.
func sendPacket(buff []byte, target string) {
	//fmt.Printf("Packet Content: %s", string(buff))
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		panic(err)
	}

	conn.WriteToUDP(buff, udpAddr[target])
	conn.Close()
}

// buildPacketMap() is a helper function to initiallize a map that represents
// a UDP packet currently being built for each destination we proxy to.  As
// Go forbids taking the address of an object in a map or array so the
// bytes.Buffer object must be stored in the map as a pointer rather than
// a direct object in order to call the pointer methods on it.
func buildPacketMap() map[string]*bytes.Buffer {
	members := hashRing.Members()
	hash := make(map[string]*bytes.Buffer, len(members))

	for _, v := range members {
		hash[v] = new(bytes.Buffer)
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
		target, err := hashRing.Get(getMetricName(buff[offset : offset+size]))
		if err != nil {
			panic(err)
		}

		// check built packet size and send if metric doesn't fit
		if packets[target].Len()+size > packetLen {
			sendPacket(packets[target].Bytes(), target)
			packets[target].Reset()
		}
		// add to packet
		packets[target].Write(buff[offset : offset+size])
		packets[target].Write(sep)

		numMetrics++
		offset = offset + size + 1
	}

	if numMetrics == 0 {
		// if we haven't handled any metrics, then don't update counters/stats
		// or send packets
		return
	}

	// Update interal counter
	totalMetricsLock.Lock()
	totalMetrics = totalMetrics + numMetrics
	totalMetricsLock.Unlock()

	// Handle reporting our own stats
	stats := fmt.Sprintf("%s:%d|c\n", statsMetric, numMetrics)
	target, err := hashRing.Get(statsMetric)
	if err != nil {
		panic(err)
	}
	if packets[target].Len()+len(stats) > packetLen {
		sendPacket(packets[target].Bytes(), target)
		packets[target].Reset()
	}
	packets[target].Write([]byte(stats))

	// Empty out any remaining data
	for _, target := range hashRing.Members() {
		if packets[target].Len() > 0 {
			sendPacket(packets[target].Bytes(), target)
		}
	}

	fmt.Printf("Procssed %d metrics.  Running total: %d\n", numMetrics, totalMetrics)
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

	fmt.Printf("Listening on %s:%d\n", ip, port)
	sock, err := net.ListenUDP("udp", &addr)
	if err != nil {
		fmt.Printf("Error opening UDP socket.\n")
		fmt.Println(err)
		os.Exit(2)
	}
	defer sock.Close()

	fmt.Printf("Setting socket read buffer size to: %d\n", sockBufferMaxSize())
	err = sock.SetReadBuffer(sockBufferMaxSize())
	if err != nil {
		fmt.Printf("Unable to set read buffer size on socket.  Non-fatal.")
		fmt.Println(err)
	}
	err = sock.SetDeadline(time.Now().Add(time.Second))
	if err != nil {
		fmt.Printf("Unable to set timeout on socket.\n")
		fmt.Println(err)
		os.Exit(2)
	}

	fmt.Printf("Rock and Roll!\n")
	for {
		if buff == nil {
			buff = new([BUFFERSIZE]byte)
			offset = 0
			timeout = false
		}

		i, err := sock.Read(buff[offset:])
		if err == nil {
			buff[offset+i] = '\n'
			offset = offset + i + 1
		} else if err.(net.Error).Timeout() {
			timeout = true
			err = sock.SetDeadline(time.Now().Add(time.Second))
			if err != nil {
				panic(err)
			}
		} else {
			fmt.Printf("Read Error: %s\n", err)
			continue
		}

		if offset > BUFFERSIZE-4096 || timeout {
			// Approching make buff size
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
	signal.Notify(sig, os.Interrupt, os.Kill)

	// read incming UDP packets
	go readUDP(host, port, c)

	for {
		select {
		case buff := <-c:
			//fmt.Printf("Handling %d length buffer...\n", len(buff))
			go handleBuff(buff)
		case <-sig:
			fmt.Printf("Signal received.  Shutting down...\n")
			fmt.Printf("Received %d metrics.\n", totalMetrics)
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

	flag.Parse()

	if len(flag.Args()) == 0 {
		fmt.Printf("One or most host specifications are needed to locate statsd daemons.\n")
		os.Exit(1)
	}

	hashRing = consistent.New()
	hashRing.NumberOfReplicas = 1

	for _, v := range flag.Args() {
		var addr *net.UDPAddr
		var err error
		host := strings.Split(v, ":")

		switch len(host) {
		case 1:
			fmt.Printf("Invalid statsd location: %s\n", v)
			fmt.Printf("Must be of the form HOST:PORT or HOST:PORT:INSTANCE\n")
			os.Exit(1)
		case 2:
			addr, err = net.ResolveUDPAddr("udp", v)
			if err != nil {
				fmt.Printf("Error parsing HOST:PORT \"%s\"\n", v)
				fmt.Printf("%s\n", err.Error())
				os.Exit(1)
			}
		case 3:
			addr, err = net.ResolveUDPAddr("udp", host[0]+":"+host[1])
			if err != nil {
				fmt.Printf("Error parsing HOST:PORT:INSTANCE \"%s\"\n", v)
				fmt.Printf("%s\n", err.Error())
				os.Exit(1)
			}
		default:
			fmt.Printf("Unrecongnized host specification: %s\n", v)
			os.Exit(1)
		}

		if addr != nil {
			udpAddr[v] = addr
			hashRing.Add(v)
		}
	}

	runServer(bindAddress, port)

	fmt.Printf("Done!\n")
}
