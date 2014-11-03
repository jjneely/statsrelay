StatsdRelay
===========

StatsdRelay is a Golang implementation of a proxy or load balancer for Etsy's
excellent nodejs statsd utility.  This utility will listen for UDP metrics
in the same format as Etsy's statsd and forward them to one statsd service
from a pool of available host:port combinations.  Each metric is hashed with
a consistent hashing algorithm so that the same metric is always sent to
the same statsd server.

This is written in Go with a simple goal: Be fast.  Current statsd
distributions come with a nodejs proxy tool that does much the same thing.
However, that proxy daemon is unable to keep up in high-traffic situations.

You can layer statsdrelay as needed to build a tree, or run multiple versions
behind a generic UDP load balancer.  Provided the configuration is the same
all statsdrelay daemons will route metrics to the same statsd server.

Usage
-----
    statsrelay [options] HOST:PORT:INSTANCE [HOST:PORT:INSTANCE ...]

    -b="0.0.0.0": IP Address to listen on
    -bind="0.0.0.0": IP Address to listen on
    -p=9125: Port to listen on
    -port=9125: Port to listen on
    -prefix="statsrelay": The prefix to use with self generated stats


You must specify at least one HOST:PORT combination.  The INSTANCE can be
used to further populate or weight the consistent hashing ring as you see fit.
The instance is stripped before using the HOST and PORT to create a UDP
socket.
