# StatsRelay Makefile
#
# Go discourages the use of Makefiles to build.  However, we use this
# Makefile for the DEB packaging process.  Also, as this code performs
# much better under Go 1.3, which is not available at this writing for
# Ubuntu we package a compiled binary rather than generate the binary
# in the build process.  Much less for Ubuntu Precise, where this code
# must run.  XXX: This means the DEB packages are broken.

# Nothing done here, we don't compile in the DSC build process
all: statsrelay

statsrelay: statsrelay.go jump.go
	go build

install:
	install -D -m 0755 statsrelay $(DESTDIR)/usr/bin/statsrelay

dsc:
	dpkg-source -b .

clean:
	rm -f *~ statsrelay
