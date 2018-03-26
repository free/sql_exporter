FROM quay.io/prometheus/golang-builder AS builder

# Get database_exporter
ADD .   /go/src/github.com/Corundex/database_exporter
WORKDIR /go/src/github.com/Corundex/database_exporter

# Do makefile
RUN make

# Make image and copy build database_exporter
FROM        quay.io/prometheus/busybox:glibc
MAINTAINER  The Prometheus Authors <prometheus-developers@googlegroups.com>
COPY        --from=builder /go/src/github.com/Corundex/database_exporter/database_exporter  /bin/database_exporter

EXPOSE      9399
ENTRYPOINT  [ "/bin/database_exporter" ]