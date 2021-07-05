FROM        golang:1.16 AS builder
WORKDIR     /app
ENV         CGO_ENABLED=0
COPY        go.* ./
RUN         go mod download
COPY        . ./
RUN         go build -trimpath ./cmd/sql_exporter

FROM        busybox
RUN         touch /sql_exporter.yml
EXPOSE      9399
ENTRYPOINT  [ "/bin/sql_exporter" ]
COPY        --from=builder /app/sql_exporter /bin/
