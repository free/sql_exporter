# Prometheus SQL Exporter [![Build Status](https://travis-ci.org/free/sql_exporter.svg)](https://travis-ci.org/free/sql_exporter) [![Go Report Card](https://goreportcard.com/badge/github.com/free/sql_exporter)](https://goreportcard.com/report/github.com/free/sql_exporter) [![GoDoc](https://godoc.org/github.com/free/sql_exporter?status.svg)](https://godoc.org/github.com/free/sql_exporter)

Database agnostic SQL exporter for [Prometheus](https://prometheus.io).

## Overview

SQL Exporter is a configuration driven exporter that exposes information gathered from DBMSs, for use by the Prometheus
monitoring system. Out of the box it provides support for MySQL, PostgreSQL, Microsoft SQL Server and Clickhouse, but
any DBMS for which a Go driver is available may be supported by rebuilding the binary with the driver linked in.

Which metrics get collected and how is entirely controlled through configuration. By default metrics are collected
every time `/metrics` is polled, but minimum collection intervals may optionally be set on groups of queries, causing
polls issued before that interval has elapsed to be served cached metrics. This allows collection frequency and timing
to be entirely controlled by Prometheus, while preventing expensive queries from putting excessive load on the database.

A SQL Exporter instance may poll multiple DBMS instances (of varying kinds) at the same time. To organize these, SQL
Exporter borrows Prometheus concepts such as job and instance: a job may e.g. cover all MySQL instances; or all replicas
of a pricing database; and consist of a set of instances. Looking from the bottom up, SQL queries are grouped into
collectors -- logical groups of queries mapped to the metrics they populate, e.g. _user stats_ or _I/O stats_. They may
be generic DBMS-specific collectors (e.g. _MySQL user stats_) or custom, deployment specific metrics (e.g. _pricing
data freshness_). Each job definition references a number of collectors; the queries defined by those collectors will
be executed on all instances of that job, generating the metrics mapped to them. All exported metrics get `job` and
`instance` automatic labels, as well as any custom target labels (e.g. `env="prod"`), similarly to Prometheus targets.

## Usage

Get Prometheus SQL Exporter, either as a [packaged release](https://github.com/free/sql_exporter/releases/latest) or
build it yourself:

```
$ go get github.com/free/sql_exporter/cmd/sql_exporter
```

then run it from the command line:

```
$ ./sql_exporter
```

Use the `-help` flag to get help information.

```
$ ./sql_exporter -help
Usage of ./sql_exporter:
  -config.file string
      SQL Exporter configuration file name. (default "sql_exporter.yml")
  -web.listen-address string
      Address to listen on for web interface and telemetry. (default ":9399")
  -web.metrics-path string
      Path under which to expose metrics. (default "/metrics")
  [...]
```

## Configuration

SQL Exporter uses YAML for configuration. Below is a very basic configuration, which will produce metrics that look like
`pricing_update_time{job="pricing_db",instance="dbserver1.example.com:1433",env="prod",market="US"} 1511196767`. For a
slightly more complex, fully documented example see
[`documentation/sql_exporter.yml`](https://github.com/free/sql_exporter/tree/master/documentation/sql_exporter.yml).
For practical configuration examples and DBMS-specific "standard" collectors, check the
[`examples`](https://github.com/free/sql_exporter/tree/master/examples) directory.

```yaml
jobs:

  - job_name: pricing_db

    # Collectors (defined below) applied to all targets in this job.
    collectors: [stock_pricing_freshness]

    # Similar to the Prometheus static_configs.
    static_configs:
      - targets:
          # Map of instance name (to export as instance label) to DSN
          'dbserver1.example.com:1433': 'sqlserver://prom_user:prom_password@dbserver1.example.com:1433'
          'dbserver2.example.com:1433': 'sqlserver://prom_user:prom_password@dbserver2.example.com:1433'
        labels:
          env: 'prod'

# Collector definitions. A collector is a named set of related metrics/queries that are collected together.
collectors:

  # Collectors are referenced by name.
  - collector_name: stock_pricing_freshness

    # A Prometheus metric with (optional) additional labels, value and labels populated from a single query. 
    metrics:
      - metric_name: pricing_update_time
        type: counter
        help: 'Time when each pricing for a market was last updated.'
        key_labels:
          # Populated from the `market` column of each row.
          - Market
        values: [LastUpdateTime]
        query: |
          SELECT Market, max(UpdateTime) AS LastUpdateTime
          FROM MarketPrices
          GROUP BY market
```

### Prometheus configuration

In the Prometheus configuration, SQL exporter is set up similarly to the Prometheus Gateway, i.e. a regular target with
`honor_labels: true` added, so that the `job` and `instance` labels applied to metrics are not overwritten with the
job and instance label of the SQL Exporter instance. See the [Prometheus documentation](
https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config) for details.

```yaml
  - job_name: 'sql_exporter'
    honor_labels: true
    static_configs:
      - targets: ['localhost:9399']
        labels:
          # If SQL Exporter targets also have an `env` label defined, it will override this because of `honor_labels`.
          # This label will essentially only apply to the sql_exporter instance's metrics (e.g. heap or CPU metrics).
          env: 'prod'
```
