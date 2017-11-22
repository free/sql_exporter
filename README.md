# Prometheus SQL Exporter [![Build Status](https://travis-ci.org/free/sql_exporter.svg)](https://travis-ci.org/free/sql_exporter) [![Go Report Card](https://goreportcard.com/badge/github.com/free/sql_exporter)](https://goreportcard.com/report/github.com/free/sql_exporter) [![GoDoc](https://godoc.org/github.com/free/sql_exporter?status.svg)](https://godoc.org/github.com/free/sql_exporter)

Database agnostic SQL exporter for [Prometheus](https://prometheus.io).

## Overview

SQL Exporter is a configuration driven exporter that exposes metrics gathered from DBMSs, for use by the Prometheus
monitoring system. Out of the box it provides support for MySQL, PostgreSQL, Microsoft SQL Server and Clickhouse, but
any DBMS for which a Go driver is available may be monitored after rebuilding the binary including the specific driver.

The collected metrics and the queries behind them are entirely configuration defined. Per the Prometheus philosophy,
scrapes are synchronous (metrics are collected on every `/metrics` poll) but minimum collection intervals may optionally
be set on expensive queries, resulting in serving cached metrics.

A SQL Exporter instance may poll multiple DBMS instances (of different kinds) at the same time. To organize these, SQL
Exporter borrows Prometheus concepts such as job and instance: a job may e.g. cover all MySQL instances or all replicas
of a pricing database; and consists of a set of instances. Looking at it from the bottom up, SQL queries are grouped
into collectors -- logical groups of queries, e.g. _query stats_ or _I/O stats_, mapped to metrics they populate. They
may be generic DBMS-specific collectors (e.g. _MySQL user stats_) or custom, deployment specific metrics (e.g. _pricing
data quality_). Each job references a number of collectors; the queries defined by those collectors will be executed on
all instances of that job, generating the metrics mapped to them.

All exported metrics get `job` and `instance` automatic labels, as well as any custom target labels (e.g. `env="prod"`),
similarly to Prometheus targets.

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
`pricing_update_time{job="pricing_db",instance="dbserver1.example.com:1433",env="prod",market="US"}`. For a complete,
fully documented example see
[`documentation/sql_exporter.yml`](https://github.com/free/sql_exporter/tree/master/documentation/sql_exporter.yml).
You will find practical configuration examples and DBMS-specific "standard" collectors in the
[`examples`](https://github.com/free/sql_exporter/tree/master/examples) directory.

```yaml
# A SQL Exporter job is the equivalent of a Prometheus job: a set of related DB instances.
jobs:

  # All metrics for the targets defined here get a `job="pricing_db"` label.
  - job_name: pricing_db

    # Collectors (referenced by name) to be applied to all targets in this job.
    collectors: [stock_pricing_freshness]

    # Similar to Prometheus static_configs.
    static_configs:
      - targets:
          # Map of instance name (to export as instance label) to DSN
          'dbserver1.example.com:1433': 'sqlserver://prom_user:prom_password@dbserver1.example.com:1433'
          'dbserver2.example.com:1433': 'sqlserver://prom_user:prom_password@dbserver2.example.com:1433'
        labels:
          env: 'prod'

# Collector definitions. A collector is a set of related metrics/queries that are collected together.
collectors:

  # Collectors are referenced by name.
  - collector_name: stock_pricing_freshness

    # A Prometheus metric with (optional) additional labels, value and labels populated from one query.
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

### Prometheus Configuration

In Prometheus, SQL Exporter is configured similarly to a Prometheus Pushgateway, i.e. a regular target with
`honor_labels: true` set, so that the `job` and `instance` labels applied to metrics are kept, not overwritten with
the job and instance labels of the SQL Exporter instance. See the [Prometheus documentation](
https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config) for details.

```yaml
  - job_name: 'sql_exporter'
    honor_labels: true
    static_configs:
      - targets: ['localhost:9399']
        labels:
          # If SQL Exporter targets also have an `env` label defined, it will override this because of
          # `honor_labels`. This label will essentially only apply to sql_exporter's own metrics (e.g.
          # heap or CPU usage).
          env: 'prod'
```

### Data Source Names

To keep things simple and yet allow fully configurable DSNs to be configured, SQL Exporter expects each instance to be
specified as a mapping of instance name (`dbserver1.example.com:1433` above) to DSN
(`sqlserver://prom_user:prom_password@dbserver1.example.com:1433`). However, because the Go `sql` library does not allow
for automatic driver selection based on the DSN (i.e. an explicit driver name needs to be specified) SQL Exporter uses
the schema part of the DSN (the part before the `://`) to determine which driver to use.

Unfortunately, while this works out of the box with the [MS SQL Server](https://github.com/denisenkom/go-mssqldb) and
[PostgreSQL](github.com/lib/pq) drivers, the [MySQL driver](github.com/go-sql-driver/mysql) DSNs format does not include
a schema and the [Clickhouse](github.com/kshvakov/clickhouse) one uses `tcp://`. So SQL Exporter does a bit of massaging
of DSNs for the latter two drivers in order for this to work:


DB | SQL Exporter expected DSN | Driver sees
---|---|---
MySQL | `mysql://user:passw@protocol(host:port)/dbname` | `user:passw@protocol(host:port)/dbname`
PostgreSQL | `postgres://user:passw@host:port/dbname` | _unchanged_
SQL Server | `sqlserver://user:passw@host:port/instance` | _unchanged_
Clickhouse | `clickhouse://host:port?username=user&password=passw&database=dbname` | `tcp://host:port?username=user&password=passw&database=dbname`

## Why It Exists

SQL Exporter started off as an exporter for Microsoft SQL Server, for which no reliable exporters exist. But what is
the point of a configuration driven SQL exporter, if you're going to use it alongside 2 other exporters with wholly
different configurations, because you also need to monitor MySQL and PostgreSQL instances?

A couple of alternative database agnostic exporters are available -- https://github.com/justwatchcom/sql_exporter and
https://github.com/chop-dbhi/prometheus-sql -- but they both do the collection at fixed intervals, independent of
Prometheus scrapes. This is partly a philosophical issue, but practical issues are not all that difficult to imagine:
jitter, duplicate data points or collected but not scraped data points. The control they provide over which labels get
applied is limited and the base label set spammy.

Finally, in existing implementations configurations are not easily reused without copy-pasting and editing across
jobs/instances.
