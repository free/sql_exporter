# Prometheus SQL Exporter [![Build Status](https://travis-ci.org/free/sql_exporter.svg)](https://travis-ci.org/free/sql_exporter) [![Go Report Card](https://goreportcard.com/badge/github.com/free/sql_exporter)](https://goreportcard.com/report/github.com/free/sql_exporter) [![GoDoc](https://godoc.org/github.com/free/sql_exporter?status.svg)](https://godoc.org/github.com/free/sql_exporter)

Database agnostic SQL exporter for [Prometheus](https://prometheus.io).

## Overview

SQL Exporter is a configuration driven exporter that exposes metrics gathered from DBMSs, for use by the Prometheus
monitoring system. Out of the box, it provides support for MySQL, PostgreSQL, Microsoft SQL Server and Clickhouse, but
any DBMS for which a Go driver is available may be monitored after rebuilding the binary with the DBMS driver included.

The collected metrics and the queries behind them are entirely configuration defined. SQL queries are grouped into
collectors -- logical groups of queries, e.g. *query stats* or *I/O stats*, mapped to the metrics they populate. They
may be DBMS-specific collectors (e.g. *MySQL InnoDB stats*) or custom, deployment specific metrics (e.g. *pricing data
freshness*). This means you can quickly and easily set up custom collectors to measure data quality, whatever that might
mean in your specific case.

Per the Prometheus philosophy, scrapes are synchronous (metrics are collected on every `/metrics` poll) but, in order to
reduce load, minimum collection intervals may optionally be set per collector, producing cached metrics when queried
more frequently than the configured interval.

## Usage

Get Prometheus SQL Exporter, either as a [packaged release](https://github.com/free/sql_exporter/releases/latest) or
build it yourself:

```
$ go install github.com/free/sql_exporter/cmd/sql_exporter
```

then run it from the command line:

```
$ sql_exporter
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

This document describes the concepts and uses a few bare bones configuration examples to illustrate them.
For comprehensive, documented configuration files check out the
[`documentation`](https://github.com/free/sql_exporter/tree/master/documentation) directory.
You will find ready to use "standard" DBMS-specific collector definitions in the
[`examples`](https://github.com/free/sql_exporter/tree/master/examples) directory.

There are two configurations in which SQL Exporter may be deployed. One is the Prometheus standard *exporter as agent*,
alongside the monitored DB server. The other is a multi-target, hub like deployment, with SQL Exporter polling multiple
DBMS instances of different types and with different uses concurrently, useful for test and development.

### Collectors

SQL Exporter uses YAML for configuration. Collectors may be defined inline, in the exporter configuration file, under
`collectors`, or they may be defined in separate files and referenced in the exporter configuration by name, making them
easy to share and reuse.

The collector definition below generates gauge metrics of the form `pricing_update_time{market="US"}`.

**`./pricing_data_freshness.collector.yml`**

```yaml
# This collector will be referenced in the exporter configuration as `pricing_data_freshness`.
collector_name: pricing_data_freshness

# A Prometheus metric with (optional) additional labels, value and labels populated from one query.
metrics:
  - metric_name: pricing_update_time
    type: gauge
    help: 'Time when prices for a market were last updated.'
    key_labels:
      # Populated from the `market` column of each row.
      - Market
    values: [LastUpdateTime]
    query: |
      SELECT Market, max(UpdateTime) AS LastUpdateTime
      FROM MarketPrices
      GROUP BY market
```

### Single Target Deployment

In this configuration, one SQL Exporter instance gets deployed alongside each monitored DB server. Only collector
defined metrics, no SQL Exporter process metrics are exported. If the database is down, `/metrics` will respond with
HTTP code 500 Internal Server Error (causing Prometheus to record `up=0` for the scrape).

**`./sql_exporter.yml`**

```yaml
# Global settings and defaults.
global:
  # Prometheus times out scrapes after 10s by default, give ourselves a bit of headroom.
  # Make sure this is shorter than Prometheus' scrape_timeout or a slow DB may appear to be down when
  # collection takes longer.
  scrape_timeout: 9s
  # Minimum interval between collector runs: by default (0s) collectors are executed on every scrape.
  min_interval: 0s
  # Maximum number of open connections to any one target. Metric queries will run concurrently on
  # multiple connections.
  max_connections: 1
  # Maximum number of idle connections to any one target.
  max_idle_connections: 1

# The target to monitor and the list of collectors to execute on it.
target:
  # Data source name always has a URI schema that matches the driver name. In some cases (e.g. MySQL)
  # the schema gets dropped or replaced to match the driver expected DSN format.
  data_source_name: 'sqlserver://prom_user:prom_password@dbserver1.example.com:1433'

  # Collectors (referenced by name) to execute on the target.
  collectors: [pricing_data_freshness]

# Collector definition files.
collector_files: 
  - "*.collector.yml"
```

### Multi-Target

**Note:** *While SQL Exporter itself will run just as reliably in multi-target mode, there are constraints (such as one
single, global `scrape_timeout` value) which limit its use to test and development.*

For multi-target deployment, SQL Exporter borrows Prometheus concepts such as job and instance: a job may e.g. cover all
MySQL instances or all replicas of a pricing database; and consists of a set of instances. Each job references by name
a number of collectors: the queries defined by those collectors are executed on all instances belonging to the job.

All collector defined metrics get `job` and `instance` automatic labels, as well as any custom target labels (e.g.
`env="prod"`) applied, similar to Prometheus `static_configs`. In addition to these, synthetic `up` and
`scrape_duration` metrics are generated for each target. And SQL Exporter exports its own process metrics.

**`./sql_exporter.yml`**

```yaml
# A SQL Exporter job is the equivalent of a Prometheus job: a set of related DB instances.
jobs:

  # All metrics for the targets defined here get a `job="pricing_db"` label.
  - job_name: pricing_db

    # Collectors (referenced by name) to execute on all targets in this job.
    collectors: [pricing_data_freshness]

    # Similar to Prometheus static_configs.
    static_configs:
      - targets:
          # Map of instance name (exported as instance label) to DSN
          'dbserver1.example.com:1433': 'sqlserver://prom_user:prom_password@dbserver1.example.com:1433'
          'dbserver2.example.com:1433': 'sqlserver://prom_user:prom_password@dbserver2.example.com:1433'
        labels:
          env: 'prod'

# Collector definition files.
collector_files: 
  - "*.collector.yml"
```

In Prometheus, a multi-target SQL Exporter is configured similarly to a Prometheus Pushgateway, i.e. a regular target
with `honor_labels: true` set, so that the `job` and `instance` labels applied to metrics are kept, not overwritten with
the job and instance labels of the SQL Exporter instance. See the [Prometheus documentation](
https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config) for details.

**`./prometheus.yml`** [snippet]

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

To keep things simple and yet allow fully configurable database connections to be set up, SQL Exporter uses DSNs (like
`sqlserver://prom_user:prom_password@dbserver1.example.com:1433`) to refer to database instances. However, because the
Go `sql` library does not allow for automatic driver selection based on the DSN (i.e. an explicit driver name must be
specified) SQL Exporter uses the schema part of the DSN (the part before the `://`) to determine which driver to use.

Unfortunately, while this works out of the box with the [MS SQL Server](https://github.com/denisenkom/go-mssqldb) and
[PostgreSQL](github.com/lib/pq) drivers, the [MySQL driver](github.com/go-sql-driver/mysql) DSNs format does not include
a schema and the [Clickhouse](github.com/kshvakov/clickhouse) one uses `tcp://`. So SQL Exporter does a bit of massaging
of DSNs for the latter two drivers in order for this to work:


DB | SQL Exporter expected DSN | Driver sees
---|---|---
MySQL | `mysql://user:passw@protocol(host:port)/dbname` | `user:passw@protocol(host:port)/dbname`
PostgreSQL | `postgres://user:passw@host:port/dbname` | *unchanged*
SQL Server | `sqlserver://user:passw@host:port/instance` | *unchanged*
Clickhouse | `clickhouse://host:port?username=user&password=passw&database=dbname` | `tcp://host:port?username=user&password=passw&database=dbname`

## Why It Exists

SQL Exporter started off as an exporter for Microsoft SQL Server, for which no reliable exporters exist. But what is
the point of a configuration driven SQL exporter, if you're going to use it along with 2 more exporters with wholly
different world views and configurations, because you also have MySQL and PostgreSQL instances to monitor?

A couple of alternative database agnostic exporters are available -- https://github.com/justwatchcom/sql_exporter and
https://github.com/chop-dbhi/prometheus-sql -- but they both do the collection at fixed intervals, independent of
Prometheus scrapes. This is partly a philosophical issue, but practical issues are not all that difficult to imagine:
jitter; duplicate data points; or collected but not scraped data points. The control they provide over which labels get
applied is limited, and the base label set spammy. And finally, configurations are not easily reused without
copy-pasting and editing across jobs and instances.
