# Prometheus Database Exporter

[![Build Status](https://travis-ci.org/Corundex/database_exporter.svg?branch=master)](https://travis-ci.org/Corundex/database_exporter)
[![Go Report Card](https://goreportcard.com/badge/github.com/Corundex/database_exporter)](https://goreportcard.com/report/github.com/Corundex/database_exporter)
[![GoDoc](https://godoc.org/github.com/Corundex/database_exporter?status.svg)](https://godoc.org/github.com/Corundex/database_exporter)
[![Docker Pulls](https://img.shields.io/docker/pulls/corund/database_exporter.svg?maxAge=0)](https://hub.docker.com/r/corund/database_exporter/)

Database agnostic SQL exporter for [Prometheus](https://prometheus.io).

## Overview

Database Exporter is a configuration driven exporter that exposes metrics gathered from DBMSs, for use by the Prometheus
monitoring system. Out of the box, it provides support for MySQL, PostgreSQL, Oracle DB, Microsoft SQL Server and Clickhouse, but
any DBMS for which a Go driver is available may be monitored after rebuilding the binary with the DBMS driver included.

The collected metrics and the queries that produce them are entirely configuration defined. SQL queries are grouped into
collectors -- logical groups of queries, e.g. *query stats* or *I/O stats*, mapped to the metrics they populate.
Collectors may be DBMS-specific (e.g. *MySQL InnoDB stats*) or custom, deployment specific (e.g. *pricing data
freshness*). This means you can quickly and easily set up custom collectors to measure data quality, whatever that might
mean in your specific case.

Per the Prometheus philosophy, scrapes are synchronous (metrics are collected on every `/metrics` poll) but, in order to
keep load at reasonable levels, minimum collection intervals may optionally be set per collector, producing cached
metrics when queried more frequently than the configured interval.

## Usage

Get Prometheus Database Exporter [packaged release](https://github.com/Corundex/database_exporter/releases/latest)
or build it yourself:

```bash
go build github.com/Corundex/database_exporter
```

then run it from the command line:

```bash
./database_exporter
```

Use the `-help` flag to get help information.

```bash
./database_exporter -help
```

Usage of ./database_exporter:

```yaml
  -config.file string
      Database Exporter configuration file name. (default "database_exporter.yml", you can use sample oracle_exporter.yml, postgres_exporter.yml, mssql_exporter.yml or mysql_exporter.yml)
  -web.listen-address string
      Address to listen on for web interface and telemetry. (default ":9285")
  -web.metrics-path string
      Path under which to expose metrics. (default "/metrics")
  [...]
```

## Configuration

Database Exporter is deployed alongside the DB server it collects metrics from. If both the exporter and the DB
server are on the same host, they will share the same failure domain: they will usually be either both up and running
or both down. When the database is unreachable, `/metrics` responds with HTTP code 500 Internal Server Error, causing
Prometheus to record `up=0` for that scrape. Only metrics defined by collectors are exported on the `/metrics` endpoint.
Database Exporter process metrics are exported at `/database_exporter_metrics`.

The configuration examples listed here only cover some basic element.
[`documentation/database_exporter.yml`](https://github.com/Corundex/database_exporter/tree/master/documentation/database_exporter.yml).
You will find ready to use "standard" DBMS-specific collector definitions in the
[`examples`](https://github.com/Corundex/database_exporter/tree/master/examples) directory. You may contribute your own collector
definitions and metric additions if you think they could be more widely useful, even if they are merely different takes
on already covered DBMSs.

**`./database_exporter.yml`**

```yaml
# Global settings and defaults.
global:
  # Subtracted from Prometheus' scrape_timeout to give us some headroom and prevent Prometheus from
  # timing out first.
  scrape_timeout_offset: 500ms
  # Minimum interval between collector runs: by default (0s) collectors are executed on every scrape.
  min_interval: 0s
  # Maximum number of open connections to any one target. Metric queries will run concurrently on
  # multiple connections.
  max_connections: 3
  # Maximum number of idle connections to any one target.
  max_idle_connections: 3

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

### Collectors

Collectors may be defined inline, in the exporter configuration file, under `collectors`, or they may be defined in
separate files and referenced in the exporter configuration by name, making them easy to share and reuse.

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
      GROUP BY Market
```

### Data Source Names

To keep things simple and yet allow fully configurable database connections to be set up, Database Exporter uses DSNs (like
`sqlserver://prom_user:prom_password@dbserver1.example.com:1433`) to refer to database instances. However, because the
Go `sql` library does not allow for automatic driver selection based on the DSN (i.e. an explicit driver name must be
specified) Database Exporter uses the schema part of the DSN (the part before the `://`) to determine which driver to use.

While this works out of the box with the [MS SQL Server](https://github.com/denisenkom/go-mssqldb) and
[PostgreSQL](https://github.com/lib/pq) drivers, [Oracle OCI8](https://github.com/mattn/go-oci8) and [MySQL driver](https://github.com/go-sql-driver/mysql) DSNs format does not include
a schema and the [Clickhouse](https://github.com/kshvakov/clickhouse) one uses `tcp://`. So Database Exporter does a bit of massaging
of DSNs for the latter two drivers in order for this to work:

DB | Database Exporter expected DSN | Driver sees
:---|:---|:---
MySQL | `mysql://user:passw@protocol(host:port)/dbname` | `user:passw@protocol(host:port)/dbname`
Oracle | `oracle://user/password@host:port/sid` | `user/password@host:port/sid`
PostgreSQL | `postgres://user:passw@host:port/dbname` | *unchanged*
SQL Server | `sqlserver://user:passw@host:port/instance` | *unchanged*
SQLite3 | `sqlite3://file:mybase.db?cache=shared&mode=rwc` | `file:mybase.db?cache=shared&mode=rwc`
in-memory SQLite3 | `sqlite3://file::memory:?mode=memory&cache=shared` | `file::memory:?mode=memory&cache=shared`
Clickhouse | `clickhouse://host:port?username=user&password=passw&database=db` | `tcp://host:port?username=user&password=passw&database=db`
Couchbase instance | `n1ql://host:port@creds=[{"user":"Administrator","pass":"admin123"}]@timeout=10s` | `host:port`
Couchbase cluster | `n1ql://http://host:port/@creds=[{"user":"Administrator","pass":"admin123"}]@timeout=10s` | `http://host:port/`

## Why It Exists

Database exporter started from [SQL Exporter](https://github.com/free/sql_exporter) which started off as an exporter for Microsoft SQL Server, for which no reliable exporters exist. But what is the point of a configuration driven Database exporter, if you're going to use it along with 2 more exporters with wholly
different world views and configurations, because you also have MySQL, Oracle and PostgreSQL instances to monitor?

A couple of alternative database agnostic exporters are available:
[database_exporter](https://github.com/justwatchcom/database_exporter)
[prometheus-sql](https://github.com/chop-dbhi/prometheus-sql)
but they both do the collection at fixed intervals, independent of
Prometheus scrapes. This is partly a philosophical issue, but practical issues are not all that difficult to imagine:
jitter; duplicate data points; or collected but not scraped data points. The control they provide over which labels get
applied is limited, and the base label set spammy. And finally, configurations are not easily reused without
copy-pasting and editing across jobs and instances.
