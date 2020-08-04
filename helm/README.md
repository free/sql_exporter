# sql-exporter

![Version: 1.0.0](https://img.shields.io/badge/Version-1.0.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 1.0.0](https://img.shields.io/badge/AppVersion-1.0.0-informational?style=flat-square)

A Helm chart for SQL Metrics

**Homepage:** <https://github.com/free/sql_exporter>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| Patrick Domnick | patrickfdomnick@gmail.com |  |

## Source Code

* <https://github.com/free/sql_exporter>

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` |  |
| database.database | string | `""` |  |
| database.host | string | `""` |  |
| database.metrics | list | `[]` |  |
| database.password | string | `""` |  |
| database.port | string | `""` |  |
| database.protocol | string | `"mysql"` |  |
| database.user | string | `""` |  |
| deployment.port | int | `9399` |  |
| fullnameOverride | string | `""` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"githubfree/sql_exporter"` |  |
| image.tag | float | `0.5` |  |
| imagePullSecrets | object | `{}` |  |
| livenessProbe.failureThreshold | int | `6` |  |
| livenessProbe.initialDelaySeconds | int | `30` |  |
| livenessProbe.tcpSocket.port | string | `"http"` |  |
| livenessProbe.timeoutSeconds | int | `5` |  |
| nameOverride | string | `""` |  |
| nodeSelector | object | `{}` |  |
| podSecurityContext | object | `{}` |  |
| readinessProbe.initialDelaySeconds | int | `5` |  |
| readinessProbe.periodSeconds | int | `5` |  |
| readinessProbe.tcpSocket.port | string | `"http"` |  |
| readinessProbe.timeoutSeconds | int | `3` |  |
| replicaCount | int | `1` |  |
| resources.limits | object | `{}` |  |
| securityContext | object | `{}` |  |
| service.port | int | `9399` |  |
| service.type | string | `"ClusterIP"` |  |
| serviceMonitor.alerts.enabled | bool | `false` |  |
| serviceMonitor.alerts.rules | list | `[]` |  |
| serviceMonitor.enabled | bool | `false` |  |
| serviceMonitor.namespace | string | `"monitoring"` |  |
| serviceMonitor.path | string | `"/metrics"` |  |
| serviceMonitor.selector | object | `{}` |  |
| tolerations | list | `[]` |  |
