{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "sql-exporter.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "sql-exporter.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "sql-exporter.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "sql-exporter.labels" -}}
helm.sh/chart: {{ include "sql-exporter.chart" . }}
{{ include "sql-exporter.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "sql-exporter.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sql-exporter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the sql_export.yml file
*/}}
{{- define "sql-exporter.expoter" -}}
{{- print "" -}}
{{- print "global:" | nindent 4 -}}
{{- print "scrape_timeout_offset: 500ms" | nindent 6 -}}
{{- print "min_interval: 0s" | nindent 6 -}}
{{- print "max_connections: 3" | nindent 6 -}}
{{- print "max_idle_connections: 3" | nindent 6 -}}
{{- print "target:" | nindent 4 -}}
{{- if eq .Values.database.protocol "mysql" -}}
{{- printf "data_source_name: %s://%s:%s@(%s:%s)/%s" .Values.database.protocol .Values.database.user .Values.database.password .Values.database.host .Values.database.port .Values.database.database | nindent 6 -}}
{{- end -}}
{{- if or (eq .Values.database.protocol "postgres") (eq .Values.database.protocol "sqlserver") -}}
{{- printf "data_source_name: %s://%s:%s@%s:%s/%s" .Values.database.protocol .Values.database.user .Values.database.password .Values.database.host .Values.database.port .Values.database.database | nindent 6 -}}
{{- end -}}
{{- if eq .Values.database.protocol "clickhouse" -}}
{{- printf "data_source_name: %s://%s:%s?username=%s&password=%s&dbname=%s" .Values.database.protocol .Values.database.host .Values.database.port .Values.database.user .Values.database.password  .Values.database.database | nindent 6 -}}
{{- end -}}
{{- print "collectors: [sql_standard]" | nindent 6 -}}
{{- print "collector_files:" | nindent 4 -}}
{{- print "- /bin/config/standard.collector.yml" | nindent 6 -}}
{{- end -}}

{{/*
Create the standard.collector.yml file
*/}}
{{- define "sql-exporter.collector" -}}
{{- print "" -}}
{{- print "collector_name: sql_standard" | nindent 4 -}}
{{- print "metrics: " | nindent 4 -}}
{{ .Values.database.metrics | toYaml | nindent 6}}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "sql-exporter.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{ default (include "sql-exporter.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}
