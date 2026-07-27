{{/*
Expand the name of the chart.
*/}}
{{- define "prism-alert.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS label spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "prism-alert.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "prism-alert.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "prism-alert.labels" -}}
helm.sh/chart: {{ include "prism-alert.chart" . }}
{{ include "prism-alert.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "prism-alert.selectorLabels" -}}
app.kubernetes.io/name: {{ include "prism-alert.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "prism-alert.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "prism-alert.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Parse a listen address (:8080 or host:port) into a TCP port number string.
*/}}
{{- define "prism-alert.portFromAddr" -}}
{{- $addr := . -}}
{{- if not $addr -}}
8080
{{- else if hasPrefix ":" $addr -}}
{{- trimPrefix ":" $addr -}}
{{- else -}}
{{- (splitList ":" $addr | last) -}}
{{- end -}}
{{- end }}

{{/*
Health/probe HTTP port derived from env.listenAddr.
*/}}
{{- define "prism-alert.publicPort" -}}
{{- include "prism-alert.portFromAddr" .Values.env.listenAddr -}}
{{- end }}

{{/*
Image reference with tag defaulting to appVersion.
*/}}
{{- define "prism-alert.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end }}

{{/*
Name of the ConfigMap holding rule YAML: an externally-managed one when
rulesConfigMap is set, otherwise the chart-rendered "<fullname>-rules".
*/}}
{{- define "prism-alert.rulesConfigMapName" -}}
{{- if .Values.rulesConfigMap -}}
{{- .Values.rulesConfigMap -}}
{{- else -}}
{{- printf "%s-rules" (include "prism-alert.fullname" .) -}}
{{- end -}}
{{- end }}

{{/*
Whether any rule source is mounted (inline rules or an external ConfigMap).
*/}}
{{- define "prism-alert.hasRules" -}}
{{- if or .Values.rulesConfigMap .Values.rules -}}true{{- end -}}
{{- end }}
