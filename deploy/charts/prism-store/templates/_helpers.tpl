{{/*
Expand the name of the chart.
*/}}
{{- define "prism-store.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS label spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "prism-store.fullname" -}}
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
{{- define "prism-store.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "prism-store.labels" -}}
helm.sh/chart: {{ include "prism-store.chart" . }}
{{ include "prism-store.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "prism-store.selectorLabels" -}}
app.kubernetes.io/name: {{ include "prism-store.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "prism-store.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "prism-store.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Parse a listen address (:8080 or host:port) into a TCP port number string.
*/}}
{{- define "prism-store.portFromAddr" -}}
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
Public HTTP port derived from env.listenAddr.
*/}}
{{- define "prism-store.publicPort" -}}
{{- include "prism-store.portFromAddr" .Values.env.listenAddr -}}
{{- end }}

{{/*
Admin HTTP port derived from env.adminListenAddr (empty when combined plane).
*/}}
{{- define "prism-store.adminPort" -}}
{{- if .Values.env.adminListenAddr -}}
{{- include "prism-store.portFromAddr" .Values.env.adminListenAddr -}}
{{- end -}}
{{- end }}

{{/*
Image reference with tag defaulting to appVersion.
*/}}
{{- define "prism-store.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end }}
