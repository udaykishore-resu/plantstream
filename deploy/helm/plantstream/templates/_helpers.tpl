{{- define "plantstream.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "plantstream.fullname" -}}
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

{{- define "plantstream.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "plantstream.labels" -}}
helm.sh/chart: {{ include "plantstream.chart" . }}
{{ include "plantstream.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: plantstream
{{- end }}

{{- define "plantstream.selectorLabels" -}}
app.kubernetes.io/name: {{ include "plantstream.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "plantstream.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "plantstream.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "plantstream.plantConfigMap" -}}
{{- if .Values.plant.existingConfigMap }}{{ .Values.plant.existingConfigMap }}{{ else }}{{ include "plantstream.fullname" . }}-plant{{ end }}
{{- end }}
