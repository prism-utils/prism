package query

import (
	"strings"
	"sync"

	"github.com/elk-utilities/prism/internal/store/logmeta"
)

// lastLabelValuesObservation records how label values were resolved (tests only).
var lastLabelValuesObservation struct {
	mu     sync.Mutex
	source string // "index" or "scan"
	sql    string
}

func recordLabelValuesObservation(source, sql string) {
	lastLabelValuesObservation.mu.Lock()
	lastLabelValuesObservation.source = source
	lastLabelValuesObservation.sql = sql
	lastLabelValuesObservation.mu.Unlock()
}

// LabelValuesObservationForTest returns the source and SQL from the last label_values call.
func LabelValuesObservationForTest() (source, sql string) {
	lastLabelValuesObservation.mu.Lock()
	defer lastLabelValuesObservation.mu.Unlock()
	return lastLabelValuesObservation.source, lastLabelValuesObservation.sql
}

// labelValuesFromIndex serves indexed labels without touching the message column.
func labelValuesFromIndex(dataDir, tenant, name string, limit int) ([]string, error) {
	recordLabelValuesObservation("index", "")
	return logmeta.LabelValues(dataDir, tenant, name, limit)
}

func labelValuesSQLWouldReferenceMessage(sql string) bool {
	return strings.Contains(strings.ToLower(sql), "message")
}

func selectorAllowsLabelIndex(q *logQLQuery) bool {
	if q == nil {
		return true
	}
	return len(q.matchers) == 0 && len(q.filters) == 0
}
