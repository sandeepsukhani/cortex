package purger

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/util"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/weaveworks/common/user"
)

type deleteRequestHandlerMetrics struct {
	deleteRequestsReceivedTotal *prometheus.CounterVec
}

func newDeleteRequestHandlerMetrics(r prometheus.Registerer) *deleteRequestHandlerMetrics {
	m := deleteRequestHandlerMetrics{}

	m.deleteRequestsReceivedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "purger_delete_requests_received_total",
		Help:      "Number of delete requests received per user",
	}, []string{"user"})

	if r != nil {
		r.MustRegister(
			m.deleteRequestsReceivedTotal,
		)
	}

	return &m
}

// DeleteRequestHandler provides handlers for delete requests
type DeleteRequestHandler struct {
	deleteStore *DeleteStore
	metrics     *deleteRequestHandlerMetrics
}

// NewDeleteRequestHandler creates a DeleteRequestHandler
func NewDeleteRequestHandler(deleteStore *DeleteStore, registerer prometheus.Registerer) *DeleteRequestHandler {
	deleteMgr := DeleteRequestHandler{
		deleteStore: deleteStore,
		metrics:     newDeleteRequestHandlerMetrics(registerer),
	}

	return &deleteMgr
}

// AddDeleteRequestHandler handles addition of new delete request
func (dm *DeleteRequestHandler) AddDeleteRequestHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	params := r.URL.Query()
	match := params["match[]"]
	if len(match) == 0 {
		http.Error(w, "selectors not set", http.StatusBadRequest)
		return
	}

	for i := range match {
		_, err := promql.ParseMetricSelector(match[i])
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	startParam := params.Get("start")
	startTime := int64(0)
	if startParam != "" {
		startTime, err = util.ParseTime(startParam)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	endParam := params.Get("end")
	endTime := int64(model.Now())

	if endParam != "" {
		endTime, err = util.ParseTime(endParam)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if endTime > int64(model.Now()) {
			http.Error(w, "deletes in future not allowed", http.StatusBadRequest)
			return
		}
	}

	if startTime > endTime {
		http.Error(w, "start time can't be greater than end time", http.StatusBadRequest)
		return
	}

	if err := dm.deleteStore.AddDeleteRequest(ctx, userID, model.Time(startTime), model.Time(endTime), match); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	dm.metrics.deleteRequestsReceivedTotal.WithLabelValues(userID).Inc()
}

// GetAllDeleteRequestsHandler handles get all delete requests
func (dm *DeleteRequestHandler) GetAllDeleteRequestsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	deleteRequests, err := dm.deleteStore.GetAllDeleteRequestsForUser(ctx, userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(deleteRequests); err != nil {
		http.Error(w, fmt.Sprintf("Error marshalling response: %v", err), http.StatusInternalServerError)
	}
}
