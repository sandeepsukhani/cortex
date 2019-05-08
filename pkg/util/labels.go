package util

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
)

// LabelsToMetric converts a Labels to Metric
func LabelsToMetric(ls labels.Labels) model.Metric {
	m := make(model.Metric, len(ls))
	for _, l := range ls {
		m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}
	return m
}
