package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type MockMetricsRegistry struct {
	reg *prometheus.Registry
}

func (s *MockMetricsRegistry) Factory() promauto.Factory {
	return promauto.With(s.reg)
}

func NewMockMetricsRegistry() *MockMetricsRegistry {
	return &MockMetricsRegistry{
		reg: prometheus.NewRegistry(),
	}
}
