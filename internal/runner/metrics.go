package runner

import "github.com/prometheus/client_golang/prometheus"

// Metrics are upkeeper's Prometheus metrics.
type Metrics struct {
	Worked  *prometheus.CounterVec
	Wakes   prometheus.Counter
	Patrols prometheus.Counter
	Steps   prometheus.Histogram
	Tokens  prometheus.Counter
}

// NewMetrics registers the metrics with reg, when it isn't nil.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Worked:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "upkeeper_investigations_total", Help: "Investigations worked, by how they ended."}, []string{"outcome"}),
		Wakes:   prometheus.NewCounter(prometheus.CounterOpts{Name: "upkeeper_gpu_wakes_total", Help: "Times upkeeper woke the GPU."}),
		Patrols: prometheus.NewCounter(prometheus.CounterOpts{Name: "upkeeper_patrols_total", Help: "Model patrols run."}),
		Steps:   prometheus.NewHistogram(prometheus.HistogramOpts{Name: "upkeeper_steps", Help: "Model steps per investigation.", Buckets: []float64{2, 5, 10, 15, 20, 30, 50}}),
		Tokens:  prometheus.NewCounter(prometheus.CounterOpts{Name: "upkeeper_tokens_total", Help: "Model tokens used."}),
	}
	if reg != nil {
		reg.MustRegister(m.Worked, m.Wakes, m.Patrols, m.Steps, m.Tokens)
	}
	return m
}
