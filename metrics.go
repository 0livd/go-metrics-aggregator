package metricsaggregator

import (
	"fmt"
	"log"
	"math"
	"time"
)

type (
	MetricMetadata map[string]string
	Metric         struct {
		Name       string
		Time       time.Time
		Value      float64
		Metadata   MetricMetadata
		Unit       string
		Statistics struct{ SampleCount, Sum, Minimum, Maximum float64 }
	}
	metricBackend interface {
		Publish(*metricBatch) error
	}
)

var (
	granularity        = 60 * time.Second
	metricBatchSize    = 20
	maxMetricsInBuffer = 100
	namespacePrefix    = "go-metrics-"
)

type metricBatch []*Metric

func (mb *metricBatch) byName() map[string]metricBatch {
	metricsByName := map[string]metricBatch{}
	for _, metric := range *mb {
		metricsByName[metric.Name] = append(metricsByName[metric.Name], metric)
	}
	return metricsByName
}

func (mb metricBatch) String() string {
	var toJoin []Metric
	for _, m := range mb {
		toJoin = append(toJoin, *m)
	}
	return fmt.Sprintf("%+v", toJoin)
}

// aggregate aggregates metrics according to their name and metadata
func (mb *metricBatch) aggregate() metricBatch {
	// NOTE We suppose that metrics with the same name have the same unit
	var aggregatedMetrics metricBatch
	metricsByName := mb.byName()
	for name, metrics := range metricsByName {
		var indexesToSkip []int
	OUTER:
		for i, metric := range metrics {
			// Skip indexes that have already been processed
			for _, k := range indexesToSkip {
				if i == k {
					continue OUTER
				}
			}
			// Get a slice of metrics that have the same metadata and mark them to not be processed again
			sameMetadataMetrics := metricBatch{metric}
			if i+1 != len(metrics) {
				for j, m := range metrics[i+1:] {
					hasSameMetadata := true
					for k, v := range metric.Metadata {
						if m.Metadata[k] != v {
							hasSameMetadata = false
						}
					}
					if hasSameMetadata {
						indexesToSkip = append(indexesToSkip, i+1+j)
						sameMetadataMetrics = append(sameMetadataMetrics, m)
					}
				}
			}
			// Build stats
			sampleCount := float64(len(sameMetadataMetrics))
			var sum float64
			var min = math.MaxFloat64
			var max = math.MaxFloat64 * -1
			var sumTime int64
			for _, m := range sameMetadataMetrics {
				sum += m.Value
				if m.Value < min {
					min = m.Value
				}
				if m.Value > max {
					max = m.Value
				}
				sumTime += m.Time.Unix()
			}
			avTime := time.Unix(sumTime/int64(len(sameMetadataMetrics)), 0).UTC()
			aggregatedMetrics = append(aggregatedMetrics, &Metric{
				Name:     name,
				Time:     avTime,
				Value:    0,
				Metadata: metric.Metadata,
				Unit:     metric.Unit,
				Statistics: struct{ SampleCount, Sum, Minimum, Maximum float64 }{
					SampleCount: sampleCount,
					Sum:         sum,
					Minimum:     min,
					Maximum:     max,
				},
			})
		}
	}
	return aggregatedMetrics
}

// snapshotAndReset returns a copy of the batch and then resets it
func (mb *metricBatch) snapshotAndReset() *metricBatch {
	metrics := *mb
	*mb = metricBatch{}
	return &metrics
}

// truncate returns the first chunckSize metrics of the batch and then removes them from the batch
func (mb *metricBatch) truncate(chunckSize int) *metricBatch {
	var metricsChunck = &metricBatch{}
	if len(*mb) > chunckSize {
		*metricsChunck, *mb = (*mb)[:chunckSize], (*mb)[chunckSize:]
	} else {
		metricsChunck = mb.snapshotAndReset()
	}
	return metricsChunck
}

type MetricsHandler struct {
	incomingMetrics chan *Metric
	metrics         metricBatch
	ticker          *time.Ticker
	metricBackend   metricBackend
}

// NewMetricsHandler creates a metrichandler and launches its run method in a goroutine
func NewMetricsHandler(metricBackend metricBackend, sendInterval time.Duration) *MetricsHandler {
	metricsHandler := &MetricsHandler{
		incomingMetrics: make(chan *Metric),
		ticker:          time.NewTicker(sendInterval),
		metricBackend:   metricBackend,
	}
	go metricsHandler.run()
	return metricsHandler
}

func (mh *MetricsHandler) run() {
	for {
		select {
		case metric := <-mh.incomingMetrics:
			mh.metrics = append(mh.metrics, metric)
			if len(mh.metrics) >= maxMetricsInBuffer {
				mh.sendMetrics()
			}
		case <-mh.ticker.C:
			if len(mh.metrics) != 0 {
				mh.sendMetrics()
			}
		}
	}
}

func (mh *MetricsHandler) Add(metric *Metric) {
	mh.incomingMetrics <- metric
}

func (mh *MetricsHandler) sendMetrics() {
	metricsToSend := mh.metrics.snapshotAndReset().aggregate()
	for len(metricsToSend) > 0 {
		truncatedMetrics := metricsToSend.truncate(metricBatchSize)
		go func(metrics *metricBatch) {
			if err := mh.metricBackend.Publish(metrics); err != nil {
				log.Printf("Could not send metrics to metric backend with the following error %v", err)
			}
		}(truncatedMetrics)
	}
}
