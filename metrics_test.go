package metricsaggregator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type DummyBackend struct {
	receivedBatch chan *metricBatch
}

const (
	bufferSize = iota
	timer
)

func (db *DummyBackend) Publish(metricBatch *metricBatch) error {
	db.receivedBatch <- metricBatch
	return nil
}

func TestAggregation(t *testing.T) {
	ti, _ := time.Parse(time.RFC3339, "2014-11-12T11:45:26.0Z")
	t1, _ := time.Parse(time.RFC3339, "2014-11-12T11:45:30.0Z")
	t2, _ := time.Parse(time.RFC3339, "2014-11-12T11:45:28.0Z")
	t3, _ := time.Parse(time.RFC3339, "2014-11-12T11:45:27.0Z")
	handlerTest := []struct {
		Name string
		// Inputs
		currentTime time.Time
		inputBatch  *metricBatch
		// Expected results
		expected metricBatch
	}{
		{
			Name: "Multiple metrics, same name and metadata",
			inputBatch: &metricBatch{
				&Metric{
					Name:  "testMetric2",
					Time:  ti,
					Value: 1.0,
					Metadata: MetricMetadata{
						"mdata1": "data1",
					},
					Unit: "Count",
				},
				&Metric{
					Name:  "testMetric2",
					Time:  t1,
					Value: 19.0,
					Metadata: MetricMetadata{
						"mdata1": "data1",
					},
					Unit: "Count",
				},
			},
			expected: metricBatch{
				&Metric{
					Name: "testMetric2",
					Time: t2,
					Metadata: MetricMetadata{
						"mdata1": "data1",
					},
					Unit: "Count",
					Statistics: struct {
						SampleCount float64
						Sum         float64
						Minimum     float64
						Maximum     float64
					}{
						SampleCount: 2,
						Sum:         20,
						Minimum:     1,
						Maximum:     19,
					},
				},
			},
		},
		{
			Name: "Multiple metrics, different name and metadata",
			inputBatch: &metricBatch{
				&Metric{
					Name:  "testMetric1",
					Time:  ti,
					Value: 14.0,
					Metadata: MetricMetadata{
						"mdata1": "data2",
					},
					Unit: "Count",
				},
				&Metric{
					Name:  "testMetric3",
					Time:  t1,
					Value: 17.0,
					Metadata: MetricMetadata{
						"mdata1": "data2",
					},
					Unit: "Count",
				},
				&Metric{
					Name:  "testMetric3",
					Time:  t2,
					Value: 89.0,
					Metadata: MetricMetadata{
						"mdata1": "data1",
					},
					Unit: "Count",
				},
				&Metric{
					Name:  "testMetric3",
					Time:  ti,
					Value: 19.0,
					Metadata: MetricMetadata{
						"mdata1": "data1",
					},
					Unit: "Count",
				},
			},
			expected: metricBatch{
				&Metric{
					Name: "testMetric3",
					Time: t1,
					Metadata: MetricMetadata{
						"mdata1": "data2",
					},
					Unit: "Count",
					Statistics: struct {
						SampleCount float64
						Sum         float64
						Minimum     float64
						Maximum     float64
					}{
						SampleCount: 1,
						Sum:         17,
						Minimum:     17,
						Maximum:     17,
					},
				},
				&Metric{
					Name: "testMetric3",
					Time: t3,
					Metadata: MetricMetadata{
						"mdata1": "data1",
					},
					Unit: "Count",
					Statistics: struct {
						SampleCount float64
						Sum         float64
						Minimum     float64
						Maximum     float64
					}{
						SampleCount: 2,
						Sum:         108,
						Minimum:     19,
						Maximum:     89,
					},
				},
				&Metric{
					Name: "testMetric1",
					Time: ti,
					Metadata: MetricMetadata{
						"mdata1": "data2",
					},
					Unit: "Count",
					Statistics: struct {
						SampleCount float64
						Sum         float64
						Minimum     float64
						Maximum     float64
					}{
						SampleCount: 1,
						Sum:         14,
						Minimum:     14,
						Maximum:     14,
					},
				},
			},
		},
	}
	for _, test := range handlerTest {
		aggrBatch := test.inputBatch.aggregate()
		// Fail if we got a nil result but expected something
		if aggrBatch == nil {
			if test.expected != nil {
				t.Errorf("%s test failed got %+v and expected %+v", test.Name, nil, test.expected)
			}
			continue
		}
		// Loop on results and use ObjectsAreEqual to avoid ordering issues
		for _, m := range aggrBatch {
			failed := true
			for _, mm := range test.expected {
				if assert.ObjectsAreEqual(m, mm) {
					failed = false
					break
				}
			}
			if failed {
				t.Errorf("%s test failed got %+v and expected %+v", test.Name, aggrBatch, test.expected)
			}
		}
	}
}

func TestMetricsHandler(t *testing.T) {
	ti, _ := time.Parse(time.RFC3339, "2014-11-12T11:45:26.0Z")
	t1, _ := time.Parse(time.RFC3339, "2014-11-12T11:45:30.0Z")
	t2, _ := time.Parse(time.RFC3339, "2014-11-12T11:45:28.0Z")
	t3, _ := time.Parse(time.RFC3339, "2014-11-12T11:45:27.0Z")
	handlerTest := []struct {
		Name   string
		method int
		// Inputs
		currentTime time.Time
		inputBatch  *metricBatch
		// Expected results
		expected metricBatch
	}{
		{
			Name:   "Single metric",
			method: timer,
			inputBatch: &metricBatch{
				&Metric{
					Name:     "testMetric1",
					Time:     ti,
					Value:    1.0,
					Metadata: MetricMetadata{},
					Unit:     "Count",
				},
			},
			expected: metricBatch{
				&Metric{
					Name:     "testMetric1",
					Time:     ti,
					Metadata: MetricMetadata{},
					Unit:     "Count",

					Statistics: struct {
						SampleCount float64
						Sum         float64
						Minimum     float64
						Maximum     float64
					}{
						SampleCount: 1,
						Sum:         1,
						Minimum:     1,
						Maximum:     1,
					},
				},
			},
		},
		{
			Name:   "Multiple metrics",
			method: bufferSize,
			inputBatch: &metricBatch{
				&Metric{
					Name:  "testMetric1",
					Time:  ti,
					Value: 14.0,
					Metadata: MetricMetadata{
						"mdata1": "data2",
					},
					Unit: "Count",
				},
				&Metric{
					Name:  "testMetric3",
					Time:  t1,
					Value: 17.0,
					Metadata: MetricMetadata{
						"mdata1": "data2",
					},
					Unit: "Count",
				},
				&Metric{
					Name:  "testMetric3",
					Time:  t2,
					Value: 89.0,
					Metadata: MetricMetadata{
						"mdata1": "data1",
					},
					Unit: "Count",
				},
				&Metric{
					Name:  "testMetric3",
					Time:  ti,
					Value: 19.0,
					Metadata: MetricMetadata{
						"mdata1": "data1",
					},
					Unit: "Count",
				},
			},
			expected: metricBatch{
				&Metric{
					Name: "testMetric3",
					Time: t1,
					Metadata: MetricMetadata{
						"mdata1": "data2",
					},
					Unit: "Count",
					Statistics: struct {
						SampleCount float64
						Sum         float64
						Minimum     float64
						Maximum     float64
					}{
						SampleCount: 1,
						Sum:         17,
						Minimum:     17,
						Maximum:     17,
					},
				},
				&Metric{
					Name: "testMetric3",
					Time: t3,
					Metadata: MetricMetadata{
						"mdata1": "data1",
					},
					Unit: "Count",
					Statistics: struct {
						SampleCount float64
						Sum         float64
						Minimum     float64
						Maximum     float64
					}{
						SampleCount: 2,
						Sum:         108,
						Minimum:     19,
						Maximum:     89,
					},
				},
				&Metric{
					Name: "testMetric1",
					Time: ti,
					Metadata: MetricMetadata{
						"mdata1": "data2",
					},
					Unit: "Count",
					Statistics: struct {
						SampleCount float64
						Sum         float64
						Minimum     float64
						Maximum     float64
					}{
						SampleCount: 1,
						Sum:         14,
						Minimum:     14,
						Maximum:     14,
					},
				},
			},
		},
	}
	for _, test := range handlerTest {
		if test.method == bufferSize {
			maxMetricsInBuffer = 4
		}
		backend := &DummyBackend{make(chan *metricBatch)}
		metricsHandler := NewMetricsHandler(backend, 1*time.Second)
		tickChan := make(chan time.Time)
		metricsHandler.ticker.C = tickChan
		for _, metric := range *test.inputBatch {
			metricsHandler.Add(metric)
		}
		if test.method == timer {
			tickChan <- time.Now()
		}
		receivedBatch := <-backend.receivedBatch
		// Fail if we got a nil result but expected something
		if receivedBatch == nil {
			if test.expected != nil {
				t.Errorf("%s test failed got %+v and expected %+v", test.Name, nil, test.expected)
			}
			continue
		}
		// Loop on results and use ObjectsAreEqual to avoid ordering issues
		for _, m := range *receivedBatch {
			failed := true
			for _, mm := range test.expected {
				if assert.ObjectsAreEqual(m, mm) {
					failed = false
					break
				}
			}
			if failed {
				t.Errorf("%s test failed got %+v and expected %+v", test.Name, receivedBatch, test.expected)
			}
		}
		metricsHandler.Close()
	}
}
