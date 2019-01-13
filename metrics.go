package metricsaggregator

import (
	"log"
	"math"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
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
	IncomingMetrics <-chan *Metric
	metrics         metricBatch
	tickChan        <-chan time.Time
	metricBackend   metricBackend
}

// NewMetricsHandler creates a metrichandler and launch its run method in a goroutine
func NewMetricsHandler(metricBackend metricBackend, tickChan <-chan time.Time, incomingMetrics <-chan *Metric) *MetricsHandler {
	if tickChan == nil {
		tickChan = make(chan time.Time)
	}
	if incomingMetrics == nil {
		incomingMetrics = make(chan *Metric)
	}
	metricsHandler := &MetricsHandler{
		IncomingMetrics: incomingMetrics,
		tickChan:        tickChan,
		metricBackend:   metricBackend,
	}
	go metricsHandler.run()
	return metricsHandler
}

func (mh *MetricsHandler) run() {
	for {
		select {
		case metric := <-mh.IncomingMetrics:
			mh.metrics = append(mh.metrics, metric)
			if len(mh.metrics) >= maxMetricsInBuffer {
				mh.sendMetrics()
			}
		case <-mh.tickChan:
			if len(mh.metrics) != 0 {
				mh.sendMetrics()
			}
		}
	}
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

type CloudwatchBackend struct {
	namespace string
	svc       *cloudwatch.CloudWatch
}

func NewCloudwatchBackend(env string) *CloudwatchBackend {
	return &CloudwatchBackend{
		namespace: namespacePrefix + env,
		svc: cloudwatch.New(session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})))}
}

func (cb *CloudwatchBackend) Publish(metrics metricBatch) error {
	cwMetrics := adaptToCloudwatch(metrics)
	return cb.publishToCloudwatch(&cb.namespace, cwMetrics)
}

func (cb *CloudwatchBackend) publishToCloudwatch(namespace *string, metricData []*cloudwatch.MetricDatum) error {
	req, _ := cb.svc.PutMetricDataRequest(&cloudwatch.PutMetricDataInput{
		MetricData: metricData,
		Namespace:  namespace,
	})
	var metricNamesSet = map[string]bool{}
	for _, metric := range metricData {
		metricNamesSet[*metric.MetricName] = true
	}
	var metricNames []string
	for metricName := range metricNamesSet {
		metricNames = append(metricNames, metricName)
	}
	log.Printf("Successfully pushed the following %d metrics to cloudwatch: %s", len(metricData), strings.Join(metricNames, ","))
	// _ = req
	// return nil
	return req.Send()
}

func adaptToCloudwatch(metrics metricBatch) []*cloudwatch.MetricDatum {
	var cwMetrics []*cloudwatch.MetricDatum
	for _, metric := range metrics {
		var cwMetric cloudwatch.MetricDatum
		var cwDimensions = []*cloudwatch.Dimension{}
		for k, v := range metric.Metadata {
			// Make a copy of loop variables to avoid always referencing to the same location when taking their address
			kk := k
			vv := v
			cwDimensions = append(cwDimensions, &cloudwatch.Dimension{
				Name:  &kk,
				Value: &vv,
			})
		}
		if metric.Value != 0 {
			cwMetric.SetValue(metric.Value)
		}
		statSet := &cloudwatch.StatisticSet{}
		statSet.SetMaximum(metric.Statistics.Maximum).
			SetMinimum(metric.Statistics.Minimum).
			SetSampleCount(metric.Statistics.SampleCount).
			SetSum(metric.Statistics.Sum)
		cwMetric.SetMetricName(metric.Name).
			SetUnit(metric.Unit).
			SetTimestamp(metric.Time).
			SetDimensions(cwDimensions).
			SetStorageResolution(60).
			SetStatisticValues(statSet)
		cwMetrics = append(cwMetrics, &cwMetric)
	}
	return cwMetrics
}
