package metricsaggregator

import (
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

type CloudwatchBackend struct {
	namespace string
	svc       *cloudwatch.CloudWatch
}

// NewCloudwatchBackend is cloudwatch backend constructor
func NewCloudwatchBackend(env string, svc *cloudwatch.CloudWatch) *CloudwatchBackend {
	if svc == nil {
		svc = cloudwatch.New(session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		})))
	}
	return &CloudwatchBackend{
		namespace: namespacePrefix + env,
		svc:       svc}
}

// Publish takes a metrics batch and sends it to cloudwatch
func (cb *CloudwatchBackend) Publish(metrics *metricBatch) error {
	cwMetrics := adaptToCloudwatch(*metrics)
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
