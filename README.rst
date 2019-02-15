Go-metrics-aggregator
#####################

go metrics aggregator is a very minimalist metrics framework. Metrics are collected via an input channel then periodically aggregated and sent to a metric backend.

It was primarily developed to send metrics to cloudwatch, hence the aggregation to reduce costs.
All metrics are gauges (no timers, counters nor histograms) because Cloudwtach doesn't make this distinction either.

Only mean aggregation is supported for the moment, percentile aggregation is to be added.

Usage
=====

.. code-block:: go

    import (
        "time"

        metrics "github.com/0livd/go-metrics-aggregator"
    )

    func main() {
        cwBackend := metrics.NewCloudwatchBackend("test", nil)
        metricsChan := make(chan *metrics.Metric)
        metrics.NewMetricsHandler(cwBackend, nil, metricsChan)

        metricsChan <- &metrics.Metric{
            Name:     "MyMetric",
            Time:     time.Now(),
            Value:    1.0,
            Metadata: metrics.MetricMetadata{"Key": "Val"},
            Unit:     "Count",
        }
    }
