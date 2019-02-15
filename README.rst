Go-metrics-aggregator
#####################

go metrics aggregator is a very minimalist metrics framework. Metrics are added to a metrics handler then periodically aggregated and sent to a metric backend.

It was primarily developed to send metrics to cloudwatch, hence the aggregation to reduce costs.
All metrics are gauges (no timers, counters nor histograms) because Cloudwatch doesn't make this distinction either.

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
        metricsHandler := metrics.NewMetricsHandler(cwBackend, 1*time.Second)

        metricsHandler.Add(&metrics.Metric{
            Name:     "MyMetric",
            Time:     time.Now(),
            Value:    1.0,
            Metadata: metrics.MetricMetadata{"Key": "Val"},
            Unit:     "Count",
        })
        metricsHandler.Close()
    }
