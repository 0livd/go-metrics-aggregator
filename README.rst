Go-metrics-aggregator
#####################

go metrics aggregator is a very minimalist metrics framework. Metrics are collected via an input channel then periodically aggregated and sent to a metric backend.

It was primarily developed to send metrics to cloudwatch, hence the aggregation to reduce costs.
All metrics are gauges (no timers, counters nor histograms) because Cloudwtach doesn't make this distinction either.

Only mean aggregation is supported for the moment, percentile aggregation is to be added.

Usage
=====

