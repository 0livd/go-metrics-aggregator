package metricsaggregator

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/stretchr/testify/assert"
)

func str2Ptr(s string) *string     { return &s }
func float2Ptr(f float64) *float64 { return &f }
func int2Ptr(i int64) *int64       { return &i }

func TestAdaptToCloudwatch(t *testing.T) {
	ti, _ := time.Parse(time.RFC3339, "2014-11-12T11:45:26.0Z")
	handlerTest := []struct {
		Name string
		// Inputs
		currentTime time.Time
		inputBatch  *metricBatch
		// Expected results
		expected []*cloudwatch.MetricDatum
	}{
		{
			Name: "Single metric",
			inputBatch: &metricBatch{
				&Metric{
					Name:  "testMetric1",
					Time:  ti,
					Value: 1.0,
					Metadata: MetricMetadata{
						"mdata1": "data1",
					},
					Unit: "Count",
				},
			},
			expected: []*cloudwatch.MetricDatum{
				{
					Dimensions: []*cloudwatch.Dimension{{
						Name:  str2Ptr("mdata1"),
						Value: str2Ptr("data1"),
					}},
					MetricName: str2Ptr("testMetric1"),
					StatisticValues: &cloudwatch.StatisticSet{
						Maximum:     float2Ptr(0),
						Minimum:     float2Ptr(0),
						SampleCount: float2Ptr(0),
						Sum:         float2Ptr(0),
					},
					StorageResolution: int2Ptr(60),
					Timestamp:         &ti,
					Unit:              str2Ptr("Count"),
					Value:             float2Ptr(1),
				},
			},
		},
	}
	for _, test := range handlerTest {
		toCW := adaptToCloudwatch(*test.inputBatch)
		// Fail if we got a nil result but expected something
		if toCW == nil {
			if test.expected != nil {
				t.Errorf("%s test failed got %+v and expected %+v", test.Name, nil, test.expected)
			}
			continue
		}
		// Loop on results and use ObjectsAreEqual to avoid ordering issues
		for _, m := range toCW {
			failed := true
			for _, mm := range test.expected {
				if assert.ObjectsAreEqual(m, mm) {
					failed = false
					break
				}
			}
			if failed {
				t.Errorf("%s test failed got %+v and expected %+v", test.Name, toCW, test.expected)
			}
		}
	}
}
