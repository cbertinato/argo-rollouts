package honeycomb

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newAnalysisRun() *v1alpha1.AnalysisRun {
	return &v1alpha1.AnalysisRun{}
}

func TestType(t *testing.T) {
	mock := &mockAPI{}
	p, _ := NewHoneycombProvider(mock, v1alpha1.Metric{})
	assert.Equal(t, ProviderType, p.Type())
}

func stringPtr(s string) *string {
	return &s
}

func mockQueryResult() (*Query, *QueryResult) {
	query := Query{
		ID: "string",
		Breakdowns: []string{
			"user_agent",
		},
		Calculations: []Calculation{
			{
				Op:     "P99",
				Column: stringPtr("duration_ms"),
			},
		},
	}

	queryResult := QueryResult{
		Query:    query,
		ID:       "sGUnkBHgRFN",
		Complete: true,
		Data: QueryResultData{
			Series: []SeriesDatum{
				{
					Time: "2021-04-09T14:16:00Z",
					Data: map[string]interface{}{
						"P99(duration_ms)": 210,
						"name":             "TestGoogleCallbackLogin",
						"test.classname":   "github.com/honeycombio/hound/cmd/poodle/handlers",
						"test.status":      "passed",
					},
				},
				{
					Time: "2021-04-09T14:17:00Z",
					Data: map[string]interface{}{
						"P99(duration_ms)": 250,
						"name":             "TestGoogleCallbackLogin",
						"test.classname":   "github.com/honeycombio/hound/cmd/poodle/handlers",
						"test.status":      "passed",
					},
				},
			},
			Results: []ResultsDatum{
				{
					Data: map[string]interface{}{
						"P99(duration_ms)": 210,
						"name":             "TestGoogleCallbackLogin",
						"test.classname":   "github.com/honeycombio/hound/cmd/poodle/handlers",
						"test.status":      "passed",
					},
				},
				{
					Data: map[string]interface{}{
						"P99(duration_ms)": 250,
						"name":             "TestGoogleCallbackLogin",
						"test.classname":   "github.com/honeycombio/hound/cmd/poodle/handlers",
						"test.status":      "passed",
					},
				},
			},
		},
		Links: struct {
			QueryURL      string `json:"query_url"`
			GraphImageURL string `json:"graph_image_url"`
		}{
			QueryURL:      "https://ui.honeycomb.io/myteam/datasets/test-via-curl/result/HprJhV1fYy",
			GraphImageURL: "https://ui.honeycomb.io/myteam/datasets/test-via-curl/result/HprJhV1fYy/snapshot",
		},
	}

	return &query, &queryResult
}

func mockEmptyQueryResult() (*Query, *QueryResult) {
	query := Query{
		ID: "string",
		Breakdowns: []string{
			"user_agent",
		},
		Calculations: []Calculation{
			{
				Op:     "P99",
				Column: stringPtr("duration_ms"),
			},
		},
	}

	queryResult := QueryResult{
		Query:    query,
		ID:       "sGUnkBHgRFN",
		Complete: true,
		Data: QueryResultData{
			Series:  []SeriesDatum{},
			Results: []ResultsDatum{},
		},
		Links: struct {
			QueryURL      string `json:"query_url"`
			GraphImageURL string `json:"graph_image_url"`
		}{
			QueryURL:      "https://ui.honeycomb.io/myteam/datasets/test-via-curl/result/HprJhV1fYy",
			GraphImageURL: "https://ui.honeycomb.io/myteam/datasets/test-via-curl/result/HprJhV1fYy/snapshot",
		},
	}

	return &query, &queryResult
}

func TestRunSuccessfully(t *testing.T) {
	query, queryResult := mockQueryResult()
	mock := &mockAPI{
		response: queryResult,
	}

	b, err := json.Marshal(query)
	assert.NoError(t, err)

	metric := v1alpha1.Metric{
		Name:             "foo",
		SuccessCondition: "result < 300",
		FailureCondition: "result > 310",
		Provider: v1alpha1.MetricProvider{
			Honeycomb: &v1alpha1.HoneycombMetric{
				Query: string(b),
			},
		},
	}
	p, _ := NewHoneycombProvider(mock, metric)

	measurement := p.Run(newAnalysisRun(), metric)

	assert.NotNil(t, measurement.StartedAt)
	assert.Equal(t, `[210, 250]`, measurement.Value)
	assert.NotNil(t, measurement.FinishedAt)
	assert.Equal(t, v1alpha1.AnalysisPhaseSuccessful, measurement.Phase)

}

func TestGetMetadata(t *testing.T) {
	metric := v1alpha1.Metric{
		Name:             "foo",
		SuccessCondition: "result < 300",
		FailureCondition: "result > 310",
		Provider: v1alpha1.MetricProvider{
			Honeycomb: &v1alpha1.HoneycombMetric{
				Query: "bar",
			},
		},
	}
	p, _ := NewHoneycombProvider(&mockAPI{}, metric)

	metadata := p.GetMetadata(metric)
	assert.Equal(t, "bar", metadata[ResolvedHoneycombQuery])
}

func TestRunWithQueryError(t *testing.T) {
	expectedErr := fmt.Errorf("not good")
	query, queryResult := mockQueryResult()
	mock := &mockAPI{
		response: queryResult,
		err:      expectedErr,
	}

	b, err := json.Marshal(query)
	assert.NoError(t, err)

	metric := v1alpha1.Metric{
		Name:             "foo",
		SuccessCondition: "result == 300",
		FailureCondition: "result != 300",
		Provider: v1alpha1.MetricProvider{
			Honeycomb: &v1alpha1.HoneycombMetric{
				Query: string(b),
			},
		},
	}
	p, _ := NewHoneycombProvider(mock, metric)

	measurement := p.Run(newAnalysisRun(), metric)
	assert.Equal(t, expectedErr.Error(), measurement.Message)
	assert.NotNil(t, measurement.StartedAt)
	assert.Equal(t, "", measurement.Value)
	assert.NotNil(t, measurement.FinishedAt)
	assert.Equal(t, v1alpha1.AnalysisPhaseError, measurement.Phase)
}

func TestRunWithResolveArgsError(t *testing.T) {
	expectedErr := fmt.Errorf("failed to resolve {{args.var}}")
	mock := &mockAPI{
		err: expectedErr,
	}

	metric := v1alpha1.Metric{
		Name:             "foo",
		SuccessCondition: "result == 300",
		FailureCondition: "result != 300",
		Provider: v1alpha1.MetricProvider{
			Honeycomb: &v1alpha1.HoneycombMetric{
				Query: "test",
			},
		},
	}
	p, _ := NewHoneycombProvider(mock, metric)

	measurement := p.Run(newAnalysisRun(), metric)
	assert.Equal(t, expectedErr.Error(), measurement.Message)
	assert.NotNil(t, measurement.StartedAt)
	assert.Equal(t, "", measurement.Value)
	assert.NotNil(t, measurement.FinishedAt)
	assert.Equal(t, v1alpha1.AnalysisPhaseError, measurement.Phase)
}

func TestRunWithEvaluationError(t *testing.T) {
	expectedErr := fmt.Errorf("no results returned")
	query, queryResult := mockEmptyQueryResult()

	mock := &mockAPI{
		response: queryResult,
		err:      expectedErr,
	}

	b, err := json.Marshal(query)
	assert.NoError(t, err)

	metric := v1alpha1.Metric{
		Name:             "foo",
		SuccessCondition: "result == 300",
		FailureCondition: "result != 300",
		Provider: v1alpha1.MetricProvider{
			Honeycomb: &v1alpha1.HoneycombMetric{
				Query: string(b),
			},
		},
	}
	p, _ := NewHoneycombProvider(mock, metric)

	measurement := p.Run(newAnalysisRun(), metric)
	assert.Equal(t, expectedErr.Error(), measurement.Message)
	assert.NotNil(t, measurement.StartedAt)
	assert.Equal(t, "", measurement.Value)
	assert.NotNil(t, measurement.FinishedAt)
	assert.Equal(t, v1alpha1.AnalysisPhaseError, measurement.Phase)
}

func TestResume(t *testing.T) {
	mock := &mockAPI{}
	metric := v1alpha1.Metric{
		Name:             "foo",
		SuccessCondition: "result == 300",
		FailureCondition: "result != 300",
		Provider: v1alpha1.MetricProvider{
			Honeycomb: &v1alpha1.HoneycombMetric{
				Query: "test",
			},
		},
	}
	p, _ := NewHoneycombProvider(mock, metric)

	now := metav1.Now()
	previousMeasurement := v1alpha1.Measurement{
		StartedAt: &now,
		Phase:     v1alpha1.AnalysisPhaseInconclusive,
	}
	measurement := p.Resume(newAnalysisRun(), metric, previousMeasurement)
	assert.Equal(t, previousMeasurement, measurement)
}

func TestTerminate(t *testing.T) {
	mock := &mockAPI{}
	metric := v1alpha1.Metric{}
	p, _ := NewHoneycombProvider(mock, metric)
	now := metav1.Now()
	previousMeasurement := v1alpha1.Measurement{
		StartedAt: &now,
		Phase:     v1alpha1.AnalysisPhaseRunning,
	}
	measurement := p.Terminate(newAnalysisRun(), metric, previousMeasurement)
	assert.Equal(t, previousMeasurement, measurement)
}

func TestGarbageCollect(t *testing.T) {
	mock := &mockAPI{}
	metric := v1alpha1.Metric{}
	p, _ := NewHoneycombProvider(mock, metric)
	err := p.GarbageCollect(nil, metric, 0)
	assert.NoError(t, err)
}
