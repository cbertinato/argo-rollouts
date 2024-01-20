package honeycomb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/argoproj/argo-rollouts/metric"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/defaults"
	metricutil "github.com/argoproj/argo-rollouts/utils/metric"
	timeutil "github.com/argoproj/argo-rollouts/utils/time"
)

const (
	// ProviderType indicates the provider is for Honeycomb
	ProviderType           = "Honeycomb"
	HoneycombURL           = "https://api.honeycomb.io"
	ResolvedHoneycombQuery = "ResolvedHoneycombQuery"
	HoneycombSecret        = "honeycomb"
	HoneycombAPIKey        = "api-key"
)

type Calculation struct {
	Op     string  `json:"op"`
	Column *string `json:"column"`
}

type Filter struct {
	Op     string      `json:"op"`
	Column *string     `json:"column"`
	Value  interface{} `json:"value"`
}

type Order struct {
	Column string `json:"column"`
	Op     string `json:"op"`
	Order  string `json:"order"`
}

type Having struct {
	CalculateOp string  `json:"calculate_op"`
	Column      *string `json:"column"`
	Op          string  `json:"op"`
	Value       float64 `json:"value"`
}

type Query struct {
	ID           string        `json:"id"`
	Breakdowns   []string      `json:"breakdowns"`
	Calculations []Calculation `json:"calculations"`
	Filters      []Filter      `json:"filters"`
	FilterCombo  string        `json:"filter_combination"`
	Granularity  int           `json:"granularity"`
	Orders       []Order       `json:"orders"`
	Limit        int           `json:"limit"`
	EndTime      int           `json:"end_time"`
	TimeRange    int           `json:"time_range"`
	Havings      []Having      `json:"havings"`
}

type SeriesDatum struct {
	Time string      `json:"time"`
	Data interface{} `json:"data"`
}

type ResultsDatum struct {
	Data map[string]interface{} `json:"data"`
}

type QueryResultData struct {
	Series  []SeriesDatum  `json:"series"`
	Results []ResultsDatum `json:"results"`
}

type QueryResult struct {
	Query    Query           `json:"query"`
	ID       string          `json:"id"`
	Complete bool            `json:"complete"`
	Data     QueryResultData `json:"data"`
	Links    struct {
		QueryURL      string `json:"query_url"`
		GraphImageURL string `json:"graph_image_url"`
	} `json:"links"`
}

// HoneycombAPI is the interface to query Honeycomb
type HoneycombAPI interface {
	CreateQuery(ctx context.Context, query string, dataset string) (*Query, error)
	GetQueryResult(ctx context.Context, queryID string, dataset string) (*QueryResult, error)
}

type HoneycombClient struct {
	apiKey string
	client *http.Client
}

func NewHoneycombAPI(logCtx log.Entry, kubeclientset kubernetes.Interface) (HoneycombAPI, error) {
	var apiKey string

	ns := defaults.Namespace()
	secret, err := kubeclientset.CoreV1().Secrets(ns).Get(context.Background(), HoneycombSecret, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	apiKey = string(secret.Data[HoneycombAPIKey])

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{
		Transport: tr,
	}

	return &HoneycombClient{
		apiKey: apiKey,
		client: client,
	}, nil
}

type errorResponse struct {
	Error string `json:"error"`
}

func (c *HoneycombClient) CreateQuery(ctx context.Context, query string, dataset string) (*Query, error) {
	if dataset == "" {
		dataset = "__all__"
	}

	requestBody := bytes.NewBuffer([]byte(query))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, HoneycombURL+"/1/queries/"+dataset, requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-Honeycomb-Team", c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode == http.StatusOK {
		var q Query
		if err := json.Unmarshal(bodyBytes, &q); err != nil {
			return nil, fmt.Errorf("failed to unmarshal response body: %w", err)
		}
		return &q, nil
	}

	var e errorResponse
	if err := json.Unmarshal(bodyBytes, &e); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response body: %w", err)
	}

	return nil, fmt.Errorf("failed to create query: %s", e.Error)
}

type createQueryResultRequest struct {
	QueryID       string `json:"query_id"`
	DisableSeries bool   `json:"disable_series"`
	Limit         int    `json:"limit"`
}

func (c *HoneycombClient) GetQueryResult(ctx context.Context, queryID string, dataset string) (*QueryResult, error) {
	if queryID == "" {
		return nil, errors.New("query ID cannot be empty")
	}

	if dataset == "" {
		dataset = "__all__"
	}

	// first, create the query result
	reqPayload := createQueryResultRequest{
		QueryID:       queryID,
		DisableSeries: false,
		Limit:         10000,
	}
	reqBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	bodyReader := bytes.NewReader(reqBytes)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, HoneycombURL+"/1/query_results/"+dataset, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Add("X-Honeycomb-Team", c.apiKey)
	req.Header.Add("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// now check for the query result
	location := resp.Header.Get("Location")
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, HoneycombURL+location, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Add("X-Honeycomb-Team", c.apiKey)
	req.Header.Add("Content-Type", "application/json")

	var qr QueryResult

	ticker := time.NewTicker(1 * time.Second)
	timer := time.NewTimer(10 * time.Second)
	defer ticker.Stop()
	defer timer.Stop()

	// query results cannot take longer than 10 seconds to run
loop:
	for {
		select {
		case <-timer.C:
			return nil, errors.New("timed out waiting for query result")

		case <-ticker.C:
			resp, err := c.client.Do(req)
			if err != nil {
				return nil, fmt.Errorf("failed to execute request: %w", err)
			}
			defer resp.Body.Close()

			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to read response body: %w", err)
			}

			if resp.StatusCode == http.StatusOK {
				if err := json.Unmarshal(bodyBytes, &qr); err != nil {
					return nil, fmt.Errorf("failed to unmarshal response body: %w", err)
				}

				if qr.Complete {
					break loop
				}
			}

			var e errorResponse
			if err := json.Unmarshal(bodyBytes, &e); err != nil {
				return nil, fmt.Errorf("failed to unmarshal response body: %w", err)
			}
		}
	}

	return &qr, nil
}

// Implements the Provider Interface
type HoneycombProvider struct {
	api     HoneycombAPI
	timeout time.Duration
	queryID string
}

func NewHoneycombProvider(api HoneycombAPI, metric v1alpha1.Metric) (*HoneycombProvider, error) {
	p := &HoneycombProvider{
		api: api,
	}

	if metric.Provider.Honeycomb == nil || metric.Provider.Honeycomb.Timeout == nil {
		p.timeout = 10 * time.Second
		return p, nil
	}

	timeout := metric.Provider.Honeycomb.Timeout

	if *timeout < 0 {
		return nil, errors.New("honeycomb timeout should not be negative")
	}

	p.timeout = time.Duration(*timeout * int64(time.Second))
	return p, nil
}

var _ metric.Provider = (*HoneycombProvider)(nil)

func (p *HoneycombProvider) Type() string {
	return ProviderType
}

// GetMetadata returns any additional metadata which needs to be stored & displayed as part of the metrics result.
func (p *HoneycombProvider) GetMetadata(metric v1alpha1.Metric) map[string]string {
	metricsMetadata := make(map[string]string)
	if metric.Provider.Honeycomb.Query != "" {
		metricsMetadata[ResolvedHoneycombQuery] = metric.Provider.Honeycomb.Query
	}
	return metricsMetadata
}

// Run queries Honeycomb for the metric data
func (p *HoneycombProvider) Run(run *v1alpha1.AnalysisRun, metric v1alpha1.Metric) v1alpha1.Measurement {
	startTime := timeutil.MetaNow()
	newMeasurement := v1alpha1.Measurement{
		StartedAt: &startTime,
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	if p.queryID == "" {
		query, err := p.api.CreateQuery(ctx, metric.Provider.Honeycomb.Query, metric.Provider.Honeycomb.Dataset)
		if err != nil {
			return metricutil.MarkMeasurementError(newMeasurement, err)
		}

		p.queryID = query.ID
	}

	queryResult, err := p.api.GetQueryResult(ctx, p.queryID, metric.Provider.Honeycomb.Dataset)
	if err != nil {
		return metricutil.MarkMeasurementError(newMeasurement, err)
	}

	valueStr, newStatus, err := p.processResponse(metric, queryResult)
	if err != nil {
		return metricutil.MarkMeasurementError(newMeasurement, err)
	}
	newMeasurement.Value = valueStr
	newMeasurement.Phase = newStatus

	finishedTime := timeutil.MetaNow()
	newMeasurement.FinishedAt = &finishedTime
	return newMeasurement
}

type envStruct struct {
	Result int `expr:"result"`
}

func (p *HoneycombProvider) processResponse(metric v1alpha1.Metric, result *QueryResult) (string, v1alpha1.AnalysisPhase, error) {
	if len(result.Data.Results) == 0 {
		return "", v1alpha1.AnalysisPhaseFailed, errors.New("no results returned")
	}

	if len(result.Query.Calculations) == 0 {
		// this shouldn't happen, but just in case
		return "", v1alpha1.AnalysisPhaseFailed, errors.New("no calculations specifed in query")
	}

	var op string
	if result.Query.Calculations[0].Column != nil {
		op = fmt.Sprintf("%s(%s)", result.Query.Calculations[0].Op, *result.Query.Calculations[0].Column)
	} else {
		op = result.Query.Calculations[0].Op
	}

	values := make([]int, len(result.Data.Results))
	valuesStr := make([]string, len(result.Data.Results))

	for i, result := range result.Data.Results {
		resultValue := result.Data[op]
		values[i] = resultValue.(int)
		valuesStr[i] = fmt.Sprintf("%d", resultValue)
	}

	var sb strings.Builder
	sb.WriteString("[")
	sb.WriteString(strings.Join(valuesStr, ", "))
	sb.WriteString("]")
	valueStr := sb.String()

	if metric.SuccessCondition == "" && metric.FailureCondition == "" {
		//Always return success unless there is an error
		return valueStr, v1alpha1.AnalysisPhaseSuccessful, nil
	}

	// evaluate results against success/failure criteria
	var successProgram, failProgram *vm.Program
	var err error
	if metric.SuccessCondition != "" {
		successProgram, err = expr.Compile(metric.SuccessCondition, expr.Env(envStruct{}))
		if err != nil {
			return valueStr, v1alpha1.AnalysisPhaseFailed, err
		}
	}

	if metric.FailureCondition != "" {
		failProgram, err = expr.Compile(metric.FailureCondition, expr.Env(envStruct{}))
		if err != nil {
			return valueStr, v1alpha1.AnalysisPhaseFailed, err
		}
	}

	// apply threshold to the first operator if there are multiple
	successCondition := false
	failCondition := false

	for _, resultValue := range values {
		env := envStruct{
			Result: resultValue,
		}

		if metric.SuccessCondition != "" {
			output, err := expr.Run(successProgram, env)
			if err != nil {
				return valueStr, v1alpha1.AnalysisPhaseError, err
			}

			switch val := output.(type) {
			case bool:
				successCondition = val
			default:
				return valueStr, v1alpha1.AnalysisPhaseError, fmt.Errorf("expected bool, but got %T", val)
			}
		}

		if metric.FailureCondition != "" {
			output, err := expr.Run(failProgram, env)
			if err != nil {
				return valueStr, v1alpha1.AnalysisPhaseError, err
			}

			switch val := output.(type) {
			case bool:
				failCondition = val
			default:
				return valueStr, v1alpha1.AnalysisPhaseError, fmt.Errorf("expected bool, but got %T", val)
			}
		}
	}

	switch {
	case metric.SuccessCondition != "" && metric.FailureCondition == "":
		// Without a failure condition, a measurement is considered a failure if the measurement's success condition is not true
		failCondition = !successCondition
	case metric.SuccessCondition == "" && metric.FailureCondition != "":
		// Without a success condition, a measurement is considered a successful if the measurement's failure condition is not true
		successCondition = !failCondition
	}

	if failCondition {
		return valueStr, v1alpha1.AnalysisPhaseFailed, nil
	}

	if !failCondition && !successCondition {
		return valueStr, v1alpha1.AnalysisPhaseInconclusive, nil
	}

	return valueStr, v1alpha1.AnalysisPhaseSuccessful, nil
}

func (p *HoneycombProvider) Resume(run *v1alpha1.AnalysisRun, metric v1alpha1.Metric, measurement v1alpha1.Measurement) v1alpha1.Measurement {
	return measurement
}

func (p *HoneycombProvider) Terminate(run *v1alpha1.AnalysisRun, metric v1alpha1.Metric, measurement v1alpha1.Measurement) v1alpha1.Measurement {
	return measurement
}

func (p *HoneycombProvider) GarbageCollect(run *v1alpha1.AnalysisRun, metric v1alpha1.Metric, i int) error {
	return nil
}
