// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package victoriaMetricsInstance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	oleltrace "go.opentelemetry.io/otel/trace"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/curl"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/influxdb/decoder"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/trace"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb"
)

const (
	MetricLableName = "__name__"
)

// Instance vm 查询实例
type Instance struct {
	ctx context.Context

	address   string
	apiPrefix string
	username  string
	password  string

	timeout time.Duration
	curl    curl.Curl
}

// NewInstance 初始化查询引擎
func NewInstance(ctx context.Context, address string, timeout time.Duration, curl curl.Curl) *Instance {
	return &Instance{
		ctx:     ctx,
		address: address,
		timeout: timeout,
		curl:    curl,
	}
}

func NewInstanceWithAPIPrefix(ctx context.Context, address, apiPrefix string, timeout time.Duration, curl curl.Curl) *Instance {
	return NewInstanceWithAPIPrefixAndBasicAuth(ctx, address, apiPrefix, "", "", timeout, curl)
}

func NewInstanceWithAPIPrefixAndBasicAuth(
	ctx context.Context, address, apiPrefix, username, password string, timeout time.Duration, curl curl.Curl,
) *Instance {
	return &Instance{
		ctx:       ctx,
		address:   address,
		apiPrefix: apiPrefix,
		username:  username,
		password:  password,
		timeout:   timeout,
		curl:      curl,
	}
}

var _ tsdb.Instance = (*Instance)(nil)

func (i *Instance) urlPath(name, params string) string {
	urlPath := strings.TrimRight(i.address, "/")
	if i.apiPrefix != "" {
		urlPath = fmt.Sprintf("%s/%s", urlPath, strings.Trim(i.apiPrefix, "/"))
	}
	urlPath = fmt.Sprintf("%s/%s", urlPath, strings.TrimLeft(name, "/"))
	if params != "" {
		urlPath = fmt.Sprintf("%s?%s", urlPath, params)
	}
	return urlPath
}

// GetInstanceType 获取实例类型
func (i *Instance) GetInstanceType() string {
	return consul.VictoriaMetricsStorageType
}

// QueryRaw 查询原始数据
func (i *Instance) QueryRaw(
	ctx context.Context,
	query *metadata.Query,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.SeriesSet {
	if hints != nil && hints.Func == "series" {
		return i.Series(ctx, query, time.UnixMilli(hints.Start), time.UnixMilli(hints.End), matchers...)
	}

	// 数据量过大，暂时不支持此查询
	return storage.EmptySeriesSet()
}

func (i *Instance) matrixFormat(data *Data, span oleltrace.Span) promql.Matrix {
	seriesNum := 0
	pointNum := 0

	matrix := make(promql.Matrix, len(data.Data.Result))
	for index, series := range data.Data.Result {
		metricIndex := 0
		metric := make(labels.Labels, len(series.Metric))
		for name, value := range series.Metric {
			metric[metricIndex] = labels.Label{
				Name:  name,
				Value: value,
			}
			metricIndex++
		}

		var values [][]interface{}
		if data.Data.ResultType == "vector" {
			values = append(values, series.Value)
		} else {
			values = series.Values
		}

		points := make([]promql.Point, len(values))
		for idx := 0; idx < len(values); idx++ {
			if len(values[idx]) != 2 {
				continue
			}
			var (
				nt  int64
				nv  float64
				err error
			)

			// 时间从 float64 转换为 int64
			switch pt := values[idx][0].(type) {
			case float64:
				// 从秒转换为毫秒
				nt = int64(pt) * 1e3
			default:
				continue
			}

			// 值从 string 转换为 float64
			switch pv := values[idx][1].(type) {
			case string:
				nv, err = strconv.ParseFloat(pv, 64)
				if err != nil {
					continue
				}
			default:
				continue
			}
			points[idx] = promql.Point{
				T: nt,
				V: nv,
			}
		}
		matrix[index] = promql.Series{
			Metric: metric,
			Points: points,
		}

		seriesNum++
		pointNum += len(points)
	}

	trace.InsertIntIntoSpan("resp-series-num", seriesNum, span)
	trace.InsertIntIntoSpan("resp-point-num", pointNum, span)

	return matrix
}

func (i *Instance) vectorFormat(data *Data, span oleltrace.Span) promql.Vector {
	vector := make(promql.Vector, 0, len(data.Data.Result))
	for _, series := range data.Data.Result {
		if len(series.Value) != 2 {
			continue
		}

		var (
			nt  int64
			nv  float64
			err error
		)
		switch pt := series.Value[0].(type) {
		case float64:
			nt = int64(pt) * 1e3
		default:
			continue
		}
		switch pv := series.Value[1].(type) {
		case string:
			nv, err = strconv.ParseFloat(pv, 64)
			if err != nil {
				continue
			}
		default:
			continue
		}

		metricIndex := 0
		metric := make(labels.Labels, len(series.Metric))
		for name, value := range series.Metric {
			metric[metricIndex] = labels.Label{
				Name:  name,
				Value: value,
			}
			metricIndex++
		}
		vector = append(vector, promql.Sample{
			Metric: metric,
			Point: promql.Point{
				T: nt,
				V: nv,
			},
		})
	}

	trace.InsertIntIntoSpan("resp-series-num", len(vector), span)
	trace.InsertIntIntoSpan("resp-point-num", len(vector), span)
	return vector
}

func (i *Instance) decodeData(resp *http.Response) (*Data, error) {
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("victoria metrics http status: %d", resp.StatusCode)
	}

	data := &Data{}
	err := json.NewDecoder(resp.Body).Decode(data)
	if err != nil {
		return nil, err
	}
	if data.Status != "success" {
		return nil, errors.New(data.Error)
	}
	return data, nil
}

func (i *Instance) decodeLabelsData(resp *http.Response) ([]string, error) {
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("victoria metrics http status: %d", resp.StatusCode)
	}

	data := &LabelsData{}
	if err := json.NewDecoder(resp.Body).Decode(data); err != nil {
		return nil, err
	}
	if data.Status != "success" {
		return nil, newVictoriaMetricsAPIError(data.Status, data.ErrorType, data.Error)
	}
	return data.Data, nil
}

func (i *Instance) decodeSeriesData(resp *http.Response) ([]Metric, error) {
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("victoria metrics http status: %d", resp.StatusCode)
	}

	data := &SeriesData{}
	if err := json.NewDecoder(resp.Body).Decode(data); err != nil {
		return nil, err
	}
	if data.Status != "success" {
		return nil, newVictoriaMetricsAPIError(data.Status, data.ErrorType, data.Error)
	}
	return data.Data, nil
}

func newVictoriaMetricsAPIError(status, errorType, message string) error {
	if message != "" {
		return errors.New(message)
	}
	if errorType != "" {
		return fmt.Errorf("victoria metrics status %s: %s", status, errorType)
	}
	return fmt.Errorf("victoria metrics status: %s", status)
}

func (i *Instance) metadataAPIURL(apiName string, query *metadata.Query, start, end time.Time) (string, string, error) {
	selector, err := metadata.NativeVMInfluxSelector(query)
	if err != nil {
		return "", "", err
	}

	values := &url.Values{}
	values.Set("match[]", selector)
	if !start.IsZero() {
		values.Set("start", strconv.FormatInt(start.Unix(), 10))
	}
	if !end.IsZero() {
		values.Set("end", strconv.FormatInt(end.Unix(), 10))
	}

	return i.urlPath(apiName, values.Encode()), selector, nil
}

func (i *Instance) queryLabelNames(
	ctx context.Context, query *metadata.Query, start, end time.Time, span oleltrace.Span,
) ([]string, error) {
	var cancel context.CancelFunc

	urlPath, selector, err := i.metadataAPIURL("labels", query, start, end)
	if err != nil {
		return nil, err
	}

	ctx, cancel = context.WithTimeout(ctx, i.timeout)
	defer cancel()
	startAnalyze := time.Now()

	trace.InsertStringIntoSpan("query-url-path", urlPath, span)
	trace.InsertStringIntoSpan("query-match", selector, span)
	log.Infof(ctx, "victoria metrics labels: %s, match: %s", urlPath, selector)

	resp, err := i.curl.Request(
		ctx, curl.Get,
		curl.Options{
			UrlPath:  urlPath,
			UserName: i.username,
			Password: i.password,
		},
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	trace.InsertStringIntoSpan("query-cost", time.Since(startAnalyze).String(), span)

	names, err := i.decodeLabelsData(resp)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	trace.InsertIntIntoSpan("resp-label-num", len(names), span)
	return names, nil
}

func (i *Instance) querySeriesMetadata(
	ctx context.Context, query *metadata.Query, start, end time.Time, span oleltrace.Span,
) ([]Metric, error) {
	var cancel context.CancelFunc

	urlPath, selector, err := i.metadataAPIURL("series", query, start, end)
	if err != nil {
		return nil, err
	}

	ctx, cancel = context.WithTimeout(ctx, i.timeout)
	defer cancel()
	startAnalyze := time.Now()

	trace.InsertStringIntoSpan("query-url-path", urlPath, span)
	trace.InsertStringIntoSpan("query-match", selector, span)
	log.Infof(ctx, "victoria metrics series: %s, match: %s", urlPath, selector)

	resp, err := i.curl.Request(
		ctx, curl.Get,
		curl.Options{
			UrlPath:  urlPath,
			UserName: i.username,
			Password: i.password,
		},
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	trace.InsertStringIntoSpan("query-cost", time.Since(startAnalyze).String(), span)

	series, err := i.decodeSeriesData(resp)
	if err != nil {
		return nil, err
	}
	trace.InsertIntIntoSpan("resp-series-num", len(series), span)
	return series, nil
}

// QueryRange 查询范围数据
func (i *Instance) QueryRange(
	ctx context.Context, promqlStr string,
	start, end time.Time, step time.Duration,
) (promql.Matrix, error) {
	var (
		cancel        context.CancelFunc
		span          oleltrace.Span
		startAnaylize time.Time

		err error
	)

	ctx, span = trace.IntoContext(ctx, trace.TracerName, "victoria-metrics-query-range")
	if span != nil {
		defer span.End()
	}
	values := &url.Values{}
	values.Set("query", promqlStr)
	values.Set("step", fmt.Sprintf("%.f", step.Seconds()))
	values.Set("start", fmt.Sprintf("%d", start.Unix()))
	values.Set("end", fmt.Sprintf("%d", end.Unix()))
	urlPath := i.urlPath("query_range", values.Encode())

	ctx, cancel = context.WithTimeout(ctx, i.timeout)
	defer cancel()
	startAnaylize = time.Now()

	trace.InsertStringIntoSpan("query-url-path", urlPath, span)
	trace.InsertStringIntoSpan("query-promql", promqlStr, span)
	log.Infof(ctx,
		"victoria metrics query: %s, promql: %s",
		urlPath, promqlStr,
	)

	resp, err := i.curl.Request(
		ctx, curl.Get,
		curl.Options{
			UrlPath:  urlPath,
			UserName: i.username,
			Password: i.password,
		},
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	trace.InsertStringIntoSpan("query-cost", time.Since(startAnaylize).String(), span)

	data, err := i.decodeData(resp)
	if err != nil {
		return nil, err
	}

	return i.matrixFormat(data, span), err
}

// Query instant 查询
func (i *Instance) Query(
	ctx context.Context, promqlStr string,
	end time.Time,
) (promql.Vector, error) {
	var (
		cancel        context.CancelFunc
		span          oleltrace.Span
		startAnaylize time.Time

		err error
	)

	ctx, span = trace.IntoContext(ctx, trace.TracerName, "victoria-metrics-query")
	if span != nil {
		defer span.End()
	}
	values := &url.Values{}
	values.Set("query", promqlStr)
	values.Set("time", fmt.Sprintf("%d", end.Unix()))
	urlPath := i.urlPath("query", values.Encode())

	ctx, cancel = context.WithTimeout(ctx, i.timeout)
	defer cancel()
	startAnaylize = time.Now()

	trace.InsertStringIntoSpan("query-url-path", urlPath, span)
	trace.InsertStringIntoSpan("query-promql", promqlStr, span)
	log.Infof(ctx,
		"victoria metrics query: %s, promql: %s",
		urlPath, promqlStr,
	)

	resp, err := i.curl.Request(
		ctx, curl.Get,
		curl.Options{
			UrlPath:  urlPath,
			UserName: i.username,
			Password: i.password,
		},
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	trace.InsertStringIntoSpan("query-cost", time.Since(startAnaylize).String(), span)

	data, err := i.decodeData(resp)
	if err != nil {
		return nil, err
	}

	return i.vectorFormat(data, span), err
}

func (i *Instance) QueryExemplar(ctx context.Context, fields []string, query *metadata.Query, start, end time.Time, matchers ...*labels.Matcher) (*decoder.Response, error) {
	panic("implement me")
}

func (i *Instance) LabelNames(ctx context.Context, query *metadata.Query, start, end time.Time, matchers ...*labels.Matcher) ([]string, error) {
	ctx, span := trace.IntoContext(ctx, trace.TracerName, "victoria-metrics-label-names")
	if span != nil {
		defer span.End()
	}

	trace.InsertStringIntoSpan("query-matchers", fmt.Sprintf("%+v", matchers), span)
	trace.InsertStringIntoSpan("query-start", start.String(), span)
	trace.InsertStringIntoSpan("query-end", end.String(), span)

	return i.queryLabelNames(ctx, query, start, end, span)
}

func (i *Instance) LabelValues(ctx context.Context, query *metadata.Query, name string, start, end time.Time, matchers ...*labels.Matcher) ([]string, error) {
	ctx, span := trace.IntoContext(ctx, trace.TracerName, "victoria-metrics-label-values")
	if span != nil {
		defer span.End()
	}

	trace.InsertStringIntoSpan("query-name", name, span)
	trace.InsertStringIntoSpan("query-matchers", fmt.Sprintf("%+v", matchers), span)
	trace.InsertStringIntoSpan("query-start", start.String(), span)
	trace.InsertStringIntoSpan("query-end", end.String(), span)

	series, err := i.querySeriesMetadata(ctx, query, start, end, span)
	if err != nil {
		return nil, err
	}

	labelMap := make(map[string]struct{}, len(series))
	for _, metric := range series {
		if value, ok := metric[name]; ok && value != "" {
			labelMap[value] = struct{}{}
		}
	}

	values := make([]string, 0, len(labelMap))
	for value := range labelMap {
		values = append(values, value)
	}
	sort.Strings(values)
	trace.InsertIntIntoSpan("resp-label-value-num", len(values), span)
	return values, nil
}

func (i *Instance) Series(ctx context.Context, query *metadata.Query, start, end time.Time, matchers ...*labels.Matcher) storage.SeriesSet {
	ctx, span := trace.IntoContext(ctx, trace.TracerName, "victoria-metrics-series")
	if span != nil {
		defer span.End()
	}

	trace.InsertStringIntoSpan("query-matchers", fmt.Sprintf("%+v", matchers), span)
	trace.InsertStringIntoSpan("query-start", start.String(), span)
	trace.InsertStringIntoSpan("query-end", end.String(), span)

	metrics, err := i.querySeriesMetadata(ctx, query, start, end, span)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	if len(metrics) == 0 {
		return storage.EmptySeriesSet()
	}

	series := make([]storage.Series, 0, len(metrics))
	for _, metric := range metrics {
		series = append(series, storage.MockSeries(nil, nil, metricLabelSet(metric)))
	}
	return newNativeVMSeriesSet(series)
}

type nativeVMSeriesSet struct {
	series []storage.Series
	idx    int
}

func newNativeVMSeriesSet(series []storage.Series) storage.SeriesSet {
	return &nativeVMSeriesSet{
		series: series,
		idx:    -1,
	}
}

func (s *nativeVMSeriesSet) Next() bool {
	s.idx++
	return s.idx < len(s.series)
}

func (s *nativeVMSeriesSet) At() storage.Series {
	if s.idx < 0 || s.idx >= len(s.series) {
		return nil
	}
	return s.series[s.idx]
}

func (s *nativeVMSeriesSet) Err() error {
	return nil
}

func (s *nativeVMSeriesSet) Warnings() storage.Warnings {
	return nil
}

func metricLabelSet(metric Metric) []string {
	keys := make([]string, 0, len(metric))
	for key := range metric {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	labelSet := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		labelSet = append(labelSet, key, metric[key])
	}
	return labelSet
}
