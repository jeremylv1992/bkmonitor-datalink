// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package victoriaMetrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/curl"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/influxdb/decoder"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/trace"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb"
)

const (
	BkUserName    = "admin"
	PreferStorage = "vm"

	ContentType = "Content-Type"

	APISeries      = "series"
	APILabelNames  = "labels"
	APILabelValues = "label_values"
	APIQueryRange  = "query_range"
	APIQuery       = "query"

	OK = "00"

	VectorType = "vector"
	MatrixType = "matrix"
)

// Instance vm 查询实例
type Instance struct {
	Ctx context.Context

	MaxConditionNum int

	ContentType string

	Address string
	UriPath string

	Code   string
	Secret string
	Token  string

	AuthenticationMethod string

	InfluxCompatible bool
	UseNativeOr      bool

	Timeout time.Duration
	Curl    curl.Curl
}

var _ tsdb.Instance = (*Instance)(nil)

func (i *Instance) vectorFormat(ctx context.Context, resp *VmResponse, span *trace.Span) (promql.Vector, error) {
	if !resp.Result {
		return nil, fmt.Errorf(
			"%s, %s, %s", resp.Message, resp.Errors.Error, resp.Errors.QueryId,
		)
	}
	if resp.Code != OK {
		return nil, fmt.Errorf(
			"%s, %s, %s", resp.Message, resp.Errors.Error, resp.Errors.QueryId,
		)
	}

	prefix := "response-"
	span.Set(fmt.Sprintf("%s-list-num", prefix), len(resp.Data.List))
	span.Set(fmt.Sprintf("%s-cluster", prefix), resp.Data.Cluster)
	span.Set(fmt.Sprintf("%s-sql", prefix), resp.Data.SQL)
	span.Set(fmt.Sprintf("%s-device", prefix), resp.Data.Device)
	span.Set(fmt.Sprintf("%s-elapsed-time", prefix), resp.Data.BksqlCallElapsedTime)
	span.Set(fmt.Sprintf("%s-total-records", prefix), resp.Data.TotalRecords)
	span.Set(fmt.Sprintf("%s-result-table", prefix), resp.Data.ResultTableIds)

	if len(resp.Data.List) > 0 {
		data := resp.Data.List[0].Data
		seriesNum := 0

		vector := make(promql.Vector, 0, len(data.Result))
		for _, series := range data.Result {
			metricIndex := 0
			metric := make(labels.Labels, len(series.Metric))
			for name, value := range series.Metric {
				metric[metricIndex] = labels.Label{
					Name:  name,
					Value: value,
				}
				metricIndex++
			}

			var point promql.Point
			if data.ResultType != VectorType {
				continue
			}

			nt, nv, err := series.Value.Point()
			if err != nil {
				log.Errorf(ctx, err.Error())
				continue
			}
			point.T = nt
			point.V = nv
			vector = append(vector, promql.Sample{
				Metric: metric,
				Point:  point,
			})

			seriesNum++
		}

		span.Set("resp-series-num", seriesNum)
		return vector, nil
	}

	return nil, nil
}

func (i *Instance) matrixFormat(ctx context.Context, resp *VmResponse, span *trace.Span) (promql.Matrix, error) {
	if !resp.Result {
		return nil, fmt.Errorf(
			"%s, %s, %s", resp.Message, resp.Errors.Error, resp.Errors.QueryId,
		)
	}
	if resp.Code != OK {
		return nil, fmt.Errorf(
			"%s, %s, %s", resp.Message, resp.Errors.Error, resp.Errors.QueryId,
		)
	}

	prefix := "vm-data"
	span.Set(fmt.Sprintf("%s-list-num", prefix), len(resp.Data.List))
	span.Set(fmt.Sprintf("%s-cluster", prefix), resp.Data.Cluster)
	span.Set(fmt.Sprintf("%s-sql", prefix), resp.Data.SQL)
	span.Set(fmt.Sprintf("%s-device", prefix), resp.Data.Device)
	span.Set(fmt.Sprintf("%s-elapsed-time", prefix), resp.Data.BksqlCallElapsedTime)
	span.Set(fmt.Sprintf("%s-total-records", prefix), resp.Data.TotalRecords)
	span.Set(fmt.Sprintf("%s-result-table", prefix), resp.Data.ResultTableIds)

	if len(resp.Data.List) > 0 {
		data := resp.Data.List[0].Data
		seriesNum := 0
		pointNum := 0

		matrix := make(promql.Matrix, 0, len(data.Result))
		for _, series := range data.Result {
			metricIndex := 0
			metric := make(labels.Labels, len(series.Metric))
			for name, value := range series.Metric {
				metric[metricIndex] = labels.Label{
					Name:  name,
					Value: value,
				}
				metricIndex++
			}

			points := make([]promql.Point, 0)
			if data.ResultType == VectorType {
				nt, nv, err := series.Value.Point()
				if err != nil {
					log.Errorf(ctx, err.Error())
					continue
				}
				points = append(points, promql.Point{
					T: nt,
					V: nv,
				})
			} else {
				for _, value := range series.Values {
					nt, nv, err := value.Point()
					if err != nil {
						log.Errorf(ctx, err.Error())
						continue
					}
					points = append(points, promql.Point{
						T: nt,
						V: nv,
					})
				}
			}
			matrix = append(matrix, promql.Series{
				Metric: metric,
				Points: points,
			})

			seriesNum++
			pointNum += len(points)
		}

		span.Set("resp-series-num", seriesNum)
		span.Set("resp-point-num", pointNum)
		return matrix, nil
	}

	return nil, nil
}

func (i *Instance) labelFormat(ctx context.Context, resp *VmLableValuesResponse, span *trace.Span) ([]string, error) {
	if !resp.Result {
		return nil, fmt.Errorf(
			"%s, %s, %s", resp.Message, resp.Errors.Error, resp.Errors.QueryId,
		)
	}
	if resp.Code != OK {
		log.Errorf(ctx, resp.Errors.Error)
		return nil, fmt.Errorf(
			"%s, %s, %s", resp.Message, resp.Errors.Error, resp.Errors.QueryId,
		)
	}

	prefix := "vm-data"
	span.Set(fmt.Sprintf("%s-list-num", prefix), len(resp.Data.List))
	span.Set(fmt.Sprintf("%s-cluster", prefix), resp.Data.Cluster)
	span.Set(fmt.Sprintf("%s-sql", prefix), resp.Data.SQL)
	span.Set(fmt.Sprintf("%s-device", prefix), resp.Data.Device)
	span.Set(fmt.Sprintf("%s-elapsed-time", prefix), resp.Data.BksqlCallElapsedTime)
	span.Set(fmt.Sprintf("%s-total-records", prefix), resp.Data.TotalRecords)
	span.Set(fmt.Sprintf("%s-result-table", prefix), resp.Data.ResultTableIds)

	lbsMap := make(map[string]struct{}, 0)
	for _, d := range resp.Data.List {
		for _, v := range d.Data {
			lbsMap[v] = struct{}{}
		}
	}
	lbs := make([]string, 0, len(lbsMap))
	for k := range lbsMap {
		lbs = append(lbs, k)
	}

	return lbs, nil
}

func (i *Instance) seriesFormat(ctx context.Context, resp *VmSeriesResponse, span *trace.Span) ([]map[string]string, error) {
	if !resp.Result {
		return nil, fmt.Errorf("%s", resp.Message)
	}
	if resp.Code != OK {
		return nil, fmt.Errorf("%s", resp.Message)
	}

	prefix := "vm-data"
	span.Set(fmt.Sprintf("%s-list-num", prefix), len(resp.Data.List))
	span.Set(fmt.Sprintf("%s-cluster", prefix), resp.Data.Cluster)
	span.Set(fmt.Sprintf("%s-sql", prefix), resp.Data.SQL)
	span.Set(fmt.Sprintf("%s-device", prefix), resp.Data.Device)
	span.Set(fmt.Sprintf("%s-elapsed-time", prefix), resp.Data.BksqlCallElapsedTime)
	span.Set(fmt.Sprintf("%s-total-records", prefix), resp.Data.TotalRecords)
	span.Set(fmt.Sprintf("%s-result-table", prefix), resp.Data.ResultTableIds)

	series := make([]map[string]string, 0)
	for _, d := range resp.Data.List {
		series = append(series, d.Data...)
	}

	return series, nil
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
	return nil
}

// vmQuery
func (i *Instance) vmQuery(
	ctx context.Context, sql string, data interface{}, span *trace.Span,
) error {
	var (
		cancel        context.CancelFunc
		startAnaylize time.Time

		err error
	)

	address := fmt.Sprintf("%s/%s", i.Address, i.UriPath)
	user := metadata.GetUser(ctx)
	params := &Params{
		SQL:                        sql,
		BkdataAuthenticationMethod: i.AuthenticationMethod,
		BkUsername:                 BkUserName,
		BkAppCode:                  i.Code,
		PreferStorage:              PreferStorage,
		BkdataDataToken:            i.Token,
		BkAppSecret:                i.Secret,
	}
	body, err := json.Marshal(params)
	if err != nil {
		return err
	}

	ctx, cancel = context.WithTimeout(ctx, i.Timeout)
	defer cancel()
	startAnaylize = time.Now()

	span.Set("query-source", user.Source)
	span.Set("query-space-uid", user.SpaceUid)
	span.Set("query-username", user.Name)
	span.Set("query-address", i.Address)
	span.Set("query-uri-path", i.UriPath)

	log.Infof(ctx,
		"victoria metrics query: %s, body: %s, sql: %s",
		address, body, sql,
	)

	resp, err := i.Curl.Request(
		ctx, curl.Post,
		curl.Options{
			UrlPath: address,
			Body:    body,
			Headers: map[string]string{
				ContentType: i.ContentType,
			},
			Timeout: i.Timeout,
		},
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf(resp.Status)
	}

	queryCost := time.Since(startAnaylize)
	metric.TsDBRequestSecond(
		ctx, queryCost, user.SpaceUid, consul.VictoriaMetricsStorageType,
	)

	err = json.NewDecoder(resp.Body).Decode(data)
	if err != nil {
		return err
	}

	return nil
}

// QueryRange 查询范围数据
func (i *Instance) QueryRange(
	ctx context.Context, promqlStr string,
	start, end time.Time, step time.Duration,
) (promql.Matrix, error) {
	var (
		vmExpand *metadata.VmExpand

		vmResp = &VmResponse{}
		err    error
	)

	ctx, span := trace.NewSpan(ctx, "victoria-metrics-query-range")
	defer span.End(&err)

	vmExpand = metadata.GetExpand(ctx)

	span.Set("query-start", start.String())
	span.Set("query-end", end.String())
	span.Set("query-step", step.String())
	span.Set("query-promql", promqlStr)

	if vmExpand == nil || len(vmExpand.ResultTableList) == 0 {
		return promql.Matrix{}, nil
	}

	ves, _ := json.Marshal(vmExpand)
	log.Infof(ctx, "vm-expand: %s", ves)

	if i.MaxConditionNum > 0 && vmExpand.ConditionNum > i.MaxConditionNum {
		return nil, fmt.Errorf("condition length is too long %d > %d", vmExpand.ConditionNum, i.MaxConditionNum)
	}

	paramsQueryRange := &ParamsQueryRange{
		InfluxCompatible: i.InfluxCompatible,
		APIType:          APIQueryRange,
		APIParams: struct {
			Query string `json:"query"`
			Start int64  `json:"start"`
			End   int64  `json:"end"`
			Step  int64  `json:"step"`
		}{
			Query: promqlStr,
			Start: start.Unix(),
			End:   end.Unix(),
			Step:  int64(step.Seconds()),
		},
		UseNativeOr:           i.UseNativeOr,
		MetricFilterCondition: vmExpand.MetricFilterCondition,
		ResultTableList:       vmExpand.ResultTableList,
	}

	if vmExpand.ClusterName != "" {
		paramsQueryRange.ClusterName = vmExpand.ClusterName
	}

	sql, err := json.Marshal(paramsQueryRange)
	if err != nil {
		return nil, err
	}

	err = i.vmQuery(ctx, string(sql), vmResp, span)
	if err != nil {
		return nil, err
	}

	return i.matrixFormat(ctx, vmResp, span)
}

// Query instant 查询
func (i *Instance) Query(
	ctx context.Context, promqlStr string,
	end time.Time,
) (promql.Vector, error) {
	var (
		vmExpand *metadata.VmExpand

		vmResp = &VmResponse{}
		err    error
	)

	ctx, span := trace.NewSpan(ctx, "victoria-metrics-query")
	defer span.End(&err)

	vmExpand = metadata.GetExpand(ctx)

	span.Set("query-promql", promqlStr)
	span.Set("query-end", end.String())

	if vmExpand == nil || len(vmExpand.ResultTableList) == 0 {
		return promql.Vector{}, nil
	}

	ves, _ := json.Marshal(vmExpand)
	span.Set("vm-expand", string(ves))

	if i.MaxConditionNum > 0 && vmExpand.ConditionNum > i.MaxConditionNum {
		return nil, fmt.Errorf("condition length is too long %d > %d", vmExpand.ConditionNum, i.MaxConditionNum)
	}

	paramsQuery := &ParamsQuery{
		InfluxCompatible: i.InfluxCompatible,
		APIType:          APIQuery,
		APIParams: struct {
			Query   string `json:"query"`
			Time    int64  `json:"time"`
			Timeout int64  `json:"timeout"`
		}{
			Query:   promqlStr,
			Time:    end.Unix(),
			Timeout: int64(i.Timeout.Seconds()),
		},
		UseNativeOr:           i.UseNativeOr,
		MetricFilterCondition: vmExpand.MetricFilterCondition,
		ResultTableList:       vmExpand.ResultTableList,
	}

	if vmExpand.ClusterName != "" {
		paramsQuery.ClusterName = vmExpand.ClusterName
	}

	sql, err := json.Marshal(paramsQuery)
	if err != nil {
		return nil, err
	}

	err = i.vmQuery(ctx, string(sql), vmResp, span)
	if err != nil {
		return nil, err
	}

	return i.vectorFormat(ctx, vmResp, span)
}

func (i *Instance) metric(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, error) {
	var (
		vmExpand *metadata.VmExpand

		resp = &VmLableValuesResponse{}
		err  error
	)

	ctx, span := trace.NewSpan(ctx, "victoria-metrics-instance-metric")
	defer span.End(&err)

	vmExpand = metadata.GetExpand(ctx)

	span.Set("query-name", name)

	if vmExpand == nil {
		return nil, nil
	}

	if i.MaxConditionNum > 0 && vmExpand.ConditionNum > i.MaxConditionNum {
		return nil, fmt.Errorf("condition length is too long %d > %d", vmExpand.ConditionNum, i.MaxConditionNum)
	}

	ves, _ := json.Marshal(vmExpand)
	span.Set("vm-expand", string(ves))

	paramsQuery := &ParamsLabelValues{
		InfluxCompatible: i.InfluxCompatible,
		APIType:          APILabelValues,
		APIParams: struct {
			Label string `json:"label"`
		}{
			Label: name,
		},
		UseNativeOr:           i.UseNativeOr,
		MetricFilterCondition: vmExpand.MetricFilterCondition,
		ResultTableList:       vmExpand.ResultTableList,
	}

	if vmExpand.ClusterName != "" {
		paramsQuery.ClusterName = vmExpand.ClusterName
	}

	sql, err := json.Marshal(paramsQuery)
	if err != nil {
		return nil, err
	}

	err = i.vmQuery(ctx, string(sql), resp, span)
	if err != nil {
		return nil, err
	}

	return i.labelFormat(ctx, resp, span)
}

func (i *Instance) LabelNames(ctx context.Context, query *metadata.Query, start, end time.Time, matchers ...*labels.Matcher) ([]string, error) {
	var (
		vmExpand *metadata.VmExpand

		resp = &VmLableValuesResponse{}
		err  error
	)

	ctx, span := trace.NewSpan(ctx, "victoria-metrics-query")
	defer span.End(&err)

	vmExpand = metadata.GetExpand(ctx)

	if vmExpand == nil {
		return nil, nil
	}

	ves, _ := json.Marshal(vmExpand)
	span.Set("vm-expand", string(ves))

	if i.MaxConditionNum > 0 && vmExpand.ConditionNum > i.MaxConditionNum {
		return nil, fmt.Errorf("condition length is too long %d > %d", vmExpand.ConditionNum, i.MaxConditionNum)
	}

	span.Set("query-matchers", fmt.Sprintf("%+v", matchers))
	span.Set("query-start", start.String())
	span.Set("query-end", end.String())

	paramsQuery := &ParamsSeries{
		InfluxCompatible: i.InfluxCompatible,
		APIType:          APILabelNames,
		APIParams: struct {
			Match string `json:"match[]"`
			Start int64  `json:"start"`
			End   int64  `json:"end"`
		}{
			Start: start.Unix(),
			End:   end.Unix(),
		},
		UseNativeOr:           i.UseNativeOr,
		MetricFilterCondition: vmExpand.MetricFilterCondition,
		ResultTableList:       vmExpand.ResultTableList,
	}

	if vmExpand.ClusterName != "" {
		paramsQuery.ClusterName = vmExpand.ClusterName
	}

	vector := &parser.VectorSelector{
		LabelMatchers: matchers,
	}
	paramsQuery.APIParams.Match = vector.String()

	sql, err := json.Marshal(paramsQuery)
	if err != nil {
		return nil, err
	}

	err = i.vmQuery(ctx, string(sql), resp, span)
	if err != nil {
		return nil, err
	}

	return i.labelFormat(ctx, resp, span)
}

func (i *Instance) LabelValues(ctx context.Context, query *metadata.Query, name string, start, end time.Time, matchers ...*labels.Matcher) ([]string, error) {
	var (
		vmExpand *metadata.VmExpand

		resp = &VmResponse{}
		err  error
	)

	ctx, span := trace.NewSpan(ctx, "victoria-metrics-query")
	defer span.End(&err)

	if name == labels.MetricName {
		return i.metric(ctx, name, matchers...)
	}

	vmExpand = metadata.GetExpand(ctx)

	// 检查 vmExpand 以及 vmExpand.ResultTableGroup 不能为空
	if vmExpand == nil || len(vmExpand.ResultTableList) == 0 {
		return nil, nil
	}

	if i.MaxConditionNum > 0 && vmExpand.ConditionNum > i.MaxConditionNum {
		return nil, fmt.Errorf("condition length is too long %d > %d", vmExpand.ConditionNum, i.MaxConditionNum)
	}

	ves, _ := json.Marshal(vmExpand)
	span.Set("vm-expand", string(ves))
	span.Set("query-name", name)
	span.Set("query-matchers", fmt.Sprintf("%+v", matchers))
	span.Set("query-start", start.String())
	span.Set("query-end", end.String())

	referenceName := ""
	for _, m := range matchers {
		if m.Name == labels.MetricName {
			referenceName = m.Value
		}
	}

	if referenceName == "" {
		return nil, fmt.Errorf("reference name is empty: %v", matchers)
	}

	// 如果使用 end - start 作为 step，查询的时候会多查一个step的数据量，所以这里需要减少点数
	step := (end.Unix() - start.Unix()) / 10
	if step < 60 {
		step = 60
	}

	paramsQueryRange := &ParamsQueryRange{
		InfluxCompatible: i.InfluxCompatible,
		APIType:          APIQueryRange,
		APIParams: struct {
			Query string `json:"query"`
			Start int64  `json:"start"`
			End   int64  `json:"end"`
			Step  int64  `json:"step"`
		}{
			Start: start.Unix(),
			End:   end.Unix(),
			Step:  step,
		},
		UseNativeOr:           i.UseNativeOr,
		MetricFilterCondition: vmExpand.MetricFilterCondition,
		ResultTableList:       vmExpand.ResultTableList,
	}

	if vmExpand.ClusterName != "" {
		paramsQueryRange.ClusterName = vmExpand.ClusterName
	}

	vector := &parser.VectorSelector{
		LabelMatchers: matchers,
	}
	expr := &parser.AggregateExpr{
		Op:       parser.COUNT,
		Expr:     vector,
		Grouping: []string{name},
	}
	paramsQueryRange.APIParams.Query = expr.String()

	sql, err := json.Marshal(paramsQueryRange)
	if err != nil {
		return nil, err
	}

	err = i.vmQuery(ctx, string(sql), resp, span)
	if err != nil {
		return nil, err
	}

	series, err := i.matrixFormat(ctx, resp, span)
	if err != nil {
		return nil, err
	}

	lbsMap := make(map[string]struct{}, 0)
	for _, s := range series {
		for _, l := range s.Metric {
			if l.Name == name {
				lbsMap[l.Value] = struct{}{}
			}
		}
	}

	lbs := make([]string, 0, len(lbsMap))
	for k := range lbsMap {
		lbs = append(lbs, k)
	}

	return lbs, nil
}

func (i *Instance) QueryExemplar(ctx context.Context, fields []string, query *metadata.Query, start, end time.Time, matchers ...*labels.Matcher) (*decoder.Response, error) {
	panic("implement me")
}

func (i *Instance) Series(ctx context.Context, query *metadata.Query, start, end time.Time, matchers ...*labels.Matcher) storage.SeriesSet {
	return nil
}
