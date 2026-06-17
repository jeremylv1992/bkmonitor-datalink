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
	"errors"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/curl"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

const (
	testTime  = "2022-11-28 10:00:00"
	parseTime = "2006-01-02 15:04:05"
)

func TestInstanceQueryRangeUsesNativeVMSelectPrefix(t *testing.T) {
	log.InitTestLogger()
	ctx := context.Background()
	endTime, _ := time.ParseInLocation(parseTime, testTime, time.Local)
	startTime := endTime.Add(-5 * time.Minute)

	mockCurl := curl.NewMockCurl(map[string]string{
		`http://127.0.0.1/select/0/prometheus/api/v1/query_range?end=1669600800&query=%7B__name__%3D%22cpu_detail_usage%22%2Cdb%3D%22system%22%7D&start=1669600500&step=60`: `{"status":"success","isPartial":false,"data":{"resultType":"matrix","result":[{"metric":{"db":"system"},"values":[[1669600500,"1.23"],[1669600560,"2.34"]]}]}}`,
	}, log.OtLogger)

	ins := NewInstanceWithAPIPrefix(
		ctx, "http://127.0.0.1", "/select/0/prometheus/api/v1", time.Minute, mockCurl,
	)
	matrix, err := ins.QueryRange(
		ctx, `{__name__="cpu_detail_usage",db="system"}`, startTime, endTime, time.Minute,
	)
	require.NoError(t, err)
	require.Len(t, matrix, 1)
	assert.Equal(t, "system", matrix[0].Metric.Get("db"))
	require.Len(t, matrix[0].Points, 2)
	assert.Equal(t, 1.23, matrix[0].Points[0].V)
	assert.Equal(t, int64(1669600500000), matrix[0].Points[0].T)
}

func TestInstanceQueryRangeUsesBasicAuth(t *testing.T) {
	log.InitTestLogger()
	ctx := context.Background()
	endTime, _ := time.ParseInLocation(parseTime, testTime, time.Local)
	startTime := endTime.Add(-5 * time.Minute)

	mockCurl := curl.NewMockCurl(map[string]string{
		`http://127.0.0.1/select/0/prometheus/api/v1/query_range?end=1669600800&query=%7B__name__%3D%22cpu_detail_usage%22%2Cdb%3D%22system%22%7D&start=1669600500&step=60`: `{"status":"success","isPartial":false,"data":{"resultType":"matrix","result":[{"metric":{"db":"system"},"values":[[1669600500,"1.23"]]}]}}`,
	}, log.OtLogger)

	ins := NewInstanceWithAPIPrefixAndBasicAuth(
		ctx, "http://127.0.0.1", "/select/0/prometheus/api/v1", "query", "secret", time.Minute, mockCurl,
	)
	_, err := ins.QueryRange(
		ctx, `{__name__="cpu_detail_usage",db="system"}`, startTime, endTime, time.Minute,
	)
	require.NoError(t, err)
	assert.Equal(t, "query", mockCurl.UserName)
	assert.Equal(t, "secret", mockCurl.Password)
}

func TestInstanceQueryUsesNativeVMSelectPrefix(t *testing.T) {
	log.InitTestLogger()
	ctx := context.Background()
	endTime, _ := time.ParseInLocation(parseTime, testTime, time.Local)

	mockCurl := curl.NewMockCurl(map[string]string{
		`http://127.0.0.1/select/0/prometheus/api/v1/query?query=%7B__name__%3D%22cpu_detail_usage%22%2Cdb%3D%22system%22%7D&time=1669600800`: `{"status":"success","isPartial":false,"data":{"resultType":"vector","result":[{"metric":{"db":"system"},"value":[1669600800,"3.45"]}]}}`,
	}, log.OtLogger)

	ins := NewInstanceWithAPIPrefix(
		ctx, "http://127.0.0.1", "/select/0/prometheus/api/v1", time.Minute, mockCurl,
	)
	vector, err := ins.Query(ctx, `{__name__="cpu_detail_usage",db="system"}`, endTime)
	require.NoError(t, err)
	require.Len(t, vector, 1)
	assert.Equal(t, "system", vector[0].Metric.Get("db"))
	assert.Equal(t, 3.45, vector[0].V)
	assert.Equal(t, int64(1669600800000), vector[0].T)
}

func TestInstanceQueryRangeReturnsVMError(t *testing.T) {
	log.InitTestLogger()
	ctx := context.Background()
	endTime, _ := time.ParseInLocation(parseTime, testTime, time.Local)
	startTime := endTime.Add(-5 * time.Minute)

	mockCurl := curl.NewMockCurl(map[string]string{
		`http://127.0.0.1/api/query_range?end=1669600800&query=sum%28111&start=1669600500&step=60`: `{"status":"error","errorType":"422","error":"bad query"}`,
	}, log.OtLogger)

	ins := NewInstance(ctx, "http://127.0.0.1/api", time.Minute, mockCurl)
	_, err := ins.QueryRange(ctx, `sum(111`, startTime, endTime, time.Minute)
	assert.Equal(t, errors.New("bad query"), err)
}

func TestInstanceLabelNamesUsesNativeVMLabelsAPI(t *testing.T) {
	log.InitTestLogger()
	ctx := context.Background()
	endTime, _ := time.ParseInLocation(parseTime, testTime, time.Local)
	startTime := endTime.Add(-5 * time.Minute)
	query := nativeVMInfoTestQuery(t)
	selector := `{__name__="cpu_detail_usage",db="system",bk_biz_id="2"}`

	mockCurl := curl.NewMockCurl(map[string]string{
		nativeVMInfoURL("labels", selector, startTime, endTime): `{"status":"success","data":["bk_biz_id","db","__name__"]}`,
	}, log.OtLogger)

	ins := NewInstanceWithAPIPrefixAndBasicAuth(
		ctx, "http://127.0.0.1", "/select/0/prometheus/api/v1", "query", "secret", time.Minute, mockCurl,
	)
	names, err := ins.LabelNames(ctx, query, startTime, endTime)
	require.NoError(t, err)
	assert.Equal(t, []string{"__name__", "bk_biz_id", "db"}, names)
	assert.Equal(t, "query", mockCurl.UserName)
	assert.Equal(t, "secret", mockCurl.Password)
}

func TestInstanceLabelValuesUsesNativeVMSeriesAPI(t *testing.T) {
	log.InitTestLogger()
	ctx := context.Background()
	endTime, _ := time.ParseInLocation(parseTime, testTime, time.Local)
	startTime := endTime.Add(-5 * time.Minute)
	query := nativeVMInfoTestQuery(t)
	selector := `{__name__="cpu_detail_usage",db="system",bk_biz_id="2"}`

	mockCurl := curl.NewMockCurl(map[string]string{
		nativeVMInfoURL("series", selector, startTime, endTime): `{"status":"success","data":[{"__name__":"cpu_detail_usage","db":"system","bk_biz_id":"3"},{"__name__":"cpu_detail_usage","db":"system","bk_biz_id":"2"},{"__name__":"cpu_detail_usage","db":"system","bk_biz_id":"2"}]}`,
	}, log.OtLogger)

	ins := NewInstanceWithAPIPrefix(
		ctx, "http://127.0.0.1", "/select/0/prometheus/api/v1", time.Minute, mockCurl,
	)
	values, err := ins.LabelValues(ctx, query, "bk_biz_id", startTime, endTime)
	require.NoError(t, err)
	assert.Equal(t, []string{"2", "3"}, values)
}

func TestInstanceSeriesUsesNativeVMSeriesAPI(t *testing.T) {
	log.InitTestLogger()
	ctx := context.Background()
	endTime, _ := time.ParseInLocation(parseTime, testTime, time.Local)
	startTime := endTime.Add(-5 * time.Minute)
	query := nativeVMInfoTestQuery(t)
	selector := `{__name__="cpu_detail_usage",db="system",bk_biz_id="2"}`

	mockCurl := curl.NewMockCurl(map[string]string{
		nativeVMInfoURL("series", selector, startTime, endTime): `{"status":"success","data":[{"__name__":"cpu_detail_usage","db":"system","bk_biz_id":"2"},{"__name__":"cpu_detail_usage","db":"system","bk_biz_id":"3"}]}`,
	}, log.OtLogger)

	ins := NewInstanceWithAPIPrefix(
		ctx, "http://127.0.0.1", "/select/0/prometheus/api/v1", time.Minute, mockCurl,
	)
	set := ins.Series(ctx, query, startTime, endTime)
	require.NoError(t, set.Err())

	var got []labels.Labels
	for set.Next() {
		got = append(got, set.At().Labels())
	}
	require.NoError(t, set.Err())
	require.Len(t, got, 2)
	assert.Equal(t, "cpu_detail_usage", got[0].Get(labels.MetricName))
	assert.Equal(t, "system", got[0].Get("db"))
	assert.Equal(t, "2", got[0].Get("bk_biz_id"))
	assert.Equal(t, "3", got[1].Get("bk_biz_id"))
}

func TestInstanceQueryRawDelegatesSeriesLookup(t *testing.T) {
	log.InitTestLogger()
	ctx := context.Background()
	endTime, _ := time.ParseInLocation(parseTime, testTime, time.Local)
	startTime := endTime.Add(-5 * time.Minute)
	query := nativeVMInfoTestQuery(t)
	selector := `{__name__="cpu_detail_usage",db="system",bk_biz_id="2"}`

	mockCurl := curl.NewMockCurl(map[string]string{
		nativeVMInfoURL("series", selector, startTime, endTime): `{"status":"success","data":[{"__name__":"cpu_detail_usage","db":"system","bk_biz_id":"2"}]}`,
	}, log.OtLogger)

	ins := NewInstanceWithAPIPrefix(
		ctx, "http://127.0.0.1", "/select/0/prometheus/api/v1", time.Minute, mockCurl,
	)
	set := ins.QueryRaw(ctx, query, &storage.SelectHints{
		Start: startTime.UnixMilli(),
		End:   endTime.UnixMilli(),
		Func:  "series",
	})
	require.NotNil(t, set)
	require.NoError(t, set.Err())
	require.True(t, set.Next())
	assert.Equal(t, "2", set.At().Labels().Get("bk_biz_id"))
}

func nativeVMInfoTestQuery(t *testing.T) *metadata.Query {
	t.Helper()

	bizMatcher, err := labels.NewMatcher(labels.MatchEqual, "bk_biz_id", "2")
	require.NoError(t, err)

	return &metadata.Query{
		DB:                                "system",
		Measurement:                       "cpu_detail",
		Field:                             "usage",
		NativeVMDBLabel:                   "db",
		NativeVMMeasurementFieldSeparator: "_",
		NativeVMMatchers:                  []*labels.Matcher{bizMatcher},
	}
}

func nativeVMInfoURL(name, selector string, start, end time.Time) string {
	values := &url.Values{}
	values.Set("match[]", selector)
	values.Set("start", strconv.FormatInt(start.Unix(), 10))
	values.Set("end", strconv.FormatInt(end.Unix(), 10))
	return "http://127.0.0.1/select/0/prometheus/api/v1/" + name + "?" + values.Encode()
}
