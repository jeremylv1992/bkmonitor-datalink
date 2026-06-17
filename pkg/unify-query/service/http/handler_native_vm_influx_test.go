// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	omd "github.com/TencentBlueKing/bkmonitor-datalink/pkg/offline-data-archive/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/offline-data-archive/policy/stores/shard"
	goRedis "github.com/go-redis/redis/v8"
	"github.com/prometheus/prometheus/model/labels"
	promPromql "github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
	uqInfluxdb "github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/influxdb"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/influxdb/decoder"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/mock"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/redis"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb"
	ir "github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/router/influxdb"
)

type emptyArchiveMetadata struct{}

func (m *emptyArchiveMetadata) PublishShard(ctx context.Context, channelValue interface{}) error {
	return nil
}

func (m *emptyArchiveMetadata) SubscribeShard(ctx context.Context) <-chan *goRedis.Message {
	return nil
}

func (m *emptyArchiveMetadata) GetShardID(ctx context.Context, sd *shard.Shard) (string, error) {
	return "", nil
}

func (m *emptyArchiveMetadata) GetAllShards(ctx context.Context) map[string]*shard.Shard {
	return nil
}

func (m *emptyArchiveMetadata) SetShard(ctx context.Context, k string, sd *shard.Shard) error {
	return nil
}

func (m *emptyArchiveMetadata) GetShard(ctx context.Context, k string) (*shard.Shard, error) {
	return nil, nil
}

func (m *emptyArchiveMetadata) GetDistributedLock(ctx context.Context, key, val string, expiration time.Duration) (string, error) {
	return "", nil
}

func (m *emptyArchiveMetadata) RenewalLock(ctx context.Context, key string, renewalDuration time.Duration) (bool, error) {
	return true, nil
}

func (m *emptyArchiveMetadata) GetPolicies(ctx context.Context, clusterName, tagRouter string) (map[string]*omd.Policy, error) {
	return nil, nil
}

func (m *emptyArchiveMetadata) GetShards(ctx context.Context, clusterName, tagRouter, database string) (map[string]*shard.Shard, error) {
	return nil, nil
}

func (m *emptyArchiveMetadata) GetReadShardsByTimeRange(ctx context.Context, clusterName, tagRouter, database, retentionPolicy string, start int64, end int64) ([]*shard.Shard, error) {
	return nil, nil
}

type captureNativeVMInstance struct {
	promql string
}

func (i *captureNativeVMInstance) QueryRaw(ctx context.Context, query *metadata.Query, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return nil
}

func (i *captureNativeVMInstance) QueryRange(ctx context.Context, promql string, start, end time.Time, step time.Duration) (promPromql.Matrix, error) {
	i.promql = promql
	return promPromql.Matrix{
		{
			Metric: labels.Labels{
				{Name: "db", Value: "system"},
			},
			Points: []promPromql.Point{
				{T: start.UnixMilli(), V: 1.23},
			},
		},
	}, nil
}

func (i *captureNativeVMInstance) Query(ctx context.Context, promql string, end time.Time) (promPromql.Vector, error) {
	i.promql = promql
	return promPromql.Vector{
		{
			Metric: labels.Labels{
				{Name: "db", Value: "system"},
			},
			Point: promPromql.Point{T: end.UnixMilli(), V: 1.23},
		},
	}, nil
}

func (i *captureNativeVMInstance) QueryExemplar(ctx context.Context, fields []string, query *metadata.Query, start, end time.Time, matchers ...*labels.Matcher) (*decoder.Response, error) {
	return nil, nil
}

func (i *captureNativeVMInstance) LabelNames(ctx context.Context, query *metadata.Query, start, end time.Time, matchers ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (i *captureNativeVMInstance) LabelValues(ctx context.Context, query *metadata.Query, name string, start, end time.Time, matchers ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (i *captureNativeVMInstance) Series(ctx context.Context, query *metadata.Query, start, end time.Time, matchers ...*labels.Matcher) storage.SeriesSet {
	return nil
}

func (i *captureNativeVMInstance) GetInstanceType() string {
	return consul.VictoriaMetricsStorageType
}

func TestQueryTsNativeVMInfluxUsesInfluxMetadataMetricQL(t *testing.T) {
	ctx := context.Background()
	log.InitTestLogger()
	mock.SetOfflineDataArchiveMetadata(&emptyArchiveMetadata{})

	var (
		called         bool
		capturedPath   string
		capturedPromQL string
	)
	vmselect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		capturedPath = r.URL.Path
		capturedPromQL = r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","isPartial":false,"data":{"resultType":"matrix","result":[{"metric":{"db":"system"},"values":[[1677081600,"1.23"]]}]}}`)
	}))
	t.Cleanup(vmselect.Close)
	reloadTestNativeVMClusterInfo(t, "cluster-a", vmselect.URL)

	storageID := "native-vm-handler-storage"
	instance := &captureNativeVMInstance{}
	tsdb.SetStorage(storageID, &tsdb.Storage{
		Type:     consul.InfluxDBStorageType,
		Address:  "http://influxdb-proxy:8080",
		Timeout:  time.Minute,
		Instance: instance,
	})

	spaceUid := "native-vm-handler-space"
	tableID := "system.cpu_detail"
	mock.SetSpaceAndProxyMockData(
		ctx,
		"native_vm_handler_test",
		"native_vm_handler_test",
		spaceUid,
		&redis.TsDB{
			TableID:         tableID,
			Field:           []string{"usage"},
			MeasurementType: redis.BKTraditionalMeasurement,
		},
		&ir.Proxy{
			MeasurementType: redis.BKTraditionalMeasurement,
			StorageID:       storageID,
			ClusterName:     "cluster-a",
			Db:              "system",
			Measurement:     "cpu_detail",
			VmRt:            "legacy_vm_rt_should_not_be_used",
		},
	)

	_, err := queryTs(ctx, &structured.QueryTs{
		SpaceUid: spaceUid,
		QueryList: []*structured.Query{
			{
				TableID:       structured.TableID(tableID),
				FieldName:     "usage",
				ReferenceName: "a",
				Conditions: structured.Conditions{
					FieldList: []structured.ConditionField{
						{
							DimensionName: "bk_biz_id",
							Operator:      structured.ConditionEqual,
							Value:         []string{"2"},
						},
					},
				},
			},
		},
		MetricMerge: "a",
		Start:       "1677081600",
		End:         "1677081660",
		Step:        "60s",
	})
	require.NoError(t, err)

	require.True(t, called)
	assert.Empty(t, instance.promql)
	assert.Equal(t, "/select/0/prometheus/api/v1/query_range", capturedPath)
	assert.Contains(t, capturedPromQL, `__name__="cpu_detail_usage"`)
	assert.Contains(t, capturedPromQL, `db="system"`)
	assert.Contains(t, capturedPromQL, `bk_biz_id="2"`)
	assert.NotContains(t, capturedPromQL, "result_table_id")
	assert.NotContains(t, capturedPromQL, "legacy_vm_rt_should_not_be_used")
}

func TestQueryTsNativeVMInfluxUsesMetricQLOrSelector(t *testing.T) {
	ctx := context.Background()
	log.InitTestLogger()
	mock.SetOfflineDataArchiveMetadata(&emptyArchiveMetadata{})

	var capturedPromQL string
	vmselect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPromQL = r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","isPartial":false,"data":{"resultType":"matrix","result":[{"metric":{"db":"system"},"values":[[1677081600,"1.23"]]}]}}`)
	}))
	t.Cleanup(vmselect.Close)
	reloadTestNativeVMClusterInfo(t, "cluster-a", vmselect.URL)

	storageID := "native-vm-handler-or-storage"
	tsdb.SetStorage(storageID, &tsdb.Storage{
		Type:    consul.InfluxDBStorageType,
		Address: "http://influxdb-proxy:8080",
		Timeout: time.Minute,
	})

	spaceUid := "native-vm-handler-or-space"
	tableID := "system.cpu_detail"
	mock.SetSpaceAndProxyMockData(
		ctx,
		"native_vm_handler_or_test",
		"native_vm_handler_or_test",
		spaceUid,
		&redis.TsDB{
			TableID:         tableID,
			Field:           []string{"usage"},
			MeasurementType: redis.BKTraditionalMeasurement,
		},
		&ir.Proxy{
			MeasurementType: redis.BKTraditionalMeasurement,
			StorageID:       storageID,
			ClusterName:     "cluster-a",
			Db:              "system",
			Measurement:     "cpu_detail",
		},
	)

	_, err := queryTs(ctx, &structured.QueryTs{
		SpaceUid: spaceUid,
		QueryList: []*structured.Query{
			{
				TableID:       structured.TableID(tableID),
				FieldName:     "usage",
				ReferenceName: "a",
				Conditions: structured.Conditions{
					FieldList: []structured.ConditionField{
						{
							DimensionName: "bk_biz_id",
							Operator:      structured.ConditionEqual,
							Value:         []string{"2"},
						},
						{
							DimensionName: "bk_biz_id",
							Operator:      structured.ConditionEqual,
							Value:         []string{"3"},
						},
					},
					ConditionList: []string{structured.ConditionOr},
				},
				TimeAggregation: structured.TimeAggregation{
					Function: "avg_over_time",
					Window:   "1m",
				},
			},
		},
		MetricMerge: "a",
		Start:       "1677081600",
		End:         "1677081660",
		Step:        "60s",
	})
	require.NoError(t, err)

	assert.Contains(t, capturedPromQL, `avg_over_time({`)
	assert.Contains(
		t,
		capturedPromQL,
		`__name__="cpu_detail_usage",db="system",bk_biz_id="2" or __name__="cpu_detail_usage",db="system",bk_biz_id="3"`,
	)
	assert.Contains(t, capturedPromQL, `}[1m]`)
	assert.NotContains(t, capturedPromQL, "__bk_native_vm_ref_")
	assert.NotContains(t, capturedPromQL, `{__name__="a"}`)
}

func TestQueryTsNativeVMInfluxExpandsAllReferencesInBinaryExpr(t *testing.T) {
	ctx := context.Background()
	log.InitTestLogger()
	mock.SetOfflineDataArchiveMetadata(&emptyArchiveMetadata{})

	var capturedPromQL string
	vmselect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPromQL = r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","isPartial":false,"data":{"resultType":"matrix","result":[]}}`)
	}))
	t.Cleanup(vmselect.Close)
	reloadTestNativeVMClusterInfo(t, "cluster-a", vmselect.URL)

	storageID := "native-vm-multi-ref-storage"
	tsdb.SetStorage(storageID, &tsdb.Storage{
		Type:    consul.InfluxDBStorageType,
		Address: "http://influxdb-proxy:8080",
		Timeout: time.Minute,
	})

	spaceUid := "native-vm-multi-ref-space"
	mockNativeVMInfluxTables(ctx, spaceUid, storageID, map[string]nativeVMInfluxTable{
		"system.container_memory_rss": {
			field:       "value",
			db:          "system",
			measurement: "container_memory_rss",
		},
		"system.container_spec_memory_limit_bytes": {
			field:       "value",
			db:          "system",
			measurement: "container_spec_memory_limit_bytes",
		},
	})

	_, err := queryTs(ctx, &structured.QueryTs{
		SpaceUid: spaceUid,
		QueryList: []*structured.Query{
			{
				TableID:       "system.container_memory_rss",
				FieldName:     "value",
				ReferenceName: "a",
				AggregateMethodList: []structured.AggregateMethod{
					{Method: "max", Dimensions: []string{"bcs_cluster_id", "pod_name"}},
				},
			},
			{
				TableID:       "system.container_spec_memory_limit_bytes",
				FieldName:     "value",
				ReferenceName: "b",
				AggregateMethodList: []structured.AggregateMethod{
					{Method: "max", Dimensions: []string{"bcs_cluster_id", "pod_name"}},
				},
			},
		},
		MetricMerge: "a/b*100",
		Start:       "1677081600",
		End:         "1677081660",
		Step:        "60s",
	})
	require.NoError(t, err)

	assert.Contains(t, capturedPromQL, `__name__="container_memory_rss_value"`)
	assert.Contains(t, capturedPromQL, `__name__="container_spec_memory_limit_bytes_value"`)
	assert.NotContains(t, capturedPromQL, "(b)")
	assert.NotContains(t, capturedPromQL, `{__name__="b"}`)
}

func TestQueryTsNativeVMInfluxExpandsAllReferencesInThreeReferenceExpr(t *testing.T) {
	ctx := context.Background()
	log.InitTestLogger()
	mock.SetOfflineDataArchiveMetadata(&emptyArchiveMetadata{})

	var capturedPromQL string
	vmselect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPromQL = r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","isPartial":false,"data":{"resultType":"matrix","result":[]}}`)
	}))
	t.Cleanup(vmselect.Close)
	reloadTestNativeVMClusterInfo(t, "cluster-a", vmselect.URL)

	storageID := "native-vm-three-ref-storage"
	tsdb.SetStorage(storageID, &tsdb.Storage{
		Type:    consul.InfluxDBStorageType,
		Address: "http://influxdb-proxy:8080",
		Timeout: time.Minute,
	})

	spaceUid := "native-vm-three-ref-space"
	mockNativeVMInfluxTables(ctx, spaceUid, storageID, map[string]nativeVMInfluxTable{
		"system.kube_pod_container_resource_requests_cpu_cores": {
			field:       "value",
			db:          "system",
			measurement: "kube_pod_container_resource_requests_cpu_cores",
		},
		"system.kube_node_status_allocatable_cpu_cores": {
			field:       "value",
			db:          "system",
			measurement: "kube_node_status_allocatable_cpu_cores",
		},
	})

	_, err := queryTs(ctx, &structured.QueryTs{
		SpaceUid: spaceUid,
		QueryList: []*structured.Query{
			{
				TableID:       "system.kube_pod_container_resource_requests_cpu_cores",
				FieldName:     "value",
				ReferenceName: "a",
				AggregateMethodList: []structured.AggregateMethod{
					{Method: "sum", Dimensions: []string{"bcs_cluster_id"}},
				},
			},
			{
				TableID:       "system.kube_node_status_allocatable_cpu_cores",
				FieldName:     "value",
				ReferenceName: "b",
				AggregateMethodList: []structured.AggregateMethod{
					{Method: "sum", Dimensions: []string{"bcs_cluster_id"}},
				},
			},
			{
				TableID:       "system.kube_node_status_allocatable_cpu_cores",
				FieldName:     "value",
				ReferenceName: "c",
				AggregateMethodList: []structured.AggregateMethod{
					{Method: "count", Dimensions: []string{"bcs_cluster_id"}},
				},
			},
		},
		MetricMerge: "a/b-(c-1)/c",
		Start:       "1677081600",
		End:         "1677081660",
		Step:        "60s",
	})
	require.NoError(t, err)

	assert.Contains(t, capturedPromQL, `__name__="kube_pod_container_resource_requests_cpu_cores_value"`)
	assert.Contains(t, capturedPromQL, `__name__="kube_node_status_allocatable_cpu_cores_value"`)
	assert.NotContains(t, capturedPromQL, "(b)")
	assert.NotContains(t, capturedPromQL, "(c)")
	assert.NotContains(t, capturedPromQL, `{__name__="b"}`)
	assert.NotContains(t, capturedPromQL, `{__name__="c"}`)
}

func TestQueryTsNativeVMInfluxKeepsBusinessStatusForMissingReferenceSelector(t *testing.T) {
	ctx := context.Background()
	log.InitTestLogger()
	mock.SetOfflineDataArchiveMetadata(&emptyArchiveMetadata{})

	var capturedPromQL string
	vmselect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPromQL = r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","isPartial":false,"data":{"resultType":"matrix","result":[]}}`)
	}))
	t.Cleanup(vmselect.Close)
	reloadTestNativeVMClusterInfo(t, "cluster-a", vmselect.URL)

	storageID := "native-vm-missing-ref-status-storage"
	tsdb.SetStorage(storageID, &tsdb.Storage{
		Type:    consul.InfluxDBStorageType,
		Address: "http://influxdb-proxy:8080",
		Timeout: time.Minute,
	})

	spaceUid := "native-vm-missing-ref-status-space"
	mockNativeVMInfluxTables(ctx, spaceUid, storageID, map[string]nativeVMInfluxTable{
		"system.kube_pod_container_resource_requests_cpu_cores": {
			field:       "value",
			db:          "system",
			measurement: "kube_pod_container_resource_requests_cpu_cores",
		},
	})

	res, err := queryTs(ctx, &structured.QueryTs{
		SpaceUid: spaceUid,
		QueryList: []*structured.Query{
			{
				TableID:       "system.kube_pod_container_resource_requests_cpu_cores",
				FieldName:     "value",
				ReferenceName: "a",
				AggregateMethodList: []structured.AggregateMethod{
					{Method: "sum", Dimensions: []string{"bcs_cluster_id"}},
				},
			},
			{
				TableID:       "system.kube_node_status_allocatable_cpu_cores",
				FieldName:     "value",
				ReferenceName: "b",
				AggregateMethodList: []structured.AggregateMethod{
					{Method: "sum", Dimensions: []string{"bcs_cluster_id"}},
				},
			},
			{
				TableID:       "system.kube_node_status_allocatable_cpu_cores",
				FieldName:     "value",
				ReferenceName: "c",
				AggregateMethodList: []structured.AggregateMethod{
					{Method: "count", Dimensions: []string{"bcs_cluster_id"}},
				},
			},
		},
		MetricMerge: "a/b-(c-1)/c",
		Start:       "1677081600",
		End:         "1677081660",
		Step:        "60s",
	})
	require.NoError(t, err)

	resp, ok := res.(*PromData)
	require.True(t, ok)
	require.NotNil(t, resp.Status)
	assert.Equal(t, metadata.SpaceTableIDFieldIsNotExists, resp.Status.Code)
	assert.Contains(t, resp.Status.Message, "kube_node_status_allocatable_cpu_cores")
	assert.Contains(t, capturedPromQL, `__name__="kube_pod_container_resource_requests_cpu_cores_value"`)
	assert.Contains(t, capturedPromQL, structured.NativeVMMissingReferenceSelector("b"))
	assert.Contains(t, capturedPromQL, structured.NativeVMMissingReferenceSelector("c"))
	assert.NotContains(t, capturedPromQL, `{__name__="b"}`)
	assert.NotContains(t, capturedPromQL, `{__name__="c"}`)
}

func TestQueryTsNativeVMInfluxExpandsReferenceInNestedAggregateExpr(t *testing.T) {
	ctx := context.Background()
	log.InitTestLogger()
	mock.SetOfflineDataArchiveMetadata(&emptyArchiveMetadata{})

	var capturedPromQL string
	vmselect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPromQL = r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","isPartial":false,"data":{"resultType":"matrix","result":[]}}`)
	}))
	t.Cleanup(vmselect.Close)
	reloadTestNativeVMClusterInfo(t, "cluster-a", vmselect.URL)

	storageID := "native-vm-nested-ref-storage"
	tsdb.SetStorage(storageID, &tsdb.Storage{
		Type:    consul.InfluxDBStorageType,
		Address: "http://influxdb-proxy:8080",
		Timeout: time.Minute,
	})

	spaceUid := "native-vm-nested-ref-space"
	mockNativeVMInfluxTables(ctx, spaceUid, storageID, map[string]nativeVMInfluxTable{
		"system.kube_pod_status_phase": {
			field:       "value",
			db:          "system",
			measurement: "kube_pod_status_phase",
		},
		"system.kube_pod_owner": {
			field:       "value",
			db:          "system",
			measurement: "kube_pod_owner",
		},
	})

	_, err := queryTs(ctx, &structured.QueryTs{
		SpaceUid: spaceUid,
		QueryList: []*structured.Query{
			{
				TableID:       "system.kube_pod_status_phase",
				FieldName:     "value",
				ReferenceName: "a",
				AggregateMethodList: []structured.AggregateMethod{
					{Method: "max", Dimensions: []string{"bcs_cluster_id", "namespace", "pod"}},
				},
			},
			{
				TableID:       "system.kube_pod_owner",
				FieldName:     "value",
				ReferenceName: "b",
				AggregateMethodList: []structured.AggregateMethod{
					{Method: "max", Dimensions: []string{"bcs_cluster_id", "namespace", "pod"}},
					{Method: "topk", VArgsList: []interface{}{1}, Position: 1},
				},
			},
		},
		MetricMerge: "a * b",
		Start:       "1677081600",
		End:         "1677081660",
		Step:        "60s",
	})
	require.NoError(t, err)

	assert.Contains(t, capturedPromQL, `__name__="kube_pod_status_phase_value"`)
	assert.Contains(t, capturedPromQL, `__name__="kube_pod_owner_value"`)
	assert.Contains(t, capturedPromQL, "topk")
	assert.NotContains(t, capturedPromQL, "(b)")
	assert.NotContains(t, capturedPromQL, `{__name__="b"}`)
}

func reloadTestNativeVMClusterInfo(t *testing.T, clusterName, selectAddress string) {
	t.Helper()

	oldHGetAll := redis.HGetAll
	defer func() {
		redis.HGetAll = oldHGetAll
	}()
	redis.HGetAll = func(ctx context.Context, key string) (map[string]string, error) {
		return map[string]string{
			clusterName: `{
				"cluster_name": "` + clusterName + `",
				"readable": true,
				"select": {
					"address": "` + selectAddress + `",
					"api_prefix": "/select/0/prometheus/api/v1",
					"basic_auth": {"username": "query", "password": "secret"}
				},
				"influx_compat": {
					"db_label": "db",
					"measurement_field_separator": "_",
					"skip_single_field": false
				}
			}`,
		}, nil
	}
	require.NoError(t, consul.ReloadVMClusterInfo())
	t.Cleanup(func() {
		cleanupHGetAll := redis.HGetAll
		defer func() {
			redis.HGetAll = cleanupHGetAll
		}()
		redis.HGetAll = func(ctx context.Context, key string) (map[string]string, error) {
			return map[string]string{}, nil
		}
		require.NoError(t, consul.ReloadVMClusterInfo())
	})
}

type nativeVMInfluxTable struct {
	field       string
	db          string
	measurement string
}

func mockNativeVMInfluxTables(ctx context.Context, spaceUid, storageID string, tables map[string]nativeVMInfluxTable) {
	for tableID, table := range tables {
		mock.SetSpaceAndProxyMockData(
			ctx,
			"native_vm_multi_ref_test",
			"native_vm_multi_ref_test",
			spaceUid,
			&redis.TsDB{
				TableID:         tableID,
				Field:           []string{table.field},
				MeasurementType: redis.BKTraditionalMeasurement,
			},
			&ir.Proxy{
				MeasurementType: redis.BKTraditionalMeasurement,
				StorageID:       storageID,
				ClusterName:     "cluster-a",
				Db:              table.db,
				Measurement:     table.measurement,
			},
		)
	}

	proxyInfo := ir.ProxyInfo{}
	for tableID, table := range tables {
		proxyInfo[tableID] = &ir.Proxy{
			MeasurementType: redis.BKTraditionalMeasurement,
			StorageID:       storageID,
			ClusterName:     "cluster-a",
			Db:              table.db,
			Measurement:     table.measurement,
		}
	}
	uqInfluxdb.MockRouter(proxyInfo, ir.QueryRouterInfo{})
}
