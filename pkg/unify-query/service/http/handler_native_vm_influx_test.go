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
	"github.com/hashicorp/consul/api"
	"github.com/prometheus/prometheus/model/labels"
	promPromql "github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
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

func reloadTestNativeVMClusterInfo(t *testing.T, clusterName, selectAddress string) {
	t.Helper()

	oldGetDataWithPrefix := consul.GetDataWithPrefix
	defer func() {
		consul.GetDataWithPrefix = oldGetDataWithPrefix
	}()
	consul.GetDataWithPrefix = func(prefix string) (api.KVPairs, error) {
		return api.KVPairs{
			{
				Key: "bkmonitorv3/unify-query/data/vmcluster_info/" + clusterName,
				Value: []byte(`{
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
				}`),
			},
		}, nil
	}
	require.NoError(t, consul.ReloadVMClusterInfo())
	t.Cleanup(func() {
		cleanupGetDataWithPrefix := consul.GetDataWithPrefix
		defer func() {
			consul.GetDataWithPrefix = cleanupGetDataWithPrefix
		}()
		consul.GetDataWithPrefix = func(prefix string) (api.KVPairs, error) {
			return api.KVPairs{}, nil
		}
		require.NoError(t, consul.ReloadVMClusterInfo())
	})
}
