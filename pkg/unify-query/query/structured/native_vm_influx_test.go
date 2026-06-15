// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package structured

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/mock"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/redis"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb"
	ir "github.com/TencentBlueKing/bkmonitor-datalink/pkg/utils/router/influxdb"
)

func TestNativeVMInfluxMetricName(t *testing.T) {
	assert.Equal(t, "cpu_detail_usage", NativeVMInfluxMetricName("cpu_detail", "usage", "_", false))
	assert.Equal(t, "usage_value", NativeVMInfluxMetricName("usage", metadata.StaticField, "_", false))
	assert.Equal(t, "usage", NativeVMInfluxMetricName("usage", metadata.StaticField, "_", true))
}

func TestNativeVMInfluxLabelMatchers(t *testing.T) {
	matcher, err := labels.NewMatcher(labels.MatchEqual, "bk_biz_id", "2")
	require.NoError(t, err)

	matchers, err := NativeVMInfluxLabelMatchers(&metadata.Query{
		DB:                                "system",
		Measurement:                       "cpu_detail",
		Field:                             "usage",
		NativeVMDBLabel:                   "db",
		NativeVMMeasurementFieldSeparator: "_",
		NativeVMSkipSingleField:           false,
		NativeVMMatchers:                  []*labels.Matcher{matcher},
	})
	require.NoError(t, err)

	assertNativeMatcher(t, matchers, labels.MetricName, labels.MatchEqual, "cpu_detail_usage")
	assertNativeMatcher(t, matchers, "db", labels.MatchEqual, "system")
	assertNativeMatcher(t, matchers, "bk_biz_id", labels.MatchEqual, "2")
}

func TestNativeVMInfluxQueryMetadata(t *testing.T) {
	ctx := context.Background()
	storageID := "native-vm-influx-query-metadata"
	spaceUid := "native-vm-space"
	tableID := "system.cpu_detail"
	mock.SetOfflineDataArchiveMetadata(&m{})
	reloadTestVMClusterInfo(t, "cluster-a")

	tsdb.SetStorage(storageID, &tsdb.Storage{
		Type:    consul.InfluxDBStorageType,
		Address: "http://influxdb-proxy:8080",
		Timeout: time.Minute,
	})
	mock.SetSpaceAndProxyMockData(
		ctx,
		"native_vm_influx_query_metadata",
		"native_vm_influx_query_metadata",
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

	query := &Query{
		TableID:       TableID(tableID),
		FieldName:     "usage",
		ReferenceName: "a",
		Start:         "0",
		End:           "60",
		Step:          "60s",
		Conditions: Conditions{
			FieldList: []ConditionField{
				{
					DimensionName: "bk_biz_id",
					Operator:      ConditionEqual,
					Value:         []string{"2"},
				},
			},
		},
	}

	queryMetric, err := query.ToQueryMetric(ctx, spaceUid)
	require.NoError(t, err)
	require.Len(t, queryMetric.QueryList, 1)

	qry := queryMetric.QueryList[0]
	assert.True(t, qry.NativeVMInflux)
	assert.Equal(t, "system", qry.DB)
	assert.Equal(t, "cpu_detail", qry.Measurement)
	assert.Equal(t, "usage", qry.Field)
	assert.Equal(t, "http://vmselect:8481", qry.NativeVMAddress)
	assert.Equal(t, "/select/0/prometheus/api/v1", qry.NativeVMAPIPrefix)
	assert.Equal(t, "db", qry.NativeVMDBLabel)
	assertNativeMatcher(t, qry.NativeVMMatchers, "bk_biz_id", labels.MatchEqual, "2")
}

func reloadTestVMClusterInfo(t *testing.T, clusterName string) {
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
						"address": "http://vmselect:8481",
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

func assertNativeMatcher(t *testing.T, matchers []*labels.Matcher, name string, matchType labels.MatchType, value string) {
	t.Helper()
	for _, matcher := range matchers {
		if matcher.Name == name {
			assert.Equal(t, matchType, matcher.Type)
			assert.Equal(t, value, matcher.Value)
			return
		}
	}
	t.Fatalf("matcher %s=%q not found in %+v", name, value, matchers)
}
