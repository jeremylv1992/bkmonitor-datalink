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

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
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

func TestConditionGroupsToNativeVMMatchersMultiValue(t *testing.T) {
	tests := []struct {
		name      string
		cond      ConditionField
		matchType labels.MatchType
		value     string
	}{
		{
			name: "eq multi values become exact regex union",
			cond: ConditionField{
				DimensionName: "bk_target_ip",
				Operator:      ConditionEqual,
				Value:         []string{"10.10.10.80", "10.10.10.81"},
			},
			matchType: labels.MatchRegexp,
			value:     `(?:10\.10\.10\.80|10\.10\.10\.81)`,
		},
		{
			name: "ne multi values become negative exact regex union",
			cond: ConditionField{
				DimensionName: "bk_target_ip",
				Operator:      ConditionNotEqual,
				Value:         []string{"10.10.10.80", "10.10.10.81"},
			},
			matchType: labels.MatchNotRegexp,
			value:     `(?:10\.10\.10\.80|10\.10\.10\.81)`,
		},
		{
			name: "contains multi values become literal substring regex union",
			cond: ConditionField{
				DimensionName: "bk_target_ip",
				Operator:      ConditionContains,
				Value:         []string{"10.10.10.80", "10.10.10.81"},
			},
			matchType: labels.MatchRegexp,
			value:     `.*(?:10\.10\.10\.80|10\.10\.10\.81).*`,
		},
		{
			name: "ncontains multi values become negative literal substring regex union",
			cond: ConditionField{
				DimensionName: "bk_target_ip",
				Operator:      ConditionNotContains,
				Value:         []string{"10.10.10.80", "10.10.10.81"},
			},
			matchType: labels.MatchNotRegexp,
			value:     `.*(?:10\.10\.10\.80|10\.10\.10\.81).*`,
		},
		{
			name: "req multi values become raw substring regex union",
			cond: ConditionField{
				DimensionName: "pod_name",
				Operator:      ConditionRegEqual,
				Value:         []string{"kube-apiserver", "kube-scheduler"},
			},
			matchType: labels.MatchRegexp,
			value:     `.*(?:kube-apiserver|kube-scheduler).*`,
		},
		{
			name: "nreq multi values become negative raw substring regex union",
			cond: ConditionField{
				DimensionName: "pod_name",
				Operator:      ConditionNotRegEqual,
				Value:         []string{"kube-apiserver", "kube-scheduler"},
			},
			matchType: labels.MatchNotRegexp,
			value:     `.*(?:kube-apiserver|kube-scheduler).*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groups, err := conditionGroupsToNativeVMMatchers([][]ConditionField{{tt.cond}})
			require.NoError(t, err)
			require.Len(t, groups, 1)
			require.Len(t, groups[0], 1)
			require.Equal(t, tt.cond.DimensionName, groups[0][0].Name)
			require.Equal(t, tt.matchType, groups[0][0].Type)
			require.Equal(t, tt.value, groups[0][0].Value)
		})
	}
}

func TestConditionGroupsToNativeVMMatchersSubstringOpsHandleSingleAndMultiValuesConsistently(t *testing.T) {
	tests := []struct {
		name      string
		cond      ConditionField
		matchType labels.MatchType
		value     string
	}{
		{
			name: "contains single",
			cond: ConditionField{
				DimensionName: "label",
				Operator:      ConditionContains,
				Value:         []string{"a.b"},
			},
			matchType: labels.MatchRegexp,
			value:     `.*(?:a\.b).*`,
		},
		{
			name: "contains multi",
			cond: ConditionField{
				DimensionName: "label",
				Operator:      ConditionContains,
				Value:         []string{"a.b", "c.d"},
			},
			matchType: labels.MatchRegexp,
			value:     `.*(?:a\.b|c\.d).*`,
		},
		{
			name: "req single",
			cond: ConditionField{
				DimensionName: "label",
				Operator:      ConditionRegEqual,
				Value:         []string{"a.b"},
			},
			matchType: labels.MatchRegexp,
			value:     `.*(?:a.b).*`,
		},
		{
			name: "req multi",
			cond: ConditionField{
				DimensionName: "label",
				Operator:      ConditionRegEqual,
				Value:         []string{"a.b", "c.d"},
			},
			matchType: labels.MatchRegexp,
			value:     `.*(?:a.b|c.d).*`,
		},
		{
			name: "ncontains single",
			cond: ConditionField{
				DimensionName: "label",
				Operator:      ConditionNotContains,
				Value:         []string{"a.b"},
			},
			matchType: labels.MatchNotRegexp,
			value:     `.*(?:a\.b).*`,
		},
		{
			name: "ncontains empty string uses exact not equal",
			cond: ConditionField{
				DimensionName: "label",
				Operator:      ConditionNotContains,
				Value:         []string{""},
			},
			matchType: labels.MatchNotEqual,
			value:     "",
		},
		{
			name: "nreq single",
			cond: ConditionField{
				DimensionName: "label",
				Operator:      ConditionNotRegEqual,
				Value:         []string{"a.b"},
			},
			matchType: labels.MatchNotRegexp,
			value:     `.*(?:a.b).*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groups, err := conditionGroupsToNativeVMMatchers([][]ConditionField{{tt.cond}})
			require.NoError(t, err)
			require.Len(t, groups, 1)
			require.Len(t, groups[0], 1)
			require.Equal(t, "label", groups[0][0].Name)
			require.Equal(t, tt.matchType, groups[0][0].Type)
			require.Equal(t, tt.value, groups[0][0].Value)
		})
	}
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

func TestNativeVMInfluxQueryMetadataKeepsOrMatcherGroups(t *testing.T) {
	ctx := context.Background()
	storageID := "native-vm-influx-query-or-metadata"
	spaceUid := "native-vm-or-space"
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
		"native_vm_influx_query_or_metadata",
		"native_vm_influx_query_or_metadata",
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
				{
					DimensionName: "bk_biz_id",
					Operator:      ConditionEqual,
					Value:         []string{"3"},
				},
			},
			ConditionList: []string{ConditionOr},
		},
	}

	queryMetric, err := query.ToQueryMetric(ctx, spaceUid)
	require.NoError(t, err)
	require.Len(t, queryMetric.QueryList, 1)

	qry := queryMetric.QueryList[0]
	require.True(t, qry.NativeVMInflux)
	require.Len(t, qry.NativeVMMatcherGroups, 2)
	assertNativeMatcher(t, qry.NativeVMMatcherGroups[0], "bk_biz_id", labels.MatchEqual, "2")
	assertNativeMatcher(t, qry.NativeVMMatcherGroups[1], "bk_biz_id", labels.MatchEqual, "3")
	assert.False(t, qry.NativeVMUnsupportedOr)
}

func TestNativeVMInfluxQueryMetadataCombinesQueryAndFilterOrGroups(t *testing.T) {
	ctx := context.Background()
	storageID := "native-vm-influx-query-filter-or"
	spaceUid := "native-vm-filter-or-space"
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
		"native_vm_influx_query_filter_or",
		"native_vm_influx_query_filter_or",
		spaceUid,
		&redis.TsDB{
			TableID:         tableID,
			Field:           []string{"usage"},
			MeasurementType: redis.BKTraditionalMeasurement,
			Filters: []redis.Filter{
				{"bcs_cluster_id": "cluster-a"},
				{"bcs_cluster_id": "cluster-b"},
			},
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
				{
					DimensionName: "bk_biz_id",
					Operator:      ConditionEqual,
					Value:         []string{"3"},
				},
			},
			ConditionList: []string{ConditionOr},
		},
	}

	queryMetric, err := query.ToQueryMetric(ctx, spaceUid)
	require.NoError(t, err)
	require.Len(t, queryMetric.QueryList, 1)

	qry := queryMetric.QueryList[0]
	require.True(t, qry.NativeVMInflux)
	require.Len(t, qry.NativeVMMatcherGroups, 4)
	assertNativeMatcher(t, qry.NativeVMMatcherGroups[0], "bk_biz_id", labels.MatchEqual, "2")
	assertNativeMatcher(t, qry.NativeVMMatcherGroups[0], "bcs_cluster_id", labels.MatchRegexp, `.*(?:cluster-a).*`)
	assertNativeMatcher(t, qry.NativeVMMatcherGroups[1], "bk_biz_id", labels.MatchEqual, "2")
	assertNativeMatcher(t, qry.NativeVMMatcherGroups[1], "bcs_cluster_id", labels.MatchRegexp, `.*(?:cluster-b).*`)
	assertNativeMatcher(t, qry.NativeVMMatcherGroups[2], "bk_biz_id", labels.MatchEqual, "3")
	assertNativeMatcher(t, qry.NativeVMMatcherGroups[2], "bcs_cluster_id", labels.MatchRegexp, `.*(?:cluster-a).*`)
	assertNativeMatcher(t, qry.NativeVMMatcherGroups[3], "bk_biz_id", labels.MatchEqual, "3")
	assertNativeMatcher(t, qry.NativeVMMatcherGroups[3], "bcs_cluster_id", labels.MatchRegexp, `.*(?:cluster-b).*`)
}

func TestNativeVMInfluxQueryMetadataUsesNativeVMMatchersForMultiValueAndFilterContains(t *testing.T) {
	ctx := context.Background()
	storageID := "native-vm-influx-query-multi-value"
	spaceUid := "native-vm-multi-value-space"
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
		"native_vm_influx_query_multi_value",
		"native_vm_influx_query_multi_value",
		spaceUid,
		&redis.TsDB{
			TableID:         tableID,
			Field:           []string{"usage"},
			MeasurementType: redis.BKTraditionalMeasurement,
			Filters: []redis.Filter{
				{"bcs_cluster_id": "cluster.a"},
			},
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
					DimensionName: "bk_target_ip",
					Operator:      ConditionEqual,
					Value:         []string{"10.10.10.80", "10.10.10.81"},
				},
				{
					DimensionName: "pod_name",
					Operator:      ConditionRegEqual,
					Value:         []string{"kube-apiserver"},
				},
			},
			ConditionList: []string{ConditionAnd},
		},
	}

	queryMetric, err := query.ToQueryMetric(ctx, spaceUid)
	require.NoError(t, err)
	require.Len(t, queryMetric.QueryList, 1)

	qry := queryMetric.QueryList[0]
	require.True(t, qry.NativeVMInflux)
	require.Len(t, qry.NativeVMMatcherGroups, 1)
	assertNativeMatcher(t, qry.NativeVMMatcherGroups[0], "bk_target_ip", labels.MatchRegexp, `(?:10\.10\.10\.80|10\.10\.10\.81)`)
	assertNativeMatcher(t, qry.NativeVMMatcherGroups[0], "pod_name", labels.MatchRegexp, `.*(?:kube-apiserver).*`)
	assertNativeMatcher(t, qry.NativeVMMatcherGroups[0], "bcs_cluster_id", labels.MatchRegexp, `.*(?:cluster\.a).*`)
	assertNativeMatcher(t, qry.NativeVMMatchers, "bcs_cluster_id", labels.MatchRegexp, `.*(?:cluster\.a).*`)
}

func TestQueryTsToNativeVMMetricQLIgnoresUnusedReferenceWithoutSelector(t *testing.T) {
	ctx := context.Background()
	log.InitTestLogger()
	query := &QueryTs{
		QueryList: []*Query{
			{ReferenceName: "a"},
			{ReferenceName: "b"},
		},
		MetricMerge: "a",
	}

	metricQL, err := query.ToNativeVMMetricQL(ctx, &metadata.NativeVMInfluxExpand{
		MetricSelector: map[string]string{
			"a": `{__name__="cpu_detail_usage",db="system"}`,
		},
	})

	require.NoError(t, err)
	assert.Equal(t, `{__name__="cpu_detail_usage",db="system"}`, metricQL)
	assert.NotContains(t, metricQL, `{__name__="b"}`)
	assert.NotContains(t, metricQL, NativeVMMissingReferenceSelector("b"))
}

func TestQueryTsToPromExprNativeVMInfluxExpandsNestedReferences(t *testing.T) {
	ctx := context.Background()
	log.InitTestLogger()
	nameMatcher := func(metric string) []*labels.Matcher {
		matcher, err := labels.NewMatcher(labels.MatchEqual, labels.MetricName, metric)
		require.NoError(t, err)
		return []*labels.Matcher{matcher}
	}

	query := &QueryTs{
		QueryList: []*Query{
			{
				FieldName:     "value",
				ReferenceName: "a",
				AggregateMethodList: []AggregateMethod{
					{Method: "max", Dimensions: []string{"pod"}},
				},
			},
			{
				FieldName:     "value",
				ReferenceName: "b",
				AggregateMethodList: []AggregateMethod{
					{Method: "max", Dimensions: []string{"pod"}},
					{Method: "topk", VArgsList: []interface{}{1}, Position: 1},
				},
			},
		},
		MetricMerge: "a * b",
	}

	expr, err := query.ToPromExpr(
		ctx,
		map[string]string{"a": "", "b": ""},
		map[string][]*labels.Matcher{
			"a": nameMatcher("kube_pod_status_phase_value"),
			"b": nameMatcher("kube_pod_owner_value"),
		},
	)
	require.NoError(t, err)

	promQL := expr.String()
	assert.Contains(t, promQL, `__name__="kube_pod_status_phase_value"`)
	assert.Contains(t, promQL, `__name__="kube_pod_owner_value"`)
	assert.NotContains(t, promQL, "(b)")
	assert.NotContains(t, promQL, `{__name__="b"}`)
}

func reloadTestVMClusterInfo(t *testing.T, clusterName string) {
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
					"address": "http://vmselect:8481",
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
