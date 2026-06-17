// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package metadata

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckNativeVMInfluxQuery(t *testing.T) {
	bizMatcher, err := labels.NewMatcher(labels.MatchEqual, "bk_biz_id", "2")
	require.NoError(t, err)

	queryRef := QueryReference{
		"a": &QueryMetric{
			QueryList: QueryList{
				&Query{
					NativeVMInflux:                    true,
					StorageID:                         "native-vm-storage",
					ClusterName:                       "cluster-a",
					DB:                                "system",
					Measurement:                       "cpu_detail",
					Field:                             "usage",
					NativeVMAddress:                   "http://vmselect:8481",
					NativeVMAPIPrefix:                 "/select/0/prometheus/api/v1",
					NativeVMSelectUsername:            "query",
					NativeVMSelectPassword:            "secret",
					NativeVMDBLabel:                   "db",
					NativeVMMeasurementFieldSeparator: "_",
					NativeVMMatchers:                  []*labels.Matcher{bizMatcher},
				},
			},
		},
	}

	ok, expand, err := queryRef.CheckNativeVMInfluxQuery(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "native-vm-storage", expand.StorageID)
	assert.Equal(t, "cluster-a", expand.ClusterName)
	assert.Equal(t, "http://vmselect:8481", expand.Address)
	assert.Equal(t, "/select/0/prometheus/api/v1", expand.APIPrefix)
	assert.Equal(t, "query", expand.SelectUsername)
	assert.Equal(t, "secret", expand.SelectPassword)
	assert.Equal(t, "", expand.MetricAliasMapping["a"])
	assertNativeVMMatcher(t, expand.LabelsMatcher["a"], labels.MetricName, labels.MatchEqual, "cpu_detail_usage")
	assertNativeVMMatcher(t, expand.LabelsMatcher["a"], "db", labels.MatchEqual, "system")
	assertNativeVMMatcher(t, expand.LabelsMatcher["a"], "bk_biz_id", labels.MatchEqual, "2")
	assert.Equal(t, `{__name__="cpu_detail_usage",db="system",bk_biz_id="2"}`, expand.MetricSelector["a"])
}

func TestNativeVMInfluxSelectorBuildsMetricQLOrGroups(t *testing.T) {
	biz2, err := labels.NewMatcher(labels.MatchEqual, "bk_biz_id", "2")
	require.NoError(t, err)
	biz3, err := labels.NewMatcher(labels.MatchEqual, "bk_biz_id", "3")
	require.NoError(t, err)
	pod, err := labels.NewMatcher(labels.MatchRegexp, "pod_name", `api-\d+`)
	require.NoError(t, err)

	selector, err := NativeVMInfluxSelector(&Query{
		DB:                                "system",
		Measurement:                       "cpu_detail",
		Field:                             "usage",
		NativeVMDBLabel:                   "db",
		NativeVMMeasurementFieldSeparator: "_",
		NativeVMMatcherGroups: [][]*labels.Matcher{
			{biz2, pod},
			{biz3},
		},
	})
	require.NoError(t, err)

	assert.Equal(
		t,
		`{__name__="cpu_detail_usage",db="system",bk_biz_id="2",pod_name=~"api-\\d+" or __name__="cpu_detail_usage",db="system",bk_biz_id="3"}`,
		selector,
	)
}

func TestNativeVMInfluxSelectorEscapesLabelValues(t *testing.T) {
	matcher, err := labels.NewMatcher(labels.MatchEqual, "pod", "api\"\\n")
	require.NoError(t, err)

	selector, err := NativeVMInfluxSelector(&Query{
		DB:                    "system",
		Measurement:           "cpu_detail",
		Field:                 "usage",
		NativeVMMatcherGroups: [][]*labels.Matcher{{matcher}},
	})
	require.NoError(t, err)

	assert.Equal(t, `{__name__="cpu_detail_usage",db="system",pod="api\"\\n"}`, selector)
}

func TestNativeVMInfluxSelectorRejectsReservedLabelConflict(t *testing.T) {
	matcher, err := labels.NewMatcher(labels.MatchEqual, "db", "other")
	require.NoError(t, err)

	_, err = NativeVMInfluxSelector(&Query{
		DB:                    "system",
		Measurement:           "cpu_detail",
		Field:                 "usage",
		NativeVMDBLabel:       "db",
		NativeVMMatcherGroups: [][]*labels.Matcher{{matcher}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved label")
}

func TestCheckNativeVMInfluxQueryRejectsMixedStorage(t *testing.T) {
	queryRef := QueryReference{
		"a": &QueryMetric{
			QueryList: QueryList{
				&Query{
					NativeVMInflux:    true,
					StorageID:         "native-vm-storage",
					DB:                "system",
					Measurement:       "cpu_detail",
					Field:             "usage",
					NativeVMAddress:   "http://vmselect:8481",
					NativeVMAPIPrefix: "/select/0/prometheus/api/v1",
				},
			},
		},
		"b": &QueryMetric{
			QueryList: QueryList{
				&Query{
					StorageID:   "influx-storage",
					DB:          "system",
					Measurement: "mem",
					Field:       "usage",
				},
			},
		},
	}

	ok, _, err := queryRef.CheckNativeVMInfluxQuery(context.Background())
	assert.False(t, ok)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mixed native vm influx")
}

func TestCheckNativeVMInfluxQueryAllowsDifferentStorageWithSameBackend(t *testing.T) {
	queryRef := QueryReference{
		"a": &QueryMetric{
			QueryList: QueryList{
				&Query{
					NativeVMInflux:    true,
					StorageID:         "storage-a",
					ClusterName:       "cluster-a",
					DB:                "system",
					Measurement:       "cpu_detail",
					Field:             "usage",
					NativeVMAddress:   "http://vmselect:8481",
					NativeVMAPIPrefix: "/select/0/prometheus/api/v1",
				},
			},
		},
		"b": &QueryMetric{
			QueryList: QueryList{
				&Query{
					NativeVMInflux:    true,
					StorageID:         "storage-b",
					ClusterName:       "cluster-b",
					DB:                "system",
					Measurement:       "mem",
					Field:             "usage",
					NativeVMAddress:   "http://vmselect:8481",
					NativeVMAPIPrefix: "/select/0/prometheus/api/v1",
				},
			},
		},
	}

	ok, expand, err := queryRef.CheckNativeVMInfluxQuery(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "http://vmselect:8481", expand.Address)
}

func TestCheckNativeVMInfluxQueryRejectsDifferentBackend(t *testing.T) {
	queryRef := QueryReference{
		"a": &QueryMetric{
			QueryList: QueryList{
				&Query{
					NativeVMInflux:    true,
					StorageID:         "storage-a",
					ClusterName:       "cluster-a",
					DB:                "system",
					Measurement:       "cpu_detail",
					Field:             "usage",
					NativeVMAddress:   "http://vmselect-a:8481",
					NativeVMAPIPrefix: "/select/0/prometheus/api/v1",
				},
			},
		},
		"b": &QueryMetric{
			QueryList: QueryList{
				&Query{
					NativeVMInflux:    true,
					StorageID:         "storage-b",
					ClusterName:       "cluster-b",
					DB:                "system",
					Measurement:       "mem",
					Field:             "usage",
					NativeVMAddress:   "http://vmselect-b:8481",
					NativeVMAPIPrefix: "/select/0/prometheus/api/v1",
				},
			},
		},
	}

	ok, _, err := queryRef.CheckNativeVMInfluxQuery(context.Background())
	assert.False(t, ok)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple backends")
}

func TestCheckNativeVMInfluxQueryAllowsMetricQLOr(t *testing.T) {
	biz2, err := labels.NewMatcher(labels.MatchEqual, "bk_biz_id", "2")
	require.NoError(t, err)
	biz3, err := labels.NewMatcher(labels.MatchEqual, "bk_biz_id", "3")
	require.NoError(t, err)

	queryRef := QueryReference{
		"a": &QueryMetric{
			QueryList: QueryList{
				&Query{
					NativeVMInflux:    true,
					StorageID:         "native-vm-storage",
					DB:                "system",
					Measurement:       "cpu_detail",
					Field:             "usage",
					NativeVMAddress:   "http://vmselect:8481",
					NativeVMAPIPrefix: "/select/0/prometheus/api/v1",
					NativeVMMatcherGroups: [][]*labels.Matcher{
						{biz2},
						{biz3},
					},
				},
			},
		},
	}

	ok, expand, err := queryRef.CheckNativeVMInfluxQuery(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(
		t,
		`{__name__="cpu_detail_usage",db="system",bk_biz_id="2" or __name__="cpu_detail_usage",db="system",bk_biz_id="3"}`,
		expand.MetricSelector["a"],
	)
	assert.Empty(t, expand.LabelsMatcher["a"])
}

func assertNativeVMMatcher(t *testing.T, matchers []*labels.Matcher, name string, matchType labels.MatchType, value string) {
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
