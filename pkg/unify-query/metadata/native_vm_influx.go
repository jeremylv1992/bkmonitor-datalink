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
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
)

type NativeVMInfluxExpand struct {
	MetricAliasMapping map[string]string
	LabelsMatcher      map[string][]*labels.Matcher
	StorageID          string
	ClusterName        string
	Address            string
	APIPrefix          string
	SelectUsername     string
	SelectPassword     string
}

func NativeVMInfluxMetricName(measurement, field, separator string, skipSingleField bool) string {
	if skipSingleField && field == StaticField {
		return measurement
	}
	return measurement + separator + field
}

func NativeVMInfluxLabelMatchers(query *Query) ([]*labels.Matcher, error) {
	if query == nil {
		return nil, fmt.Errorf("native vm influx query is nil")
	}
	if query.DB == "" {
		return nil, fmt.Errorf("native vm influx db is empty")
	}
	if query.Measurement == "" {
		return nil, fmt.Errorf("native vm influx measurement is empty")
	}
	if query.Field == "" {
		return nil, fmt.Errorf("native vm influx field is empty")
	}

	dbLabel := query.NativeVMDBLabel
	if dbLabel == "" {
		dbLabel = "db"
	}
	separator := query.NativeVMMeasurementFieldSeparator
	if separator == "" {
		separator = "_"
	}

	metricName := NativeVMInfluxMetricName(
		query.Measurement, query.Field, separator, query.NativeVMSkipSingleField,
	)
	matchers := make([]*labels.Matcher, 0, len(query.NativeVMMatchers)+2)

	metricMatcher, err := labels.NewMatcher(labels.MatchEqual, labels.MetricName, metricName)
	if err != nil {
		return nil, err
	}
	matchers = append(matchers, metricMatcher)

	dbMatcher, err := labels.NewMatcher(labels.MatchEqual, dbLabel, query.DB)
	if err != nil {
		return nil, err
	}
	matchers = append(matchers, dbMatcher)

	for _, matcher := range query.NativeVMMatchers {
		if matcher == nil {
			continue
		}
		if matcher.Name == labels.MetricName || matcher.Name == dbLabel {
			return nil, fmt.Errorf("native vm influx matcher conflicts with reserved label: %s", matcher.Name)
		}
		matchers = append(matchers, matcher)
	}
	return matchers, nil
}

func (qRef QueryReference) CheckNativeVMInfluxQuery(ctx context.Context) (bool, *NativeVMInfluxExpand, error) {
	expand := &NativeVMInfluxExpand{
		MetricAliasMapping: make(map[string]string),
		LabelsMatcher:      make(map[string][]*labels.Matcher),
	}

	nativeNum := 0
	nonNativeNum := 0

	for referenceName, queries := range qRef {
		if queries == nil || len(queries.QueryList) == 0 {
			continue
		}

		refNative := false
		for _, query := range queries.QueryList {
			if query == nil {
				continue
			}
			if !query.NativeVMInflux {
				nonNativeNum++
				continue
			}

			nativeNum++
			refNative = true
			if query.NativeVMUnsupportedOr {
				return false, expand, fmt.Errorf(
					"native vm influx query does not support or condition: %s", referenceName,
				)
			}
			if query.StorageID == "" {
				return false, expand, fmt.Errorf("native vm influx storage id is empty: %s", referenceName)
			}
			if query.NativeVMAddress == "" {
				return false, expand, fmt.Errorf("native vm influx select address is empty: %s", referenceName)
			}
			if query.NativeVMAPIPrefix == "" {
				return false, expand, fmt.Errorf("native vm influx select api prefix is empty: %s", referenceName)
			}
			if expand.StorageID == "" {
				expand.StorageID = query.StorageID
				expand.ClusterName = query.ClusterName
				expand.Address = query.NativeVMAddress
				expand.APIPrefix = query.NativeVMAPIPrefix
				expand.SelectUsername = query.NativeVMSelectUsername
				expand.SelectPassword = query.NativeVMSelectPassword
			} else if expand.Address != query.NativeVMAddress ||
				expand.APIPrefix != query.NativeVMAPIPrefix ||
				expand.SelectUsername != query.NativeVMSelectUsername ||
				expand.SelectPassword != query.NativeVMSelectPassword {
				return false, expand, fmt.Errorf(
					"native vm influx query does not support multiple backends: %s%s user=%s, %s%s user=%s",
					expand.Address, expand.APIPrefix, expand.SelectUsername,
					query.NativeVMAddress, query.NativeVMAPIPrefix, query.NativeVMSelectUsername,
				)
			}

			matchers, err := NativeVMInfluxLabelMatchers(query)
			if err != nil {
				return false, expand, err
			}
			expand.MetricAliasMapping[referenceName] = ""
			expand.LabelsMatcher[referenceName] = matchers
		}

		if refNative && len(queries.QueryList) > 1 {
			return false, expand, fmt.Errorf(
				"native vm influx query does not support many table id: %s", referenceName,
			)
		}
	}

	if nativeNum == 0 {
		return false, expand, nil
	}
	if nonNativeNum > 0 {
		return false, expand, fmt.Errorf("mixed native vm influx and non-native query is not supported")
	}

	return true, expand, nil
}
