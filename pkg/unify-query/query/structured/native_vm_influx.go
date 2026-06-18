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
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
)

const (
	nativeVMMissingReferenceMetricName = "__bk_unify_query_missing_reference__"
	nativeVMMissingReferenceLabel      = "__bk_missing_reference__"
)

func NativeVMInfluxMetricName(measurement, field, separator string, skipSingleField bool) string {
	return metadata.NativeVMInfluxMetricName(measurement, field, separator, skipSingleField)
}

func NativeVMMissingReferenceSelector(referenceName string) string {
	return fmt.Sprintf(
		`{__name__=%q,%s=%q}`,
		nativeVMMissingReferenceMetricName,
		nativeVMMissingReferenceLabel,
		referenceName,
	)
}

func NativeVMInfluxLabelMatchers(query *metadata.Query) ([]*labels.Matcher, error) {
	return metadata.NativeVMInfluxLabelMatchers(query)
}

func nativeVMRegexUnion(values []string, quote bool, substring bool) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		if quote {
			value = regexp.QuoteMeta(value)
		}
		parts = append(parts, value)
	}

	union := "(?:" + strings.Join(parts, "|") + ")"
	if substring {
		return ".*" + union + ".*"
	}
	return union
}

func conditionToNativeVMMatcher(cond ConditionField) (*labels.Matcher, error) {
	if len(cond.Value) == 0 {
		return nil, nil
	}

	switch cond.Operator {
	case ConditionEqual:
		if len(cond.Value) == 1 {
			return labels.NewMatcher(labels.MatchEqual, cond.DimensionName, cond.Value[0])
		}
		return labels.NewMatcher(labels.MatchRegexp, cond.DimensionName, nativeVMRegexUnion(cond.Value, true, false))
	case ConditionNotEqual:
		if len(cond.Value) == 1 {
			return labels.NewMatcher(labels.MatchNotEqual, cond.DimensionName, cond.Value[0])
		}
		return labels.NewMatcher(labels.MatchNotRegexp, cond.DimensionName, nativeVMRegexUnion(cond.Value, true, false))
	case ConditionContains:
		return labels.NewMatcher(labels.MatchRegexp, cond.DimensionName, nativeVMRegexUnion(cond.Value, true, true))
	case ConditionNotContains:
		if len(cond.Value) == 1 && cond.Value[0] == "" {
			return labels.NewMatcher(labels.MatchNotEqual, cond.DimensionName, "")
		}
		return labels.NewMatcher(labels.MatchNotRegexp, cond.DimensionName, nativeVMRegexUnion(cond.Value, true, true))
	case ConditionRegEqual:
		return labels.NewMatcher(labels.MatchRegexp, cond.DimensionName, nativeVMRegexUnion(cond.Value, false, true))
	case ConditionNotRegEqual:
		return labels.NewMatcher(labels.MatchNotRegexp, cond.DimensionName, nativeVMRegexUnion(cond.Value, false, true))
	default:
		return labels.NewMatcher(cond.ToPromOperator(), cond.DimensionName, cond.Value[0])
	}
}

func conditionGroupsToNativeVMMatchers(groups [][]ConditionField) ([][]*labels.Matcher, error) {
	if len(groups) == 0 {
		return nil, nil
	}

	result := make([][]*labels.Matcher, 0, len(groups))
	for _, group := range groups {
		matchers := make([]*labels.Matcher, 0, len(group))
		for _, field := range group {
			matcher, err := conditionToNativeVMMatcher(field)
			if err != nil {
				return nil, err
			}
			if matcher == nil {
				continue
			}
			matchers = append(matchers, matcher)
		}
		if len(matchers) > 0 {
			result = append(result, matchers)
		}
	}
	return result, nil
}

func combineNativeVMMatcherGroups(groupSets ...[][]*labels.Matcher) ([][]*labels.Matcher, error) {
	result := [][]*labels.Matcher{{}}
	for _, groups := range groupSets {
		if len(groups) == 0 {
			continue
		}
		next := make([][]*labels.Matcher, 0, len(result)*len(groups))
		for _, base := range result {
			for _, group := range groups {
				if len(next) >= metadata.NativeVMMaxMatcherGroups {
					return nil, fmt.Errorf(
						"native vm influx matcher group count exceeds limit: %d",
						metadata.NativeVMMaxMatcherGroups,
					)
				}
				combined := make([]*labels.Matcher, 0, len(base)+len(group))
				combined = append(combined, base...)
				combined = append(combined, group...)
				next = append(next, combined)
			}
		}
		result = next
	}
	return result, nil
}

func (q *QueryTs) ToNativeVMMetricQL(ctx context.Context, expand *metadata.NativeVMInfluxExpand) (string, error) {
	if q == nil {
		return "", fmt.Errorf("native vm query ts is nil")
	}
	if expand == nil {
		return "", fmt.Errorf("native vm expand is nil")
	}
	usedRefs, err := q.metricMergeReferenceSet()
	if err != nil {
		return "", err
	}

	referenceNameMetric := make(map[string]string, len(q.QueryList))
	placeholderSelectors := make(map[string]string, len(q.QueryList))
	for idx, query := range q.QueryList {
		if query == nil || query.ReferenceName == "" {
			continue
		}
		if _, ok := usedRefs[query.ReferenceName]; !ok {
			continue
		}
		selector, ok := expand.MetricSelector[query.ReferenceName]
		if !ok || selector == "" {
			return "", fmt.Errorf("native vm selector not found: %s", query.ReferenceName)
		}
		placeholder := fmt.Sprintf("__bk_native_vm_ref_%d__", idx)
		referenceNameMetric[query.ReferenceName] = placeholder
		placeholderSelectors[placeholder] = selector
	}

	expr, err := q.ToPromExpr(ctx, referenceNameMetric, nil)
	if err != nil {
		return "", err
	}
	if err = CheckReferenceLeak(expr, q.ReferenceNames()); err != nil {
		return "", err
	}

	metricQL := expr.String()
	for placeholder, selector := range placeholderSelectors {
		metricQL = strings.ReplaceAll(metricQL, placeholder, selector)
	}
	for placeholder := range placeholderSelectors {
		if strings.Contains(metricQL, placeholder) {
			return "", fmt.Errorf("native vm placeholder leaked: %s", placeholder)
		}
	}
	return metricQL, nil
}

func (q *QueryTs) MissingNativeVMMetricQLSelectors(expand *metadata.NativeVMInfluxExpand) ([]string, error) {
	if q == nil {
		return nil, fmt.Errorf("native vm query ts is nil")
	}
	if expand == nil {
		return nil, fmt.Errorf("native vm expand is nil")
	}
	usedRefs, err := q.metricMergeReferenceSet()
	if err != nil {
		return nil, err
	}

	missingSet := make(map[string]struct{})
	for _, query := range q.QueryList {
		if query == nil || query.ReferenceName == "" {
			continue
		}
		if _, ok := usedRefs[query.ReferenceName]; !ok {
			continue
		}
		if selector := expand.MetricSelector[query.ReferenceName]; selector == "" {
			missingSet[query.ReferenceName] = struct{}{}
		}
	}

	missing := make([]string, 0, len(missingSet))
	for referenceName := range missingSet {
		missing = append(missing, referenceName)
	}
	sort.Strings(missing)
	return missing, nil
}

func (q *QueryTs) metricMergeReferenceSet() (map[string]struct{}, error) {
	if q == nil {
		return nil, fmt.Errorf("native vm query ts is nil")
	}
	if q.MetricMerge == "" {
		return nil, fmt.Errorf("metric merge is empty")
	}
	expr, err := parser.ParseExpr(q.MetricMerge)
	if err != nil {
		return nil, err
	}

	refs := make(map[string]struct{})
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if vector, ok := node.(*parser.VectorSelector); ok && vector.Name != "" {
			refs[vector.Name] = struct{}{}
		}
		return nil
	})
	return refs, nil
}
