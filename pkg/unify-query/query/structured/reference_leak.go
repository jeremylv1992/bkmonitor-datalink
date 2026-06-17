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
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/promql/parser"
)

func CheckReferenceLeak(expr parser.Expr, references []string) error {
	if expr == nil || len(references) == 0 {
		return nil
	}

	refSet := make(map[string]struct{}, len(references))
	for _, ref := range references {
		if ref != "" {
			refSet[ref] = struct{}{}
		}
	}
	if len(refSet) == 0 {
		return nil
	}

	leaked := make(map[string]struct{})
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		vector, ok := node.(*parser.VectorSelector)
		if !ok {
			return nil
		}
		if _, ok = refSet[vector.Name]; ok {
			leaked[vector.Name] = struct{}{}
		}
		return nil
	})

	if len(leaked) == 0 {
		return nil
	}

	names := make([]string, 0, len(leaked))
	for name := range leaked {
		names = append(names, name)
	}
	sort.Strings(names)
	return fmt.Errorf("native vm influx metricql contains unresolved reference selector: %s", strings.Join(names, ","))
}

func (q *QueryTs) ReferenceNames() []string {
	if q == nil || len(q.QueryList) == 0 {
		return nil
	}

	references := make([]string, 0, len(q.QueryList))
	for _, query := range q.QueryList {
		if query == nil || query.ReferenceName == "" {
			continue
		}
		references = append(references, query.ReferenceName)
	}
	return references
}
