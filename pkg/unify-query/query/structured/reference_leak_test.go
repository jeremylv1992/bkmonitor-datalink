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
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckReferenceLeakDetectsVectorSelectorName(t *testing.T) {
	expr, err := parser.ParseExpr(`max by (pod) ({__name__="metric_a"}) / max by (pod) (b)`)
	require.NoError(t, err)

	err = CheckReferenceLeak(expr, []string{"a", "b"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "b")
}

func TestCheckReferenceLeakIgnoresExpandedNativeSelector(t *testing.T) {
	expr, err := parser.ParseExpr(`max by (pod) ({__name__="metric_a"}) / max by (pod) ({__name__="metric_b"})`)
	require.NoError(t, err)

	err = CheckReferenceLeak(expr, []string{"a", "b"})
	require.NoError(t, err)
}

func TestQueryTsReferenceNames(t *testing.T) {
	query := &QueryTs{
		QueryList: []*Query{
			{ReferenceName: "a"},
			nil,
			{ReferenceName: ""},
			{ReferenceName: "b"},
		},
	}

	assert.Equal(t, []string{"a", "b"}, query.ReferenceNames())
}
