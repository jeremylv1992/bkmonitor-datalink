# Native VM 多值 Matcher 兼容修复 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 native VM Influx 兼容路径中 `eq/ne/contains/ncontains/req/nreq` 多值条件只取第一个值、以及 Prom/MetricQL 正则全量匹配导致的旧 InfluxQL 语义差异。

**Architecture:** 在 native VM 路径新增专用的 condition-to-matcher 编译函数，不复用会改写 `ConditionField` 的 `ContainsToPromReg()`。该函数按操作符把多值折叠为一个 VM label matcher：精确集合使用转义后的 regex union，包含/显式正则使用 substring union 以补偿 InfluxQL regex 与 Prom regex 的匹配差异。

**Tech Stack:** Go, Prometheus `labels.Matcher`, MetricQL/PromQL label selector, unify-query structured query。

---

## 评估结论

整体方向合理，必须修。当前 `conditionGroupsToNativeVMMatchers()` 最终调用 `labels.NewMatcher(..., cond.Value[0])`，native VM 路径会丢弃第二个及后续 value。

需要注意三类语义不能混在一起，并且 `contains/ncontains`、`req/nreq` 的单值和多值必须同构处理：

- `eq/ne` 多值表示精确集合匹配，应转成转义后的 regex union。例如 `eq ["10.10.10.80","10.10.10.81"]` 转成 `=~"(?:10\\.10\\.10\\.80|10\\.10\\.10\\.81)"`。
- `contains/ncontains` 表达“包含任一字面量”，无论单值还是多值，都应先 `regexp.QuoteMeta`，再包成 `.*(?:...).*`。例如 `contains ["10.10.10.80"]` 转成 `=~".*(?:10\\.10\\.10\\.80).*"`，`contains ["10.10.10.80","10.10.10.81"]` 转成 `=~".*(?:10\\.10\\.10\\.80|10\\.10\\.10\\.81).*"`。
- `req/nreq` 是用户显式正则，不应转义；如果目标是兼容旧 InfluxQL regex 的 substring 行为，无论单值还是多值，都需要包成 `.*(?:...).*`。例如 `req ["kube-apiserver"]` 转成 `=~".*(?:kube-apiserver).*"`，`req ["kube-apiserver","kube-scheduler"]` 转成 `=~".*(?:kube-apiserver|kube-scheduler).*"`。

建议本次按 Influx 兼容目标处理：`contains/ncontains` 和 `req/nreq` 都不要做单值优化。当前通用 `ContainsToPromReg()` 会把单值 `contains` 优化成 `eq`，native VM 兼容路径不能复用这个逻辑。

## 文件结构

- Modify: `pkg/unify-query/query/structured/native_vm_influx.go`
  - 新增 native VM 专用 matcher 编译函数。
  - 替换 `conditionGroupsToNativeVMMatchers()` 中 `ContainsToPromReg()` + `Value[0]` 的逻辑。
  - 修复 `filterConditions` 追加到 `query.NativeVMMatchers` 时只取第一个值的问题。
- Modify: `pkg/unify-query/query/structured/native_vm_influx_test.go`
  - 增加 `eq/ne/contains/ncontains/req/nreq` 多值 matcher 生成测试。
  - 增加 `contains/req` 单值与多值同构处理测试。

## Task 1: 写 matcher 编译测试

**Files:**
- Modify: `pkg/unify-query/query/structured/native_vm_influx_test.go`

- [ ] **Step 1: 添加多值条件编译测试**

新增表格测试，直接覆盖 `conditionGroupsToNativeVMMatchers()`：

```go
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
			name: "contains single value becomes literal substring regex",
			cond: ConditionField{
				DimensionName: "bk_target_ip",
				Operator:      ConditionContains,
				Value:         []string{"10.10.10.80"},
			},
			matchType: labels.MatchRegexp,
			value:     `.*(?:10\.10\.10\.80).*`,
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
			name: "req single value becomes raw substring regex",
			cond: ConditionField{
				DimensionName: "pod_name",
				Operator:      ConditionRegEqual,
				Value:         []string{"kube-apiserver"},
			},
			matchType: labels.MatchRegexp,
			value:     `.*(?:kube-apiserver).*`,
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
			name: "ncontains multi values become negative literal substring regex union",
			cond: ConditionField{
				DimensionName: "bk_target_ip",
				Operator:      ConditionNotContains,
				Value:         []string{"10.10.10.80", "10.10.10.81"},
			},
			matchType: labels.MatchNotRegexp,
			value:     `.*(?:10\.10\.10\.80|10\.10\.10\.81).*`,
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
```

- [ ] **Step 2: 添加 contains/req 单值和多值同构测试**

```go
func TestConditionGroupsToNativeVMMatchersSubstringOpsHandleSingleAndMultiValuesConsistently(t *testing.T) {
	tests := []struct {
		name  string
		cond  ConditionField
		value string
	}{
		{
			name: "contains single",
			cond: ConditionField{
				DimensionName: "label",
				Operator:      ConditionContains,
				Value:         []string{"a.b"},
			},
			value: `.*(?:a\.b).*`,
		},
		{
			name: "contains multi",
			cond: ConditionField{
				DimensionName: "label",
				Operator:      ConditionContains,
				Value:         []string{"a.b", "c.d"},
			},
			value: `.*(?:a\.b|c\.d).*`,
		},
		{
			name: "req single",
			cond: ConditionField{
				DimensionName: "label",
				Operator:      ConditionRegEqual,
				Value:         []string{"a.b"},
			},
			value: `.*(?:a.b).*`,
		},
		{
			name: "req multi",
			cond: ConditionField{
				DimensionName: "label",
				Operator:      ConditionRegEqual,
				Value:         []string{"a.b", "c.d"},
			},
			value: `.*(?:a.b|c.d).*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groups, err := conditionGroupsToNativeVMMatchers([][]ConditionField{{tt.cond}})
			require.NoError(t, err)
			require.Equal(t, labels.MatchRegexp, groups[0][0].Type)
			require.Equal(t, tt.value, groups[0][0].Value)
		})
	}
}
```

- [ ] **Step 3: 运行测试确认失败**

Run:

```bash
cd pkg/unify-query
go test ./query/structured -run 'TestConditionGroupsToNativeVMMatchers(MultiValue|SubstringOps)' -count=1
```

Expected: FAIL，`eq` 多值仍是 `MatchEqual` 且只取第一个值，`contains` 单值仍会被旧逻辑优化成 `eq`，`req` 单值仍是原始正则。

## Task 2: 实现 native VM condition 编译函数

**Files:**
- Modify: `pkg/unify-query/query/structured/native_vm_influx.go`

- [ ] **Step 1: 增加 imports**

```go
import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
)
```

- [ ] **Step 2: 新增 regex 组装 helper**

```go
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
```

- [ ] **Step 3: 新增单个 ConditionField 编译函数**

```go
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
		return labels.NewMatcher(labels.MatchNotRegexp, cond.DimensionName, nativeVMRegexUnion(cond.Value, true, true))
	case ConditionRegEqual:
		return labels.NewMatcher(labels.MatchRegexp, cond.DimensionName, nativeVMRegexUnion(cond.Value, false, true))
	case ConditionNotRegEqual:
		return labels.NewMatcher(labels.MatchNotRegexp, cond.DimensionName, nativeVMRegexUnion(cond.Value, false, true))
	default:
		return labels.NewMatcher(cond.ToPromOperator(), cond.DimensionName, cond.Value[0])
	}
}
```

- [ ] **Step 4: 替换 `conditionGroupsToNativeVMMatchers()` 内部逻辑**

把 `ContainsToPromReg()` 和 `cond.Value[0]` 替换为：

```go
matcher, err := conditionToNativeVMMatcher(field)
if err != nil {
	return nil, err
}
if matcher == nil {
	continue
}
matchers = append(matchers, matcher)
```

- [ ] **Step 5: 修复 `query.NativeVMMatchers` 追加 filter matcher**

把 `labels.MatchEqual + cond.Value[0]` 替换为同一个 helper：

```go
matcher, err := conditionToNativeVMMatcher(cond)
if err != nil {
	return nil, err
}
if matcher == nil {
	continue
}
query.NativeVMMatchers = append(query.NativeVMMatchers, matcher)
```

## Task 3: 验证 native VM selector 输出

**Files:**
- Modify: `pkg/unify-query/query/structured/native_vm_influx_test.go`

- [ ] **Step 1: 添加端到端 MetricQL 片段测试**

构造 `QueryTs`，where 中传 `eq` 多值、`contains` 多值、`req` 单值，断言最终 `ToNativeVMMetricQL()` 含有预期 label matcher。

```go
require.Contains(t, metricQL, `bk_target_ip=~"(?:10\\.10\\.10\\.80|10\\.10\\.10\\.81)"`)
require.Contains(t, metricQL, `pod_name=~".*(?:kube-apiserver).*"`)
```

- [ ] **Step 2: 运行目标测试**

Run:

```bash
cd pkg/unify-query
go test ./query/structured -run 'NativeVM|ConditionGroupsToNativeVMMatchers' -count=1
```

Expected: PASS。

## Task 4: 影子对比验证

**Files:**
- No code change

- [ ] **Step 1: 本地目标测试**

Run:

```bash
cd pkg/unify-query
go test ./query/structured ./metadata -run 'NativeVM|ConditionGroupsToNativeVMMatchers' -count=1
```

Expected: PASS。

- [ ] **Step 2: 部署测试环境后抽样重放**

重点抽样三类请求：

- `eq` 多值 IP/实例过滤，预期 native VM 查询不再只命中第一个值。
- `contains` 单值和多值字面量过滤，预期都生成 `.*(?:literal).*` 或 `.*(?:literal|literal).*`。
- `req pod_name kube-apiserver`，预期 native VM 和旧 Influx 路径都能命中 `kube-apiserver-master-*`。

- [ ] **Step 3: 2 小时 shadow 对比**

预期：

- `K8S/Prometheus 指标写入与查询语义差异` 中由 `req` substring 造成的 series_count mismatch 明显下降。
- 不应新增 `status_code_mismatch`。
- 若仍有差异，按请求体中的 `op/value` 分类，确认是否属于存储保留时间、边界窗口或真实数据差异。

## 风险与回滚

- `req/nreq` 包 `.*` 是语义变更：从 Prom regex full-match 调整为 InfluxQL regex substring-match。该行为符合 Influx 兼容目标，但会让未显式加锚点的正则匹配范围变宽。
- 对需要全量匹配的用户，仍可使用 `^...$`。例如 `req ["^kube-apiserver$"]` 生成 `.*(?:^kube-apiserver$).*` 后仍只匹配完整值。
- 回滚方式：恢复 `conditionToNativeVMMatcher()` 中 `ConditionRegEqual/ConditionNotRegEqual` 不包 substring，仅保留多值 union。
