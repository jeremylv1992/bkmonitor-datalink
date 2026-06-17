# Native VM Missing Reference Status Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 native VM 路径在部分 reference 元数据缺失时返回 HTTP 400 的兼容性问题，保持旧 InfluxDB 路径的 HTTP 200 + body.status 业务错误形态。

**Architecture:** 在进入 native VM 直查前，先识别“表达式实际使用的 reference 是否缺少 native VM selector”。如果缺失来自元数据解析阶段已经写入的业务 status，则不再组装 MetricQL、不请求 VM backend，直接返回空 series + 原业务 status；如果没有业务 status，则继续返回硬错误，避免吞掉真实内部缺陷。同时让 MetricQL 组装只要求表达式实际使用的 reference 有 selector，避免未使用 query_list 误伤。

**Tech Stack:** Go、Prometheus parser、unify-query structured query、metadata status、native VM Influx compatibility。

---

### Task 1: 增加缺失 reference 的 handler 级回归测试

**Files:**
- Modify: `pkg/unify-query/service/http/handler_native_vm_influx_test.go`

- [ ] **Step 1: 写一个当前会失败的回归测试**

在 `TestQueryTsNativeVMInfluxExpandsAllReferencesInThreeReferenceExpr` 后增加测试。只 mock `a` 对应的表，故意不 mock `b/c` 对应字段；表达式继续使用 `b/c`。

```go
func TestQueryTsNativeVMInfluxReturnsBusinessStatusForMissingReferenceSelector(t *testing.T) {
	ctx := context.Background()
	log.InitTestLogger()
	mock.SetOfflineDataArchiveMetadata(&emptyArchiveMetadata{})

	called := false
	vmselect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
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
	assert.False(t, called, "missing reference should not request native VM backend")

	resp, ok := res.(*PromData)
	require.True(t, ok)
	assert.Empty(t, resp.Tables)
	require.NotNil(t, resp.Status)
	assert.Equal(t, metadata.SpaceTableIDFieldIsNotExists, resp.Status.Code)
	assert.Contains(t, resp.Status.Message, "kube_node_status_allocatable_cpu_cores")
}
```

- [ ] **Step 2: 运行测试并确认失败**

Run:

```bash
cd pkg/unify-query
go test ./service/http -run TestQueryTsNativeVMInfluxReturnsBusinessStatusForMissingReferenceSelector -count=1
```

Expected: 当前实现失败，错误包含 `native vm selector not found: b`。

---

### Task 2: 让 native MetricQL 只要求表达式实际使用的 reference

**Files:**
- Modify: `pkg/unify-query/query/structured/native_vm_influx.go`
- Test: `pkg/unify-query/query/structured/native_vm_influx_test.go`

- [ ] **Step 1: 增加结构化层单测**

增加一个单测覆盖“query_list 里有未使用的 reference，但 metric_merge 没用它时，不应该因为 selector 缺失失败”。

```go
func TestQueryTsToNativeVMMetricQLIgnoresUnusedReferenceWithoutSelector(t *testing.T) {
	ctx := context.Background()
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
	assert.NotContains(t, metricQL, "b")
}
```

- [ ] **Step 2: 实现表达式 reference 提取 helper**

在 `pkg/unify-query/query/structured/native_vm_influx.go` 增加 helper，使用 Prometheus parser 解析原始 `MetricMerge`。

```go
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
```

同时补充 import：

```go
import "github.com/prometheus/prometheus/promql/parser"
```

- [ ] **Step 3: 修改 `ToNativeVMMetricQL` 的 selector 检查范围**

在 `ToNativeVMMetricQL` 开头获取 `usedRefs`，遍历 `q.QueryList` 时跳过未使用 reference。

```go
usedRefs, err := q.metricMergeReferenceSet()
if err != nil {
	return "", err
}

for idx, query := range q.QueryList {
	if query == nil || query.ReferenceName == "" {
		continue
	}
	if _, used := usedRefs[query.ReferenceName]; !used {
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
```

- [ ] **Step 4: 运行结构化层测试**

Run:

```bash
cd pkg/unify-query
go test ./query/structured -run 'TestQueryTsToNativeVMMetricQL|TestCheckReferenceLeak|TestQueryTsReferenceNames' -count=1
```

Expected: PASS。

---

### Task 3: 在 handler native 分支保留旧协议错误形态

**Files:**
- Modify: `pkg/unify-query/query/structured/native_vm_influx.go`
- Modify: `pkg/unify-query/service/http/handler.go`
- Test: `pkg/unify-query/service/http/handler_native_vm_influx_test.go`

- [ ] **Step 1: 增加缺失 selector 列表 helper**

在 `pkg/unify-query/query/structured/native_vm_influx.go` 增加导出方法，供 handler 在真正组装 MetricQL 前判断。

```go
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

	missing := make([]string, 0)
	for _, query := range q.QueryList {
		if query == nil || query.ReferenceName == "" {
			continue
		}
		if _, used := usedRefs[query.ReferenceName]; !used {
			continue
		}
		if selector := expand.MetricSelector[query.ReferenceName]; selector == "" {
			missing = append(missing, query.ReferenceName)
		}
	}
	sort.Strings(missing)
	return missing, nil
}
```

同时补充 import：

```go
import "sort"
```

- [ ] **Step 2: 增加空响应构造 helper**

在 `pkg/unify-query/service/http/handler.go` 中增加小 helper，保持返回结构稳定。

```go
func newPromDataWithStatus(columns []string, status *metadata.Status) *PromData {
	resp := NewPromData(columns)
	resp.Tables = make([]*TablesItem, 0)
	resp.Status = status
	return resp
}
```

- [ ] **Step 3: 在 nativeOK 分支设置实例前做兼容判断**

在 `queryTs` 中，`CheckNativeVMInfluxQuery` 返回后、`metadata.SetExpand(ctx, nativeVMExpand)` 之前增加判断。注意必须先缓存 `metadata.GetStatus(ctx)`，避免后续 `SetExpand` 覆盖 `MessageKey`。

```go
	statusBeforeNativeExpand := metadata.GetStatus(ctx)

	nativeOK, nativeVMExpand, err := queryReference.CheckNativeVMInfluxQuery(ctx)
	if err != nil {
		log.Errorf(ctx, fmt.Sprintf("check native vm influx query: %s", err.Error()))
		return nil, err
	}
	if nativeOK {
		missingSelectors, err := query.MissingNativeVMMetricQLSelectors(nativeVMExpand)
		if err != nil {
			return nil, err
		}
		if len(missingSelectors) > 0 && statusBeforeNativeExpand != nil {
			trace.InsertStringIntoSpan("native-vm-missing-selectors", strings.Join(missingSelectors, ","), span)
			return newPromDataWithStatus(query.ResultColumns, statusBeforeNativeExpand), nil
		}
		if len(missingSelectors) > 0 {
			return nil, fmt.Errorf("native vm selector not found: %s", strings.Join(missingSelectors, ","))
		}

		referenceNameMetric = nativeVMExpand.MetricAliasMapping
```

同时补充 import：

```go
import "strings"
```

- [ ] **Step 4: 运行 handler 回归测试**

Run:

```bash
cd pkg/unify-query
go test ./service/http -run 'TestQueryTsNativeVMInfluxReturnsBusinessStatusForMissingReferenceSelector|TestQueryTsNativeVMInfluxExpandsAllReferences' -count=1
```

Expected: PASS，缺失 reference 用例返回 `*PromData`，`Status.Code=SPACE_TABLE_ID_FIELD_IS_NOT_EXISTS`，不会请求测试 VM server。

---

### Task 4: 验证已有 native VM 行为没有退化

**Files:**
- Test only

- [ ] **Step 1: 跑 native VM 相关单测**

Run:

```bash
cd pkg/unify-query
go test ./metadata ./query/structured ./service/http -run 'NativeVM|ReferenceLeak' -count=1
```

Expected: PASS。

- [ ] **Step 2: 跑 service/http 的 query_ts 相关测试**

Run:

```bash
cd pkg/unify-query
go test ./service/http -run 'TestQueryTs|TestHandlerQueryTs' -count=1
```

Expected: PASS。如果出现与本改动无关的历史失败，记录失败测试名和错误，不扩大修复范围。

- [ ] **Step 3: 检查 shadow compare 风险点**

确认以下三类线上不一致的错误形态会变成旧协议：

- `a/b-(c-1)/c` 缺 `kube_node_status_allocatable_cpu_cores`
- `a/b*100` 缺 `container_spec_memory_limit_bytes`
- `a * b` 缺 `kube_pod_owner`

预期结果：

- HTTP status: 200
- body.series: `[]`
- body.status.code: `SPACE_TABLE_ID_FIELD_IS_NOT_EXISTS`
- 不再出现 `native vm selector not found: b`

---

### Task 5: 代码审查关注点

**Files:**
- Review: `pkg/unify-query/query/structured/native_vm_influx.go`
- Review: `pkg/unify-query/service/http/handler.go`

- [ ] **Step 1: 确认没有吞掉真实 native VM 内部错误**

检查逻辑必须满足：

```go
if len(missingSelectors) > 0 && statusBeforeNativeExpand != nil {
	return newPromDataWithStatus(query.ResultColumns, statusBeforeNativeExpand), nil
}
if len(missingSelectors) > 0 {
	return nil, fmt.Errorf("native vm selector not found: %s", strings.Join(missingSelectors, ","))
}
```

这保证只有元数据阶段已产生业务 status 的缺失 reference 才走兼容返回。

- [ ] **Step 2: 确认 `SetExpand` 不再覆盖本次要返回的 status**

兼容分支必须发生在：

```go
metadata.SetExpand(ctx, nativeVMExpand)
```

之前。

- [ ] **Step 3: 确认未使用 reference 不再影响 native MetricQL 组装**

`ToNativeVMMetricQL` 必须只对 `metric_merge` 中实际出现的 reference 查 selector，防止请求带多余 query_list 时误报 400。

---

## Self-Review

**Spec coverage:** 方案覆盖了 status_code_mismatch 根因：缺失 reference selector 在 native VM 路径被硬错误处理；修复后按旧协议返回 body.status。也覆盖了未使用 reference 误伤问题。

**Placeholder scan:** 无 TBD/TODO/泛化占位。每个步骤包含明确文件、代码片段、命令和期望。

**Type consistency:** 使用现有类型 `structured.QueryTs`、`metadata.NativeVMInfluxExpand`、`metadata.Status`、`PromData`、`TablesItem`；新增方法签名在 handler 调用处一致。
