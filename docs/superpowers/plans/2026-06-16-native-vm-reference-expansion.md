# Native VM Reference 展开修复实施计划

> **给 agentic worker 的要求：** 执行本计划时需要使用 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans`。任务使用 checkbox（`- [ ]`）跟踪。

**目标：** 确保 Native VM Influx 兼容查询在发送到 VictoriaMetrics 前，`metric_merge` 中的所有 `query_list.reference_name` 都已经展开为真实 VM MetricQL selector。

**架构：** 保留现有 `QueryTs.ToPromExpr` / `HandleExpr` 的 PromQL AST 转换流程。新增 Native VM 专用回归测试和最终 AST 兜底校验：如果最终 MetricQL 仍包含裸 `reference_name` selector，直接报错，不请求 VM。普通 InfluxDB 路径不启用该校验，因为 InfluxDB 路径里的 `a` / `b` 是本地 PromQL engine 到 InfluxQL builder 的路由 key。

**技术栈：** Go、Prometheus `parser.Expr`、unify-query 现有 metadata/router mock、`httptest`。

---

## 文件结构

- 修改 `pkg/unify-query/service/http/handler_native_vm_influx_test.go`
  - 新增 Native VM handler 级回归测试，捕获最终发给 vmselect 的 HTTP query。
  - 覆盖 `a/b*100`、`a/b-(c-1)/c`、`a * topk(...)` 三类线上风险形态。
- 修改 `pkg/unify-query/query/structured/native_vm_influx_test.go`
  - 新增结构化层测试，直接验证 `QueryTs.ToPromExpr` 能递归展开嵌套 reference。
- 新增 `pkg/unify-query/query/structured/reference_leak.go`
  - 提供 `CheckReferenceLeak` 和 `QueryTs.ReferenceNames`。
- 新增 `pkg/unify-query/query/structured/reference_leak_test.go`
  - 单测覆盖裸 reference selector 检测和正常展开 selector 不误报。
- 修改 `pkg/unify-query/service/http/handler.go`
  - 在 Native VM 路径 `ToPromExpr` 后、请求 VM 前执行 `CheckReferenceLeak`。

---

### Task 1: 补充 handler 级多 reference 回归测试

**文件：**
- 修改：`pkg/unify-query/service/http/handler_native_vm_influx_test.go`

- [x] **Step 1: 增加多表 mock helper**

新增 `nativeVMInfluxTable` 和 `mockNativeVMInfluxTables`，解决 `mock.SetSpaceAndProxyMockData` 每次会重置 router mock 的问题。helper 最终用合并后的 `ir.ProxyInfo` 调用 `uqInfluxdb.MockRouter`。

- [x] **Step 2: 增加 `a/b*100` 测试**

测试名：`TestQueryTsNativeVMInfluxExpandsAllReferencesInBinaryExpr`

关键断言：

```go
assert.Contains(t, capturedPromQL, `__name__="container_memory_rss_value"`)
assert.Contains(t, capturedPromQL, `__name__="container_spec_memory_limit_bytes_value"`)
assert.NotContains(t, capturedPromQL, "(b)")
assert.NotContains(t, capturedPromQL, `{__name__="b"}`)
```

- [x] **Step 3: 增加三 reference 测试**

测试名：`TestQueryTsNativeVMInfluxExpandsAllReferencesInThreeReferenceExpr`

覆盖：

```promql
a/b-(c-1)/c
```

关键断言：

```go
assert.Contains(t, capturedPromQL, `__name__="kube_pod_container_resource_requests_cpu_cores_value"`)
assert.Contains(t, capturedPromQL, `__name__="kube_node_status_allocatable_cpu_cores_value"`)
assert.NotContains(t, capturedPromQL, "(b)")
assert.NotContains(t, capturedPromQL, "(c)")
```

- [x] **Step 4: 增加嵌套 aggregate 测试**

测试名：`TestQueryTsNativeVMInfluxExpandsReferenceInNestedAggregateExpr`

覆盖：

```promql
a * topk(1, max by (...) (b))
```

关键断言：

```go
assert.Contains(t, capturedPromQL, `__name__="kube_pod_status_phase_value"`)
assert.Contains(t, capturedPromQL, `__name__="kube_pod_owner_value"`)
assert.Contains(t, capturedPromQL, "topk")
assert.NotContains(t, capturedPromQL, "(b)")
```

---

### Task 2: 补充结构化层递归展开测试

**文件：**
- 修改：`pkg/unify-query/query/structured/native_vm_influx_test.go`

- [x] **Step 1: 增加 `QueryTs.ToPromExpr` 嵌套 reference 测试**

测试名：`TestQueryTsToPromExprNativeVMInfluxExpandsNestedReferences`

使用当前代码实际字段：

```go
AggregateMethodList: []AggregateMethod{
	{Method: "max", Dimensions: []string{"pod"}},
	{Method: "topk", VArgsList: []interface{}{1}, Position: 1},
}
```

- [x] **Step 2: 验证测试**

运行：

```bash
cd /Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query
go test ./query/structured -run TestQueryTsToPromExprNativeVMInfluxExpandsNestedReferences -count=1 -v
```

结果：通过。

---

### Task 3: 新增 Native VM reference 泄漏检测

**文件：**
- 新增：`pkg/unify-query/query/structured/reference_leak.go`
- 新增：`pkg/unify-query/query/structured/reference_leak_test.go`

- [x] **Step 1: 先写失败测试**

测试名：

```go
TestCheckReferenceLeakDetectsVectorSelectorName
TestCheckReferenceLeakIgnoresExpandedNativeSelector
TestQueryTsReferenceNames
```

初次运行预期失败：

```text
undefined: CheckReferenceLeak
query.ReferenceNames undefined
```

- [x] **Step 2: 实现 `CheckReferenceLeak`**

实现逻辑：

```go
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
```

只检测裸 selector 名称，例如 `sum by (...) (b)` 中的 `b`。已经展开成 `{__name__="metric_b"}` 的 selector 不报错。

- [x] **Step 3: 实现 `QueryTs.ReferenceNames`**

从 `QueryList` 收集非空 `ReferenceName`，忽略 `nil` query 和空字符串。

- [x] **Step 4: 验证单测**

运行：

```bash
cd /Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query
go test ./query/structured -run 'TestCheckReferenceLeak|TestQueryTsReferenceNames' -count=1 -v
```

结果：通过。

---

### Task 4: 接入 Native VM handler

**文件：**
- 修改：`pkg/unify-query/service/http/handler.go`

- [x] **Step 1: 在请求 VM 前执行泄漏检测**

在 `query.ToPromExpr(...)` 后增加：

```go
if nativeOK {
	if err = structured.CheckReferenceLeak(promQL, query.ReferenceNames()); err != nil {
		log.Errorf(ctx, err.Error())
		return nil, err
	}
}
```

- [x] **Step 2: 验证 handler 测试**

运行：

```bash
cd /Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query
go test ./service/http -run 'TestQueryTsNativeVMInflux' -count=1 -v
```

结果：通过。

---

### Task 5: 回归验证

**文件：**
- 无新增生产代码。

- [x] **Step 1: 运行 Native VM / ReferenceLeak 目标测试**

运行：

```bash
cd /Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query
go test ./metadata ./query/structured ./service/http -run 'NativeVMInflux|ReferenceLeak|ReferenceNames' -count=1 -v
```

结果：通过。

- [x] **Step 2: 运行相关 tsdb 子包**

运行：

```bash
cd /Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query
go test ./tsdb ./tsdb/prometheus ./tsdb/victoriaMetricsInstance -count=1
```

结果：通过。

- [x] **Step 3: 记录已知非本次改动失败**

运行 `go test ./tsdb/... -count=1` 时观察到既有失败：

- `tsdb/victoriaMetrics/instance_test.go` 调用 `ins.Query` 参数数量与当前签名不一致。
- `tsdb/influxdb/instance_test.go` 期望 URL 带 `http://`，实际协议为空。

运行 `go test ./metadata ./query/structured ./service/http -count=1` 时观察到既有失败：

- `metadata/reference_test.go::TestCheckVmQuery` 多个 VM/Druid query 断言失败。
- `service/http/handler_test.go::TestStructAndPromQLConvert` 多个转换断言失败。
- `service/http/info_test.go::TestHandleTsQueryInfosRequest/field_keys` 出现类型断言 panic。

这些失败不由本次 Native VM reference leak guard 引入；本次目标测试均已通过。
