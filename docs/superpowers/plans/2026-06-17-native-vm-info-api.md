# Native VM Info API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 native VM Influx 查询路径支持 `/query/ts/info/tag_keys`、`/query/ts/info/tag_values` 和 `/query/ts/info/series`。

**Architecture:** HTTP info 接口继续走 `queryInfo -> newInfoQuerier -> prometheus.Querier`。当结果表命中 native VM Influx 时，`prometheus.GetInstance` 返回 `victoriaMetricsInstance.Instance`，由该 instance 使用 InfluxDB 元数据通过 `metadata.NativeVMInfluxSelector` 拼出 MetricQL selector，再请求实际 VM 的标准 metadata API。

**Tech Stack:** Go, Prometheus `storage.SeriesSet`, VictoriaMetrics `/labels` 与 `/series` API, 现有 `curl.Curl` 抽象。

---

## File Structure

- Modify: `pkg/unify-query/tsdb/victoriaMetricsInstance/struct.go`
  - 新增 `/labels` 和 `/series` metadata API 的响应结构。
- Modify: `pkg/unify-query/tsdb/victoriaMetricsInstance/instance.go`
  - 实现 `LabelNames`、`LabelValues`、`Series`。
  - `QueryRaw` 在 `hints.Func == "series"` 时委派给 `Series`，支撑 `/query/ts/info/series`。
  - 增加本地 `storage.SeriesSet` 包装，把 VM `/series` 返回的 label map 转成 Prometheus series。
- Modify: `pkg/unify-query/tsdb/victoriaMetricsInstance/instance_test.go`
  - 补充红灯用例，覆盖请求路径、selector、basic auth、去重排序、`QueryRaw` series 委派。

## API Mapping

- `LabelNames`
  - selector: `metadata.NativeVMInfluxSelector(query)`
  - request: `GET {address}{apiPrefix}/labels?match[]={selector}&start={unix}&end={unix}`
  - response: `{"status":"success","data":["__name__","db","bk_biz_id"]}`

- `LabelValues`
  - selector: `metadata.NativeVMInfluxSelector(query)`
  - request: `GET {address}{apiPrefix}/series?match[]={selector}&start={unix}&end={unix}`
  - response: `{"status":"success","data":[{"bk_biz_id":"2"},{"bk_biz_id":"3"}]}`
  - behavior: 从 series label map 中提取目标 label，去重后排序。

- `Series`
  - selector: `metadata.NativeVMInfluxSelector(query)`
  - request: `GET {address}{apiPrefix}/series?match[]={selector}&start={unix}&end={unix}`
  - behavior: 将每个 label map 转成无样本的 `storage.Series`，供 `queryInfo` 组装 series 响应。

## Tasks

### Task 1: Add Response Types

**Files:**
- Modify: `pkg/unify-query/tsdb/victoriaMetricsInstance/struct.go`

- [ ] **Step 1: Add native VM metadata API response structs**

```go
type LabelsData struct {
	Status    string   `json:"status"`
	ErrorType string   `json:"errorType,omitempty"`
	Error     string   `json:"error,omitempty"`
	Data      []string `json:"data,omitempty"`
}

type SeriesData struct {
	Status    string   `json:"status"`
	ErrorType string   `json:"errorType,omitempty"`
	Error     string   `json:"error,omitempty"`
	Data      []Metric `json:"data,omitempty"`
}
```

### Task 2: Write Failing Instance Tests

**Files:**
- Modify: `pkg/unify-query/tsdb/victoriaMetricsInstance/instance_test.go`

- [ ] **Step 1: Add tests**

Cover these behaviors:

```go
func TestInstanceLabelNamesUsesNativeVMLabelsAPI(t *testing.T)
func TestInstanceLabelValuesUsesNativeVMSeriesAPI(t *testing.T)
func TestInstanceSeriesUsesNativeVMSeriesAPI(t *testing.T)
func TestInstanceQueryRawDelegatesSeriesLookup(t *testing.T)
```

- [ ] **Step 2: Run tests and confirm red**

Run:

```bash
cd pkg/unify-query
go test ./tsdb/victoriaMetricsInstance -run 'TestInstance(LabelNames|LabelValues|Series|QueryRawDelegates)' -count=1
```

Expected: fail/panic because the target methods are not implemented yet.

### Task 3: Implement Metadata Query Helpers

**Files:**
- Modify: `pkg/unify-query/tsdb/victoriaMetricsInstance/instance.go`

- [ ] **Step 1: Add decode helpers and request helper**

Implementation rules:

```go
func (i *Instance) metadataAPIValues(query *metadata.Query, start, end time.Time) (url.Values, string, error)
func (i *Instance) decodeLabelsData(resp *http.Response) ([]string, error)
func (i *Instance) decodeSeriesData(resp *http.Response) ([]Metric, error)
```

- [ ] **Step 2: Implement `LabelNames`**

Use `labels` endpoint and `match[]` query param.

- [ ] **Step 3: Implement `LabelValues`**

Use `series` endpoint and extract `name` from returned metric labels.

- [ ] **Step 4: Implement `Series` and local `storage.SeriesSet`**

Return `storage.ErrSeriesSet(err)` on request/decode errors, `storage.EmptySeriesSet()` on empty VM response.

- [ ] **Step 5: Implement `QueryRaw` series delegation**

```go
if hints != nil && hints.Func == "series" {
    return i.Series(ctx, query, time.UnixMilli(hints.Start), time.UnixMilli(hints.End), matchers...)
}
return storage.EmptySeriesSet()
```

### Task 4: Verify

**Files:**
- Test only.

- [ ] **Step 1: Run focused package test**

```bash
cd pkg/unify-query
go test ./tsdb/victoriaMetricsInstance -count=1
```

- [ ] **Step 2: Run HTTP native VM regression test**

```bash
cd pkg/unify-query
go test ./service/http -run 'TestQueryTsNativeVMInflux|TestQueryInfoNativeVM' -count=1
```

- [ ] **Step 3: Inspect diff**

```bash
git status --short --branch
git diff -- pkg/unify-query/tsdb/victoriaMetricsInstance docs/superpowers/plans/2026-06-17-native-vm-info-api.md
```

## Notes

- 不使用 `VmRt` 拼 selector。native VM Influx 入库后 database 在 VM 中是 `db` label，因此继续以 `query.DB/query.Measurement/query.Field` 为准。
- 这次不改 HTTP 入参结构；兼容现有 `/query/ts/info/*` 请求体。
- `/query/ts/info/series` 的 `Select` 路径会调用 `QueryRaw`，所以必须补 `QueryRaw` 的 series 分支。
