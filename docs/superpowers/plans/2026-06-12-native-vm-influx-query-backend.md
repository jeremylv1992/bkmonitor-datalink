# Native VM Influx Query Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When an InfluxDB storage backend is marked as native VictoriaMetrics, convert QueryTs metadata into standard MetricQL and query the actual VM select backend directly.

**Architecture:** Keep the existing InfluxDB path and the existing BKData VM gateway path unchanged. Add a parallel native-VM-over-Influx path that uses Influx metadata (`DB`, `Measurement`, `Field`, tags, filters) to build selectors such as `{__name__="measurement_field",db="database",tag="value"}`. Route the final MetricQL to a VM backend instance using `/select/<tenant>/prometheus/api/v1/query(_range)`.

**Tech Stack:** Go, Prometheus `parser` and `labels`, current `pkg/unify-query` metadata/query/tsdb packages, VictoriaMetrics Influx ingestion and Prometheus query APIs.

---

## External Contract

VictoriaMetrics Influx ingestion maps Influx line protocol as follows:

- Influx `db` query arg becomes a label, default name `db`.
- Influx `measurement` plus field name becomes the VM metric name with `_` as the default separator.
- Influx tags become VM labels.
- Cluster queries use `/select/<accountID>/prometheus/api/v1/query` and `/select/<accountID>/prometheus/api/v1/query_range`.

Implementation must not use `VmRt`, `result_table_id`, `MetricFilterCondition`, or `ResultTableList` for this native path.

## File Structure

- Modify `pkg/unify-query/consul/storage.go`: carry optional storage flags from Consul.
- Modify `pkg/unify-query/tsdb/struct.go`: add native VM Influx options to runtime storage metadata.
- Modify `pkg/unify-query/tsdb/storage.go`: parse storage flags and preserve normal `influxdb` storage type.
- Modify `pkg/unify-query/metadata/struct.go`: add native VM query metadata fields and expand structure.
- Modify `pkg/unify-query/metadata/queries.go`: add validation for native VM direct query grouping without `VmRt`.
- Modify `pkg/unify-query/query/structured/query_ts.go`: populate native VM metadata while QueryTs is converted to internal query metadata.
- Create `pkg/unify-query/query/structured/native_vm_influx.go`: build MetricQL metric names and label matchers from Influx metadata.
- Create `pkg/unify-query/query/structured/native_vm_influx_test.go`: unit tests for metric names, labels, filters, and OR restrictions.
- Modify `pkg/unify-query/service/http/handler.go`: select native VM path before the existing `CheckVmQuery` gateway path.
- Modify `pkg/unify-query/tsdb/victoriaMetricsInstance/instance.go`: complete native VM range and instant query backend.
- Create or extend `pkg/unify-query/tsdb/victoriaMetricsInstance/instance_test.go`: test URL construction, request params, and response conversion.
- Modify `pkg/unify-query/tsdb/prometheus/querier.go`: return native VM instance for marked Influx storage.
- Extend `pkg/unify-query/service/http/handler_test.go`: end-to-end QueryTs conversion tests for native VM.

---

### Task 1: Add Storage-Level Native VM Marker

**Files:**
- Modify: `pkg/unify-query/consul/storage.go`
- Modify: `pkg/unify-query/tsdb/struct.go`
- Modify: `pkg/unify-query/tsdb/storage.go`
- Test: `pkg/unify-query/tsdb/storage_test.go`

- [ ] **Step 1: Add optional Consul storage options**

Add a backward-compatible field:

```go
type Storage struct {
	Address  string            `json:"address"`
	Username string            `json:"username"`
	Password string            `json:"password"`
	Type     string            `json:"type"`
	Options  map[string]string `json:"options,omitempty"`
}
```

Expected storage config for the new path:

```json
{
  "type": "influxdb",
  "address": "http://vmselect:8481",
  "options": {
    "native_vm_influx": "true",
    "native_vm_tenant": "0",
    "native_vm_api_prefix": "/select/0/prometheus/api/v1",
    "native_vm_db_label": "db",
    "native_vm_measurement_field_separator": "_",
    "native_vm_skip_single_field": "false"
  }
}
```

- [ ] **Step 2: Add runtime storage fields**

Add fields to `tsdb.Storage`:

```go
NativeVMInflux                    bool
NativeVMTenant                    string
NativeVMAPIPrefix                 string
NativeVMDBLabel                   string
NativeVMMeasurementFieldSeparator string
NativeVMSkipSingleField           bool
```

- [ ] **Step 3: Parse defaults in `ReloadTsDBStorage`**

Rules:

```go
nativeVMInflux := tsDB.Type == consul.InfluxDBStorageType && tsDB.Options["native_vm_influx"] == "true"
tenant := defaultString(tsDB.Options["native_vm_tenant"], "0")
apiPrefix := defaultString(tsDB.Options["native_vm_api_prefix"], fmt.Sprintf("/select/%s/prometheus/api/v1", tenant))
dbLabel := defaultString(tsDB.Options["native_vm_db_label"], "db")
separator := defaultString(tsDB.Options["native_vm_measurement_field_separator"], "_")
skipSingleField := tsDB.Options["native_vm_skip_single_field"] == "true"
```

Keep `Storage.Type` as `influxdb`; the marker controls query routing.

- [ ] **Step 4: Test parsing**

Run:

```bash
go test ./pkg/unify-query/tsdb -run TestReloadTsDBStorageNativeVMInflux -count=1
```

Expected: storage type remains `influxdb`, and native VM options are populated.

---

### Task 2: Preserve Native VM Query Metadata

**Files:**
- Modify: `pkg/unify-query/metadata/struct.go`
- Modify: `pkg/unify-query/query/structured/query_ts.go`
- Test: `pkg/unify-query/query/structured/native_vm_influx_test.go`

- [ ] **Step 1: Extend `metadata.Query`**

Add fields:

```go
NativeVMInflux                    bool
NativeVMTenant                    string
NativeVMAPIPrefix                 string
NativeVMDBLabel                   string
NativeVMMeasurementFieldSeparator string
NativeVMSkipSingleField           bool
NativeVMMatchers                  []*labels.Matcher
NativeVMUnsupportedOr             bool
```

- [ ] **Step 2: Copy storage options into query metadata**

In `Query.ToQueryMetric`, after `query.StorageID` is chosen and before appending to `queryMetric.QueryList`, call `tsdb.GetStorage(query.StorageID)`. If `storage.NativeVMInflux` is true, copy native VM options into `metadata.Query`.

Do not set `query.StorageType = victoria_metrics` and do not require `query.VmRt`.

- [ ] **Step 3: Build native VM matchers during condition conversion**

While converting QueryTs conditions and route filters, append native matchers from structured condition data. Do not parse `query.Condition` or `query.VmCondition` strings back into labels.

Supported operators:

```go
eq       -> labels.MatchEqual
ne       -> labels.MatchNotEqual
contains -> labels.MatchRegexp with anchored union when multiple values exist
ncontains -> labels.MatchNotRegexp with anchored union when multiple values exist
reg      -> labels.MatchRegexp
nreg     -> labels.MatchNotRegexp
```

If an OR spans different labels or mixed operators, set `NativeVMUnsupportedOr = true` and return a clear error in `CheckNativeVMInfluxQuery`.

- [ ] **Step 4: Test metadata preservation**

Run:

```bash
go test ./pkg/unify-query/query/structured -run TestNativeVMInfluxQueryMetadata -count=1
```

Expected: metadata includes `DB`, `Measurement`, `Field`, `NativeVMInflux=true`, and matchers for QueryTs filters.

---

### Task 3: Build MetricQL Selectors From Influx Metadata

**Files:**
- Create: `pkg/unify-query/query/structured/native_vm_influx.go`
- Test: `pkg/unify-query/query/structured/native_vm_influx_test.go`

- [ ] **Step 1: Implement metric name helper**

```go
func NativeVMInfluxMetricName(measurement, field, separator string, skipSingleField bool) string {
	if skipSingleField && field == metadata.StaticField {
		return measurement
	}
	return measurement + separator + field
}
```

- [ ] **Step 2: Implement selector matcher helper**

For each native VM query, create label matchers:

```go
labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName)
labels.MustNewMatcher(labels.MatchEqual, query.NativeVMDBLabel, query.DB)
```

Append `query.NativeVMMatchers` after checking that no matcher duplicates `__name__` or the configured db label.

- [ ] **Step 3: Respect existing measurement type logic**

Use the values already produced by `ToQueryMetric`:

- `bk_traditional_measurement`: existing `Measurement`, `Field=<metricName>`
- `bk_standard_v2_time_series`: existing `Measurement`, `Field=<metricName>`
- `bk_exporter`: existing `Measurement`, `Field=metric_value`, plus `metric_name=<metricName>`
- `bk_split_measurement`: existing `Measurement=<metricName>`, `Field=value`

- [ ] **Step 4: Test selector output**

Run:

```bash
go test ./pkg/unify-query/query/structured -run 'TestNativeVMInfluxMetricName|TestNativeVMInfluxSelector' -count=1
```

Expected examples:

```promql
{__name__="cpu_detail_usage",db="system",bk_biz_id="2"}
{__name__="bk_exporter_metric_value",db="system",metric_name="disk_usage"}
{__name__="usage_value",db="system"}
```

---

### Task 4: Add Native VM Expand and Validation

**Files:**
- Modify: `pkg/unify-query/metadata/struct.go`
- Modify: `pkg/unify-query/metadata/queries.go`
- Test: `pkg/unify-query/metadata/native_vm_influx_test.go`

- [ ] **Step 1: Add expand structure**

```go
type NativeVMInfluxExpand struct {
	MetricAliasMapping map[string]string
	LabelsMatcher      map[string][]*labels.Matcher
	StorageID          string
	ClusterName        string
	APIPrefix          string
}
```

For native VM, set `MetricAliasMapping[ref] = ""` and put `__name__`, `db`, and tag filters in `LabelsMatcher[ref]`. This forces PromQL rendering as `{__name__="...",db="..."}` instead of `metric{...}`.

- [ ] **Step 2: Validate one VM backend per query**

Reject:

- mixed native VM and non-native query refs in one request
- multiple native VM storage IDs in one request
- `NativeVMUnsupportedOr=true`
- empty `DB`, `Measurement`, or `Field`

- [ ] **Step 3: Keep old VM validation unchanged**

Do not change `CheckVmQuery`. Add a separate `CheckNativeVMInfluxQuery` function or method and call it before `CheckVmQuery`.

- [ ] **Step 4: Test validation**

Run:

```bash
go test ./pkg/unify-query/metadata -run TestCheckNativeVMInfluxQuery -count=1
```

Expected: native VM validates without `VmRt`; existing VM gateway tests still require `VmRt`.

---

### Task 5: Complete Native VM Backend

**Files:**
- Modify: `pkg/unify-query/tsdb/victoriaMetricsInstance/instance.go`
- Test: `pkg/unify-query/tsdb/victoriaMetricsInstance/instance_test.go`

- [ ] **Step 1: Normalize URL construction**

Use storage address plus API prefix:

```go
base := strings.TrimRight(address, "/") + "/" + strings.Trim(strings.TrimSpace(apiPrefix), "/")
queryRangeURL := base + "/query_range"
queryURL := base + "/query"
```

- [ ] **Step 2: Implement range query**

Send `query`, `start`, `end`, and `step` as URL query or form parameters. Keep timestamps in Unix seconds to match current code.

- [ ] **Step 3: Implement instant query**

Send `query` and `time` to `/query`, parse `vector` responses into `promql.Vector`.

- [ ] **Step 4: Harden errors**

Return errors containing:

- HTTP status code when non-2xx
- VM `errorType` and `error` when status is not `success`
- URL path and request kind in trace spans

- [ ] **Step 5: Test backend behavior**

Run:

```bash
go test ./pkg/unify-query/tsdb/victoriaMetricsInstance -run 'TestQueryRange|TestQuery' -count=1
```

Expected: URL path is `/select/0/prometheus/api/v1/query_range` or `/query`; matrix/vector data converts to Prometheus values.

---

### Task 6: Wire QueryTs to Native VM Before Gateway VM

**Files:**
- Modify: `pkg/unify-query/service/http/handler.go`
- Modify: `pkg/unify-query/tsdb/prometheus/querier.go`
- Test: `pkg/unify-query/service/http/handler_test.go`

- [ ] **Step 1: Select native VM path first**

In `queryTs`, after `queryReference := query.ToQueryReference(ctx)`, call:

```go
nativeExpand, nativeOK, err := queryReference.CheckNativeVMInfluxQuery(ctx)
```

If `nativeOK`, set:

```go
referenceNameMetric = nativeExpand.MetricAliasMapping
referenceNameLabelMatcher = nativeExpand.LabelsMatcher
```

Then get the native VM instance for `nativeExpand.StorageID`.

- [ ] **Step 2: Keep old VM gateway path second**

Only call existing `CheckVmQuery` when `nativeOK` is false. This preserves `VmRt/result_table_id` behavior for existing gateway VM queries.

- [ ] **Step 3: Return native VM instance for marked Influx storage**

In `prometheus.GetInstance`, before the `switch storage.Type`, check `storage.NativeVMInflux`. If true, return `victoriaMetricsInstance.NewInstance` configured with `storage.Address`, `storage.NativeVMAPIPrefix`, `storage.Timeout`, and curl.

- [ ] **Step 4: Test rendered MetricQL**

Run:

```bash
go test ./pkg/unify-query/service/http -run TestQueryTsNativeVMInflux -count=1
```

Expected rendered query contains:

```promql
{__name__="cpu_detail_usage",db="system",bk_biz_id="2"}
```

Expected rendered query does not contain:

```text
result_table_id
VmRt
metric_filter_condition
```

---

### Task 7: Add Integration Verification

**Files:**
- Create: `pkg/unify-query/tsdb/victoriaMetricsInstance/integration_test.go`
- Modify: test docs if the repository has an existing integration-test convention.

- [ ] **Step 1: Add opt-in integration test**

Guard with:

```go
if os.Getenv("VM_INTEGRATION_ADDR") == "" {
	t.Skip("VM_INTEGRATION_ADDR is not set")
}
```

- [ ] **Step 2: Write line protocol through VM Influx endpoint**

Use:

```text
POST $VM_INSERT_ADDR/insert/0/influx/write?db=test_db
cpu_detail,bk_biz_id=2 usage=1.23 <unix_nano>
```

- [ ] **Step 3: Query through VM select endpoint**

Query:

```promql
{__name__="cpu_detail_usage",db="test_db",bk_biz_id="2"}
```

- [ ] **Step 4: Run integration test**

Run:

```bash
VM_INSERT_ADDR=http://vminsert:8480 VM_INTEGRATION_ADDR=http://vmselect:8481 go test ./pkg/unify-query/tsdb/victoriaMetricsInstance -run TestNativeVMInfluxIntegration -count=1
```

Expected: one series is returned with value `1.23`.

---

### Task 8: Full Regression and Rollout

**Files:**
- No new files required unless release notes are maintained in this repo.

- [ ] **Step 1: Run targeted tests**

```bash
go test ./pkg/unify-query/query/structured ./pkg/unify-query/metadata ./pkg/unify-query/tsdb ./pkg/unify-query/tsdb/victoriaMetricsInstance ./pkg/unify-query/service/http -count=1
```

- [ ] **Step 2: Run broader unify-query tests**

```bash
go test ./pkg/unify-query/... -count=1
```

- [ ] **Step 3: Roll out behind storage marker**

Only storages with `options.native_vm_influx=true` use the new path. All other InfluxDB storage and all existing BKData VM gateway routes keep current behavior.

- [ ] **Step 4: Add observability**

Add trace fields and metrics tags:

```text
native_vm_influx=true
native_vm_storage_id=<storageID>
native_vm_api_prefix=<apiPrefix>
native_vm_metric_name=<metricName>
```

Ensure logs redact passwords and tokens.

---

## Acceptance Criteria

- A result table whose InfluxDB storage has `native_vm_influx=true` is queried through native VM select API.
- MetricQL is assembled from Influx metadata and contains `db="<query.DB>"`.
- Native VM MetricQL does not contain `result_table_id` or `VmRt`.
- Existing InfluxDB query behavior is unchanged for unmarked storage.
- Existing BKData VM gateway behavior is unchanged for `VmRt`-based storage.
- Unit tests cover metric name mapping for all existing `MeasurementType` values.
- Backend tests cover both `query` and `query_range`.
