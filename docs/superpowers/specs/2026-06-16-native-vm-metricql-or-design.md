# Native VM MetricQL OR Design

## Goal

Support VictoriaMetrics MetricsQL `or` label filters for `/query/ts` native VM Influx queries.

The existing native VM path rejects structured OR conditions because it represents selectors with Prometheus `labels.Matcher`, while MetricsQL supports selector-level OR groups such as:

```metricsql
{__name__="cpu_detail_usage",db="system",bk_biz_id="2" or __name__="cpu_detail_usage",db="system",bk_biz_id="3"}
```

This is a conversion limitation in unify-query, not a VictoriaMetrics syntax limitation.

## Scope

In scope:

- `/query/ts` native VM Influx path.
- Query conditions from `Query.Conditions`.
- Space/router filters from `tsDB.Filters`.
- Existing aggregation, time aggregation, offset, subquery, and `metric_merge` behavior.

Out of scope:

- Raw PromQL endpoint behavior.
- Traditional InfluxDB execution path.
- General VictoriaMetrics storage path unrelated to native VM Influx.
- Multiple native VM backends in one request. The existing single-backend restriction remains.

## Current Flow

1. `QueryTs.ToQueryReference` builds `metadata.Query` values from structured queries.
2. If a result table cluster has readable `vmcluster_info`, each query is marked `NativeVMInflux`.
3. `CheckNativeVMInfluxQuery` builds `NativeVMInfluxExpand.LabelsMatcher`.
4. `handler.go` passes matcher overrides to `query.ToPromExpr`.
5. `query.ToPromExpr` builds a Prometheus parser AST and serializes it to a string.
6. `victoriaMetricsInstance` sends the string to vmselect.

This cannot express MetricsQL selector OR because Prometheus parser AST has no selector-level OR group representation.

## Design

Represent native VM filters as OR groups instead of a flat matcher list.

```go
type Query struct {
    NativeVMMatcherGroups [][]*labels.Matcher
}
```

- Outer slice: OR groups.
- Inner slice: AND matchers in one group.
- Each group must include the reserved native VM matchers:
  - `__name__=<measurement_field>`
  - `<db_label>=<db>`

Build raw MetricsQL selectors from these groups:

```metricsql
{__name__="cpu_detail_usage",db="system",bk_biz_id="2" or __name__="cpu_detail_usage",db="system",bk_biz_id="3"}
```

Use Prometheus AST only for the expression shell:

1. Generate a unique placeholder metric for every query reference, for example `__bk_native_vm_ref_0__`.
2. Build the existing PromQL AST with placeholders and no selector matchers.
3. Serialize the AST.
4. Replace each placeholder metric token with the raw MetricsQL selector.

Example:

```promql
sum(rate(__bk_native_vm_ref_0__[1m]))
```

becomes:

```metricsql
sum(rate({__name__="cpu_detail_usage",db="system",bk_biz_id="2" or __name__="cpu_detail_usage",db="system",bk_biz_id="3"}[1m]))
```

This preserves existing aggregation/window/offset behavior while enabling MetricsQL-specific selector syntax.

## OR Combination Rules

`Conditions.AnalysisConditions()` already returns OR groups:

```text
A and B or C
```

becomes:

```text
[[A, B], [C]]
```

For native VM:

- Convert each group into matcher groups.
- Convert `contains`/`ncontains` with the existing PromQL-compatible regexp matcher rules.
- Combine query condition groups and filter condition groups with a Cartesian product because they are ANDed together at the query level.
- Apply a safety limit for expanded groups, default `64`, to avoid very large selectors.

## Error Handling

Keep existing errors for:

- Empty storage id.
- Empty native VM address or API prefix.
- Mixed native and non-native queries.
- Multiple native VM backends in one request.
- Multiple table ids under one native reference.

Add or preserve errors for:

- Invalid label matcher.
- Reserved label conflicts with `__name__` or db label.
- OR group expansion exceeds the configured/default limit.

## Testing

Add focused tests:

- `metadata/native_vm_influx_test.go`
  - Build single-group selector.
  - Build multi-group OR selector.
  - Ensure every OR group repeats `__name__` and db label.
  - Escape label values.
  - Reject reserved label conflicts.

- `query/structured/native_vm_influx_test.go`
  - Structured OR conditions become `NativeVMMatcherGroups`.
  - Query conditions and router filters combine correctly.

- `service/http/handler_native_vm_influx_test.go`
  - `/query/ts` native VM request with OR sends MetricsQL selector OR to vmselect.
  - Aggregation/window wrapping remains correct.
  - Reference placeholder does not leak into final query.

## Rollout

The change is isolated to the native VM Influx path. Existing non-native and Prometheus-engine paths keep their current behavior.

If production validation finds a MetricsQL compatibility issue, rollback can be done by restoring the previous `NativeVMUnsupportedOr` rejection.
