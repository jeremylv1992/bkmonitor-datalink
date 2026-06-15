# VMCluster Native VM Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `metadata_vmclusterinfo` as the native VM backend metadata for InfluxDB clusters and make `unify-query` switch to native VM only when the Consul-published VM cluster matching the InfluxDB proxy `clusterName` is readable.

**Architecture:** `bk-monitor-gf` owns the database model and publishes one Consul record per VM cluster under `unify-query/data/vmcluster_info/{cluster_name}` plus a version key. `bkmonitor-datalink` watches this Consul data, keeps a local cache, and during queryts metadata expansion switches a query to native VM only when the InfluxDB proxy `clusterName` has a matching readable VM cluster record. The MetricQL syntax still uses InfluxDB metadata (`db`, `measurement`, `field`), while the execution backend uses `select.address`, `select.api_prefix`, and `select.basic_auth`.

**Tech Stack:** Django 3.2 models/migrations/admin/tasks in `bk-monitor-gf`; Go Consul readers, query metadata, and Prometheus/VM query path in `bkmonitor-datalink`.

---

## File Structure

`/Users/lvzeli/Projects/kingeye4/bk-monitor-gf`

- Create: `metadata/models/vm_cluster.py`
  - Defines `VMClusterInfo`, Consul payload generation, and refresh/cleanup helpers.
- Create: `metadata/migrations/0174_vmclusterinfo.py`
  - Creates `metadata_vmclusterinfo`.
- Modify: `metadata/models/__init__.py`
  - Exposes `VMClusterInfo`.
- Modify: `metadata/admin.py`
  - Registers `VMClusterInfo`.
- Modify: `metadata/task/config_refresh.py`
  - Publishes VM cluster info with the same route refresh batch as InfluxDB routing.
- Create: `metadata/tests/test_vmcluster_info.py`
  - Verifies Consul key shape and version refresh.

`/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink`

- Create: `pkg/unify-query/consul/vmcluster.go`
  - Reads/watches `unify-query/data/vmcluster_info`.
- Create: `pkg/unify-query/consul/vmcluster_test.go`
  - Verifies Consul key parsing and cache behavior.
- Modify: `pkg/unify-query/service/influxdb/service.go`
  - Adds VM cluster reload loop.
- Modify: `pkg/unify-query/metadata/struct.go`
  - Adds native VM select address and auth fields.
- Modify: `pkg/unify-query/metadata/native_vm_influx.go`
  - Compares native VM backend identity instead of only storage ID.
- Modify: `pkg/unify-query/query/structured/query_ts.go`
  - Uses VM cluster cache to mark queryts references as native VM.
- Modify: `pkg/unify-query/tsdb/prometheus/querier.go`
  - Creates native VM instance from query-level select backend.
- Modify: `pkg/unify-query/tsdb/victoriaMetricsInstance/instance.go`
  - Supports select basic auth for native VM query requests.
- Update existing native VM tests under `pkg/unify-query/...`.

---

### Task 1: Add VMClusterInfo Model In bk-monitor-gf

**Files:**
- Create: `/Users/lvzeli/Projects/kingeye4/bk-monitor-gf/metadata/models/vm_cluster.py`
- Modify: `/Users/lvzeli/Projects/kingeye4/bk-monitor-gf/metadata/models/__init__.py`
- Modify: `/Users/lvzeli/Projects/kingeye4/bk-monitor-gf/metadata/admin.py`

- [ ] **Step 1: Write the model**

Create `/Users/lvzeli/Projects/kingeye4/bk-monitor-gf/metadata/models/vm_cluster.py`:

```python
# -*- coding: utf-8 -*-
import json
import logging
import time

from bkcrypto.contrib.django.fields import SymmetricTextField
from django.db import models

from metadata import config
from metadata.utils import consul_tools

logger = logging.getLogger("metadata")


class VMClusterInfo(models.Model):
    """VictoriaMetrics backend used as native query storage for InfluxDB-compatible data."""

    CONSUL_PREFIX_PATH = "%s/unify-query/data/vmcluster_info" % config.CONSUL_PATH
    CONSUL_VERSION_PATH = "%s/unify-query/version/vmcluster_info" % config.CONSUL_PATH

    cluster_name = models.CharField("集群名", max_length=128, unique=True)
    readable = models.BooleanField("是否可读", default=False)
    writable = models.BooleanField("是否可写", default=False)

    select_address = models.CharField("VMSelect地址", max_length=256, default="")
    select_api_prefix = models.CharField("VMSelect API前缀", max_length=256, default="/select/0/prometheus/api/v1")
    select_username = models.CharField("VMSelect用户名", max_length=64, blank=True, default="")
    select_password = SymmetricTextField("VMSelect密码", blank=True, default="")

    insert_address = models.CharField("VMInsert地址", max_length=256, default="")
    insert_api_prefix = models.CharField("VMInsert API前缀", max_length=256, default="/insert/0/influx")
    insert_username = models.CharField("VMInsert用户名", max_length=64, blank=True, default="")
    insert_password = SymmetricTextField("VMInsert密码", blank=True, default="")

    db_label = models.CharField("InfluxDB database label", max_length=64, default="db")
    measurement_field_separator = models.CharField("measurement与field分隔符", max_length=16, default="_")
    skip_single_field = models.BooleanField("单字段是否省略field后缀", default=False)

    custom_option = models.TextField("自定义配置", default="{}")
    description = models.CharField("备注", max_length=256, blank=True, default="")

    class Meta:
        verbose_name = "VictoriaMetrics集群信息"
        verbose_name_plural = "VictoriaMetrics集群信息表"

    @property
    def consul_config_path(self):
        return "/".join([self.CONSUL_PREFIX_PATH, self.cluster_name])

    def custom_option_json(self):
        if not self.custom_option:
            return {}
        try:
            value = json.loads(self.custom_option)
        except (TypeError, ValueError):
            logger.warning("vm cluster custom_option is invalid json, cluster_name: %s", self.cluster_name)
            return {}
        return value if isinstance(value, dict) else {}

    @property
    def consul_config(self):
        return {
            "cluster_name": self.cluster_name,
            "readable": self.readable,
            "writable": self.writable,
            "select": {
                "address": self.select_address,
                "api_prefix": self.select_api_prefix,
                "basic_auth": {
                    "username": self.select_username,
                    "password": self.select_password,
                },
            },
            "insert": {
                "address": self.insert_address,
                "api_prefix": self.insert_api_prefix,
                "basic_auth": {
                    "username": self.insert_username,
                    "password": self.insert_password,
                },
            },
            "influx_compat": {
                "db_label": self.db_label,
                "measurement_field_separator": self.measurement_field_separator,
                "skip_single_field": self.skip_single_field,
            },
            "custom_option": self.custom_option_json(),
        }

    @classmethod
    def refresh_consul_cluster_config(cls, cluster_name=None):
        hash_consul = consul_tools.HashConsul()
        info_list = cls.objects.all()
        if cluster_name is not None:
            info_list = info_list.filter(cluster_name=cluster_name)

        total_count = info_list.count()
        for info in info_list:
            hash_consul.put(key=info.consul_config_path, value=info.consul_config)
            logger.info("vm cluster->[%s] refresh consul config success.", info.cluster_name)

        hash_consul.put(key=cls.CONSUL_VERSION_PATH, value={"time": time.time()})
        logger.info("all vm cluster info is refresh to consul success count->[%s].", total_count)

    @classmethod
    def clean_consul_config(cls):
        hash_consul = consul_tools.HashConsul()
        result_data = hash_consul.list(cls.CONSUL_PREFIX_PATH)
        if not result_data[1]:
            return
        for item in result_data[1]:
            key = item["Key"]
            name = key.split("/")[-1]
            if not cls.objects.filter(cluster_name=name).exists():
                hash_consul.delete(key)
                logger.info("vm cluster info:%s deleted in consul", key)
```

- [ ] **Step 2: Export and register the model**

Modify `/Users/lvzeli/Projects/kingeye4/bk-monitor-gf/metadata/models/__init__.py`:

```python
from .vm_cluster import VMClusterInfo
```

Add `"VMClusterInfo"` to `__all__`.

Modify `/Users/lvzeli/Projects/kingeye4/bk-monitor-gf/metadata/admin.py`:

```python
class VMClusterInfoAdmin(admin.ModelAdmin):
    list_display = ("cluster_name", "readable", "writable", "select_address", "insert_address")
    search_fields = ("cluster_name", "select_address", "insert_address")
    list_filter = ("readable", "writable")


admin.site.register(models.VMClusterInfo, VMClusterInfoAdmin)
```

- [ ] **Step 3: Create migration**

Create `/Users/lvzeli/Projects/kingeye4/bk-monitor-gf/metadata/migrations/0174_vmclusterinfo.py`:

```python
# Generated by Codex on 2026-06-14

import bkcrypto.contrib.django.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("metadata", "0173_essnapshotrestore_repository_name"),
    ]

    operations = [
        migrations.CreateModel(
            name="VMClusterInfo",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("cluster_name", models.CharField(max_length=128, unique=True, verbose_name="集群名")),
                ("readable", models.BooleanField(default=False, verbose_name="是否可读")),
                ("writable", models.BooleanField(default=False, verbose_name="是否可写")),
                ("select_address", models.CharField(default="", max_length=256, verbose_name="VMSelect地址")),
                ("select_api_prefix", models.CharField(default="/select/0/prometheus/api/v1", max_length=256, verbose_name="VMSelect API前缀")),
                ("select_username", models.CharField(blank=True, default="", max_length=64, verbose_name="VMSelect用户名")),
                ("select_password", bkcrypto.contrib.django.fields.SymmetricTextField(blank=True, default="", verbose_name="VMSelect密码")),
                ("insert_address", models.CharField(default="", max_length=256, verbose_name="VMInsert地址")),
                ("insert_api_prefix", models.CharField(default="/insert/0/influx", max_length=256, verbose_name="VMInsert API前缀")),
                ("insert_username", models.CharField(blank=True, default="", max_length=64, verbose_name="VMInsert用户名")),
                ("insert_password", bkcrypto.contrib.django.fields.SymmetricTextField(blank=True, default="", verbose_name="VMInsert密码")),
                ("db_label", models.CharField(default="db", max_length=64, verbose_name="InfluxDB database label")),
                ("measurement_field_separator", models.CharField(default="_", max_length=16, verbose_name="measurement与field分隔符")),
                ("skip_single_field", models.BooleanField(default=False, verbose_name="单字段是否省略field后缀")),
                ("custom_option", models.TextField(default="{}", verbose_name="自定义配置")),
                ("description", models.CharField(blank=True, default="", max_length=256, verbose_name="备注")),
            ],
            options={
                "verbose_name": "VictoriaMetrics集群信息",
                "verbose_name_plural": "VictoriaMetrics集群信息表",
            },
        ),
    ]
```

### Task 2: Publish VMClusterInfo From bk-monitor-gf

**Files:**
- Modify: `/Users/lvzeli/Projects/kingeye4/bk-monitor-gf/metadata/task/config_refresh.py`
- Test: `/Users/lvzeli/Projects/kingeye4/bk-monitor-gf/metadata/tests/test_vmcluster_info.py`

- [ ] **Step 1: Add route refresh hook**

In `/Users/lvzeli/Projects/kingeye4/bk-monitor-gf/metadata/task/config_refresh.py`, after `models.AccessVMRecord.refresh_vm_router()` add:

```python
        models.VMClusterInfo.refresh_consul_cluster_config()
        logger.debug("vm cluster refresh consul config success.")
```

- [ ] **Step 2: Write tests for Consul payload**

Create `/Users/lvzeli/Projects/kingeye4/bk-monitor-gf/metadata/tests/test_vmcluster_info.py`:

```python
import json

from metadata.models.vm_cluster import VMClusterInfo


def test_vmcluster_consul_config():
    item = VMClusterInfo(
        cluster_name="cluster-a",
        readable=True,
        writable=True,
        select_address="http://vmselect:8481",
        select_api_prefix="/select/0/prometheus/api/v1",
        select_username="query",
        select_password="query-pass",
        insert_address="http://vminsert:8480",
        insert_api_prefix="/insert/0/influx",
        insert_username="write",
        insert_password="write-pass",
        custom_option=json.dumps({"owner": "metadata"}),
    )

    assert item.consul_config_path.endswith("/unify-query/data/vmcluster_info/cluster-a")
    assert item.consul_config["readable"] is True
    assert item.consul_config["select"]["address"] == "http://vmselect:8481"
    assert item.consul_config["select"]["basic_auth"]["username"] == "query"
    assert item.consul_config["insert"]["api_prefix"] == "/insert/0/influx"
    assert item.consul_config["influx_compat"]["db_label"] == "db"
    assert item.consul_config["custom_option"] == {"owner": "metadata"}
```

### Task 3: Add VMCluster Consul Cache In bkmonitor-datalink

**Files:**
- Create: `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/consul/vmcluster.go`
- Create: `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/consul/vmcluster_test.go`
- Modify: `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/service/influxdb/service.go`

- [ ] **Step 1: Add Consul struct and cache**

Create `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/consul/vmcluster.go`:

```go
package consul

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/hashicorp/consul/api"
)

var (
	vmClusterInfoPath = "vmcluster_info"
	vmClusterInfo     = make(map[string]*VMClusterInfo)
	vmClusterInfoHash string
	vmClusterInfoLock = new(sync.RWMutex)
)

type VMBasicAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type VMEndpoint struct {
	Address   string      `json:"address"`
	APIPrefix string      `json:"api_prefix"`
	BasicAuth VMBasicAuth `json:"basic_auth"`
}

type VMInfluxCompat struct {
	DBLabel                   string `json:"db_label"`
	MeasurementFieldSeparator string `json:"measurement_field_separator"`
	SkipSingleField           bool   `json:"skip_single_field"`
}

type VMClusterInfo struct {
	ClusterName  string                 `json:"cluster_name"`
	Readable     bool                   `json:"readable"`
	Writable     bool                   `json:"writable"`
	Select       VMEndpoint             `json:"select"`
	Insert       VMEndpoint             `json:"insert"`
	InfluxCompat VMInfluxCompat         `json:"influx_compat"`
	CustomOption map[string]interface{} `json:"custom_option"`
}

func FormatVMClusterInfo(kvPairs api.KVPairs) (map[string]*VMClusterInfo, error) {
	result := make(map[string]*VMClusterInfo)
	prefix := fmt.Sprintf("%s/%s/%s/", basePath, dataPath, vmClusterInfoPath)
	for _, kvPair := range kvPairs {
		var data VMClusterInfo
		if err := json.Unmarshal(kvPair.Value, &data); err != nil {
			return nil, err
		}
		key := strings.ReplaceAll(kvPair.Key, prefix, "")
		if data.ClusterName == "" {
			data.ClusterName = key
		}
		result[key] = &data
	}
	return result, nil
}

func GetVMClusterInfo() (map[string]*VMClusterInfo, error) {
	path := fmt.Sprintf("%s/%s/%s", basePath, dataPath, vmClusterInfoPath)
	pairs, err := GetDataWithPrefix(path)
	if err != nil {
		return nil, err
	}
	return FormatVMClusterInfo(pairs)
}

func ReloadVMClusterInfo() error {
	newInfo, err := GetVMClusterInfo()
	if err != nil {
		return err
	}
	hash := HashIt(newInfo)
	if hash == vmClusterInfoHash {
		return nil
	}

	vmClusterInfoLock.Lock()
	defer vmClusterInfoLock.Unlock()
	vmClusterInfo = newInfo
	vmClusterInfoHash = hash
	return nil
}

func WatchVMClusterInfo(ctx context.Context) (<-chan interface{}, error) {
	path := fmt.Sprintf("%s/%s/%s", basePath, versionPath, vmClusterInfoPath)
	return WatchChange(ctx, path)
}

func GetCachedVMClusterInfo(clusterName string) (*VMClusterInfo, bool) {
	vmClusterInfoLock.RLock()
	defer vmClusterInfoLock.RUnlock()
	info, ok := vmClusterInfo[clusterName]
	return info, ok
}
```

- [ ] **Step 2: Add cache tests**

Create `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/consul/vmcluster_test.go`:

```go
package consul

import (
	"testing"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatVMClusterInfo(t *testing.T) {
	infos, err := FormatVMClusterInfo(api.KVPairs{
		{
			Key: []byte("bkmonitorv3/unify-query/data/vmcluster_info/cluster-a"),
			Value: []byte(`{
				"cluster_name":"cluster-a",
				"readable":true,
				"select":{
					"address":"http://vmselect:8481",
					"api_prefix":"/select/0/prometheus/api/v1",
					"basic_auth":{"username":"query","password":"secret"}
				},
				"insert":{
					"address":"http://vminsert:8480",
					"api_prefix":"/insert/0/influx",
					"basic_auth":{"username":"write","password":"secret"}
				},
				"influx_compat":{
					"db_label":"db",
					"measurement_field_separator":"_",
					"skip_single_field":false
				},
				"custom_option":{"owner":"metadata"}
			}`),
		},
	})

	require.NoError(t, err)
	require.Contains(t, infos, "cluster-a")
	assert.True(t, infos["cluster-a"].Readable)
	assert.Equal(t, "http://vmselect:8481", infos["cluster-a"].Select.Address)
	assert.Equal(t, "/insert/0/influx", infos["cluster-a"].Insert.APIPrefix)
	assert.Equal(t, "db", infos["cluster-a"].InfluxCompat.DBLabel)
}
```

- [ ] **Step 3: Add reload loop**

Modify `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/service/influxdb/service.go`:

```go
	err = s.loopReloadVMClusterInfo(s.ctx)
	if err != nil {
		log.Errorf(context.TODO(), "start loop reload vm cluster info failed,err:%s", err)
	}
```

Add:

```go
func (s *Service) loopReloadVMClusterInfo(ctx context.Context) error {
	if err := consul.ReloadVMClusterInfo(); err != nil {
		log.Errorf(context.TODO(), "reload vm cluster info failed,error:%s", err)
		return err
	}
	ch, err := consul.WatchVMClusterInfo(ctx)
	if err != nil {
		return err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Warnf(context.TODO(), "vm cluster info reload loop exit")
				return
			case <-ch:
				log.Debugf(context.TODO(), "get vm cluster info changed notify")
				if err := consul.ReloadVMClusterInfo(); err != nil {
					log.Errorf(context.TODO(), "reload vm cluster info failed, err: %s", err)
				}
			}
		}
	}()
	return nil
}
```

### Task 4: Switch queryts To Native VM By VMClusterInfo

**Files:**
- Modify: `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/metadata/struct.go`
- Modify: `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/query/structured/query_ts.go`
- Modify: `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/metadata/native_vm_influx.go`
- Update tests under `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/query/structured` and `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/metadata`.

- [ ] **Step 1: Extend metadata Query**

Add fields to `metadata.Query`:

```go
NativeVMAddress        string
NativeVMSelectUsername string
NativeVMSelectPassword string
```

Add fields to `metadata.NativeVMInfluxExpand`:

```go
Address        string
SelectUsername string
SelectPassword string
```

- [ ] **Step 2: Mark native VM only from readable matching VM cluster**

In `QueryTs.ToQueryMetric`, replace the current `tsdb.GetStorage(...).NativeVMInflux` gate with:

```go
if query.StorageID != consul.OfflineDataArchive {
	if vmCluster, ok := consul.GetCachedVMClusterInfo(clusterName); ok && vmCluster.Readable {
		if vmCluster.Select.Address != "" && vmCluster.Select.APIPrefix != "" {
			query.NativeVMInflux = true
			query.NativeVMAddress = vmCluster.Select.Address
			query.NativeVMAPIPrefix = vmCluster.Select.APIPrefix
			query.NativeVMSelectUsername = vmCluster.Select.BasicAuth.Username
			query.NativeVMSelectPassword = vmCluster.Select.BasicAuth.Password
			query.NativeVMDBLabel = vmCluster.InfluxCompat.DBLabel
			query.NativeVMMeasurementFieldSeparator = vmCluster.InfluxCompat.MeasurementFieldSeparator
			query.NativeVMSkipSingleField = vmCluster.InfluxCompat.SkipSingleField
			query.NativeVMMatchers = append(query.NativeVMMatchers, queryLabelsMatcher...)
		}
	}
}
```

If `DBLabel` or `MeasurementFieldSeparator` is empty, normalize to `db` and `_`.

- [ ] **Step 3: Check backend identity**

In `CheckNativeVMInfluxQuery`, compare all native references by:

```go
type backendIdentity struct {
	Address  string
	Prefix   string
	Username string
}
```

Reject multiple identities:

```go
if expand.Address != "" && query.NativeVMAddress != "" && expand.Address != query.NativeVMAddress {
	return false, expand, fmt.Errorf("native vm influx query does not support multiple backends: %s%s, %s%s",
		expand.Address, expand.APIPrefix, query.NativeVMAddress, query.NativeVMAPIPrefix)
}
```

Do not print passwords in errors or trace fields.

### Task 5: Query Native VM With Select Address And Basic Auth

**Files:**
- Modify: `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/tsdb/prometheus/querier.go`
- Modify: `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/tsdb/victoriaMetricsInstance/instance.go`
- Update: `/Users/lvzeli/Projects/kingeye4/bkmonitor-datalink/pkg/unify-query/tsdb/victoriaMetricsInstance/instance_test.go`

- [ ] **Step 1: Create instance from query-level backend**

In `prometheus.GetInstance`, when `qry.NativeVMInflux` is true, use query fields:

```go
if qry.NativeVMInflux {
	return victoriaMetricsInstance.NewInstanceWithAPIPrefixAndBasicAuth(
		ctx,
		qry.NativeVMAddress,
		qry.NativeVMAPIPrefix,
		qry.NativeVMSelectUsername,
		qry.NativeVMSelectPassword,
		storage.Timeout,
		curl,
	)
}
```

Storage still provides timeout/curl defaults; it no longer decides whether the query is native VM.

- [ ] **Step 2: Add basic auth support**

Add fields to `victoriaMetricsInstance.Instance`:

```go
	username string
	password string
```

Add constructor:

```go
func NewInstanceWithAPIPrefixAndBasicAuth(ctx context.Context, address, apiPrefix, username, password string, timeout time.Duration, curl curl.Curl) *Instance {
	return &Instance{
		ctx:       ctx,
		address:   address,
		apiPrefix: apiPrefix,
		username:  username,
		password:  password,
		timeout:   timeout,
		curl:      curl,
	}
}
```

When building requests, set basic auth only when username is not empty:

```go
if i.username != "" {
	req.SetBasicAuth(i.username, i.password)
}
```

### Task 6: Verification

**Files:**
- Test only; no source changes unless failures are caused by this plan.

- [ ] **Step 1: Run gf targeted tests**

Run:

```bash
cd /Users/lvzeli/Projects/kingeye4/bk-monitor-gf
pytest metadata/tests/test_vmcluster_info.py -q
```

Expected: the new VMClusterInfo payload test passes. If the local gf test environment is unavailable, record the exact import or database setup error.

- [ ] **Step 2: Run datalink targeted tests**

Run:

```bash
cd /Users/lvzeli/Projects/kingeye4/bkmonitor-datalink
go test ./pkg/unify-query/consul -run TestFormatVMClusterInfo -count=1
go test ./pkg/unify-query/metadata -run TestCheckNativeVMInfluxQuery -count=1
go test ./pkg/unify-query/query/structured -run 'TestNativeVMInflux' -count=1
go test ./pkg/unify-query/tsdb/victoriaMetricsInstance -run 'TestInstanceQueryRangeUsesNativeVMSelectPrefix|TestInstanceQueryUsesNativeVMSelectPrefix' -count=1
go test ./pkg/unify-query/service/http -run TestQueryTsNativeVMInfluxUsesInfluxMetadataMetricQL -count=1
```

Expected: targeted tests pass. Full repository tests may still fail on unrelated existing failures and should be reported separately.

---

## Self-Review

- Spec coverage: The plan covers gf model/migration/admin/refresh, Consul key shape, datalink Consul cache, native query switching, backend identity checks, select basic auth, and targeted tests.
- Placeholder scan: No placeholder task is left; every task lists concrete files and code snippets.
- Type consistency: The Consul JSON keys are consistently `select`, `insert`, `influx_compat`, `api_prefix`, and `basic_auth`; Go struct names match those keys.
