// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

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
			Key: "bkmonitorv3/unify-query/data/vmcluster_info/cluster-a",
			Value: []byte(`{
				"cluster_name": "cluster-a",
				"readable": true,
				"writable": true,
				"select": {
					"address": "http://vmselect:8481",
					"api_prefix": "/select/0/prometheus/api/v1",
					"basic_auth": {"username": "query", "password": "secret"}
				},
				"insert": {
					"address": "http://vminsert:8480",
					"api_prefix": "/insert/0/influx",
					"basic_auth": {"username": "write", "password": "secret"}
				},
				"influx_compat": {
					"db_label": "db",
					"measurement_field_separator": "_",
					"skip_single_field": false
				},
				"custom_option": {"owner": "metadata"}
			}`),
		},
	})

	require.NoError(t, err)
	require.Contains(t, infos, "cluster-a")
	assert.True(t, infos["cluster-a"].Readable)
	assert.Equal(t, "cluster-a", infos["cluster-a"].ClusterName)
	assert.True(t, infos["cluster-a"].Writable)
	assert.Equal(t, "http://vmselect:8481", infos["cluster-a"].Select.Address)
	assert.Equal(t, "/select/0/prometheus/api/v1", infos["cluster-a"].Select.APIPrefix)
	assert.Equal(t, "query", infos["cluster-a"].Select.BasicAuth.Username)
	assert.Equal(t, "http://vminsert:8480", infos["cluster-a"].Insert.Address)
	assert.Equal(t, "/insert/0/influx", infos["cluster-a"].Insert.APIPrefix)
	assert.Equal(t, "db", infos["cluster-a"].InfluxCompat.DBLabel)
	assert.Equal(t, "_", infos["cluster-a"].InfluxCompat.MeasurementFieldSeparator)
	assert.False(t, infos["cluster-a"].InfluxCompat.SkipSingleField)
	assert.Equal(t, "metadata", infos["cluster-a"].CustomOption["owner"])
}

func TestFormatVMClusterInfoUsesKeyAsClusterName(t *testing.T) {
	infos, err := FormatVMClusterInfo(api.KVPairs{
		{
			Key: "bkmonitorv3/unify-query/data/vmcluster_info/cluster-a",
			Value: []byte(`{
				"readable": true,
				"select": {"address": "http://vmselect:8481", "api_prefix": "/select/0/prometheus/api/v1"}
			}`),
		},
	})

	require.NoError(t, err)
	require.Contains(t, infos, "cluster-a")
	assert.Equal(t, "cluster-a", infos["cluster-a"].ClusterName)
}

func TestGetCachedVMClusterInfoSnapshotRedactsPassword(t *testing.T) {
	vmClusterInfoLock.Lock()
	oldInfo := vmClusterInfo
	oldHash := vmClusterInfoHash
	vmClusterInfo = map[string]*VMClusterInfo{
		"cluster-a": {
			ClusterName: "cluster-a",
			Readable:    true,
			Select: VMEndpoint{
				Address:   "http://vmselect:8481",
				APIPrefix: "/select/0/prometheus/api/v1",
				BasicAuth: VMBasicAuth{Username: "reader", Password: "secret"},
			},
			Insert: VMEndpoint{
				Address:   "http://vminsert:8480",
				APIPrefix: "/insert/0/influx",
				BasicAuth: VMBasicAuth{Username: "writer", Password: "secret"},
			},
			CustomOption: map[string]interface{}{"owner": "metadata"},
		},
	}
	vmClusterInfoHash = "test"
	vmClusterInfoLock.Unlock()
	t.Cleanup(func() {
		vmClusterInfoLock.Lock()
		defer vmClusterInfoLock.Unlock()
		vmClusterInfo = oldInfo
		vmClusterInfoHash = oldHash
	})

	snapshot := GetCachedVMClusterInfoSnapshot()

	require.Contains(t, snapshot, "cluster-a")
	assert.Equal(t, "reader", snapshot["cluster-a"].Select.BasicAuth.Username)
	assert.Empty(t, snapshot["cluster-a"].Select.BasicAuth.Password)
	assert.Equal(t, "writer", snapshot["cluster-a"].Insert.BasicAuth.Username)
	assert.Empty(t, snapshot["cluster-a"].Insert.BasicAuth.Password)

	snapshot["cluster-a"].Select.Address = "changed"
	snapshot["cluster-a"].CustomOption["owner"] = "changed"
	info, ok := GetCachedVMClusterInfo("cluster-a")
	require.True(t, ok)
	assert.Equal(t, "http://vmselect:8481", info.Select.Address)
	assert.Equal(t, "metadata", info.CustomOption["owner"])
}
