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

// VMBasicAuth is the optional basic auth used by a VM endpoint.
type VMBasicAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// VMEndpoint describes one VM HTTP endpoint, e.g. vmselect or vminsert.
type VMEndpoint struct {
	Address   string      `json:"address"`
	APIPrefix string      `json:"api_prefix"`
	BasicAuth VMBasicAuth `json:"basic_auth"`
}

// VMInfluxCompat describes how VM stores Influx line-protocol metadata.
type VMInfluxCompat struct {
	DBLabel                   string `json:"db_label"`
	MeasurementFieldSeparator string `json:"measurement_field_separator"`
	SkipSingleField           bool   `json:"skip_single_field"`
}

// VMClusterInfo is the native VM backend metadata published by metadata service.
type VMClusterInfo struct {
	ClusterName  string                 `json:"cluster_name"`
	Readable     bool                   `json:"readable"`
	Writable     bool                   `json:"writable"`
	Select       VMEndpoint             `json:"select"`
	Insert       VMEndpoint             `json:"insert"`
	InfluxCompat VMInfluxCompat         `json:"influx_compat"`
	CustomOption map[string]interface{} `json:"custom_option"`
}

func formatVMClusterInfoPath() string {
	return fmt.Sprintf("%s/%s/%s", basePath, dataPath, vmClusterInfoPath)
}

func watchVMClusterInfoPath() string {
	return fmt.Sprintf("%s/%s/%s", basePath, versionPath, vmClusterInfoPath)
}

// FormatVMClusterInfo formats raw Consul KVs into a cluster_name keyed map.
func FormatVMClusterInfo(kvPairs api.KVPairs) (map[string]*VMClusterInfo, error) {
	result := make(map[string]*VMClusterInfo)
	prefix := fmt.Sprintf("%s/", formatVMClusterInfoPath())
	for _, kvPair := range kvPairs {
		var data VMClusterInfo
		if err := json.Unmarshal(kvPair.Value, &data); err != nil {
			return nil, err
		}
		key := strings.TrimPrefix(kvPair.Key, prefix)
		if data.ClusterName == "" {
			data.ClusterName = key
		}
		result[key] = &data
	}
	return result, nil
}

// GetVMClusterInfo gets all VM cluster metadata from Consul.
func GetVMClusterInfo() (map[string]*VMClusterInfo, error) {
	pairs, err := GetDataWithPrefix(formatVMClusterInfoPath())
	if err != nil {
		return nil, err
	}
	return FormatVMClusterInfo(pairs)
}

// ReloadVMClusterInfo reloads VM cluster metadata into the local cache.
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

// WatchVMClusterInfo watches VM cluster metadata version changes.
func WatchVMClusterInfo(ctx context.Context) (<-chan interface{}, error) {
	return WatchChange(ctx, watchVMClusterInfoPath())
}

// GetCachedVMClusterInfo returns one cached VM cluster metadata item.
func GetCachedVMClusterInfo(clusterName string) (*VMClusterInfo, bool) {
	vmClusterInfoLock.RLock()
	defer vmClusterInfoLock.RUnlock()
	info, ok := vmClusterInfo[clusterName]
	return info, ok
}

// GetCachedVMClusterInfoSnapshot returns a redacted copy of the cached VM cluster metadata.
func GetCachedVMClusterInfoSnapshot() map[string]*VMClusterInfo {
	vmClusterInfoLock.RLock()
	defer vmClusterInfoLock.RUnlock()

	result := make(map[string]*VMClusterInfo, len(vmClusterInfo))
	for name, info := range vmClusterInfo {
		if info == nil {
			result[name] = nil
			continue
		}
		copied := *info
		copied.Select.BasicAuth.Password = ""
		copied.Insert.BasicAuth.Password = ""
		if info.CustomOption != nil {
			copied.CustomOption = make(map[string]interface{}, len(info.CustomOption))
			for k, v := range info.CustomOption {
				copied.CustomOption[k] = v
			}
		}
		result[name] = &copied
	}
	return result
}
