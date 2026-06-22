// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package tsdb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
)

func TestReloadTsDBStorageIgnoresNativeVMInfluxOptions(t *testing.T) {
	err := ReloadTsDBStorage(context.Background(), map[string]*consul.Storage{
		"native-vm-influx": {
			Type:    consul.InfluxDBStorageType,
			Address: "http://vmselect:8481",
			Options: map[string]string{
				"native_vm_influx":                      "true",
				"native_vm_tenant":                      "42",
				"native_vm_api_prefix":                  "/select/42/prometheus/api/v1",
				"native_vm_db_label":                    "db",
				"native_vm_measurement_field_separator": "_",
				"native_vm_skip_single_field":           "true",
			},
		},
	}, &Options{
		VM: &VMOption{
			UriPath: "unused",
			Timeout: time.Second,
		},
		InfluxDB: &InfluxDBOption{
			Timeout:    time.Minute,
			RawUriPath: "api/v1/raw/read",
		},
	})
	require.NoError(t, err)

	storage, err := GetStorage("native-vm-influx")
	require.NoError(t, err)
	assert.Equal(t, consul.InfluxDBStorageType, storage.Type)
	assert.Equal(t, "http://vmselect:8481", storage.Address)

	printed := Print()
	assert.NotContains(t, printed, "NativeVMInflux")
	assert.NotContains(t, printed, "NativeVMAPIPrefix")
	assert.NotContains(t, printed, "NativeVMDBLabel")
}
