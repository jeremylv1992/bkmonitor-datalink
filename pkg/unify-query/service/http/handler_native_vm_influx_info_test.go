// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/consul"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/mock"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/infos"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/query/structured"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/tsdb"
)

func TestQueryInfoNativeVMInfluxTagKeysUsesLabelsAPI(t *testing.T) {
	ctx := context.Background()
	selector := `{__name__="cpu_detail_usage",db="system",bk_biz_id="2"}`
	params := setupNativeVMInfoTest(ctx, t, func(w http.ResponseWriter, r *http.Request) {
		assertNativeVMInfoRequest(t, r, "/select/0/prometheus/api/v1/labels", selector)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":["db","bk_biz_id","__name__"]}`)
	})

	data, err := queryInfo(ctx, infos.TagKeys, params)
	require.NoError(t, err)
	assert.Equal(t, []string{"__name__", "bk_biz_id", "db"}, data)
}

func TestQueryInfoNativeVMInfluxTagValuesUsesSeriesAPI(t *testing.T) {
	ctx := context.Background()
	selector := `{__name__="cpu_detail_usage",db="system",bk_biz_id="2"}`
	params := setupNativeVMInfoTest(ctx, t, func(w http.ResponseWriter, r *http.Request) {
		assertNativeVMInfoRequest(t, r, "/select/0/prometheus/api/v1/series", selector)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":[{"__name__":"cpu_detail_usage","db":"system","bk_biz_id":"3"},{"__name__":"cpu_detail_usage","db":"system","bk_biz_id":"2"}]}`)
	})

	data, err := queryInfo(ctx, infos.TagValues, params)
	require.NoError(t, err)
	actual, ok := data.(TagValuesData)
	require.True(t, ok)
	assert.Equal(t, map[string][]string{"bk_biz_id": {"2", "3"}}, actual.Values)
}

func TestQueryInfoNativeVMInfluxSeriesUsesSeriesAPI(t *testing.T) {
	ctx := context.Background()
	selector := `{__name__="cpu_detail_usage",db="system",bk_biz_id="2"}`
	params := setupNativeVMInfoTest(ctx, t, func(w http.ResponseWriter, r *http.Request) {
		assertNativeVMInfoRequest(t, r, "/select/0/prometheus/api/v1/series", selector)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success","data":[{"__name__":"cpu_detail_usage","db":"system","bk_biz_id":"2","host":"node-a"},{"__name__":"cpu_detail_usage","db":"system","bk_biz_id":"3","host":"node-b"}]}`)
	})
	params.Keys = []string{"bk_biz_id", "host"}

	data, err := queryInfo(ctx, infos.Series, params)
	require.NoError(t, err)
	actual, ok := data.([]*SeriesData)
	require.True(t, ok)
	require.Len(t, actual, 1)
	assert.Equal(t, []string{"bk_biz_id", "host"}, actual[0].Keys)
	assert.Equal(t, [][]string{{"2", "node-a"}, {"3", "node-b"}}, actual[0].Series)
}

func setupNativeVMInfoTest(
	ctx context.Context, t *testing.T, handler http.HandlerFunc,
) *infos.Params {
	t.Helper()

	log.InitTestLogger()
	mock.SetOfflineDataArchiveMetadata(&emptyArchiveMetadata{})
	metadata.SetUser(ctx, "native_vm_info_test:native_vm_info_test", "native-vm-info-space")

	vmselect := httptest.NewServer(handler)
	t.Cleanup(vmselect.Close)
	reloadTestNativeVMClusterInfo(t, "cluster-a", vmselect.URL)

	storageID := "native-vm-info-storage"
	tsdb.SetStorage(storageID, &tsdb.Storage{
		Type:    consul.InfluxDBStorageType,
		Address: "http://influxdb-proxy:8080",
		Timeout: time.Minute,
	})

	tableID := "system.cpu_detail"
	mockNativeVMInfluxTables(ctx, "native-vm-info-space", storageID, map[string]nativeVMInfluxTable{
		tableID: {
			field:       "usage",
			db:          "system",
			measurement: "cpu_detail",
		},
	})

	return &infos.Params{
		TableID: structured.TableID(tableID),
		Metric:  "usage",
		Keys:    []string{"bk_biz_id"},
		Start:   "1677081600",
		End:     "1677081660",
		Conditions: structured.Conditions{
			FieldList: []structured.ConditionField{
				{
					DimensionName: "bk_biz_id",
					Operator:      structured.ConditionEqual,
					Value:         []string{"2"},
				},
			},
		},
	}
}

func assertNativeVMInfoRequest(t *testing.T, r *http.Request, path, selector string) {
	t.Helper()

	assert.Equal(t, path, r.URL.Path)
	assert.Equal(t, selector, r.URL.Query().Get("match[]"))
	assert.Equal(t, "1677081600", r.URL.Query().Get("start"))
	assert.Equal(t, "1677081660", r.URL.Query().Get("end"))
	username, password, ok := r.BasicAuth()
	require.True(t, ok)
	assert.Equal(t, "query", username)
	assert.Equal(t, "secret", password)
}
