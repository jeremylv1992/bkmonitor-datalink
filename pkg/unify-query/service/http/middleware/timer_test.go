// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
// Copyright (C) 2022 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleGetInstance(t *testing.T) {
	oldOnce := once
	oldInstancedIP := instancedIP
	oldGetInstanceipFunc := getInstanceipFunc
	defer func() {
		once = oldOnce
		instancedIP = oldInstancedIP
		getInstanceipFunc = oldGetInstanceipFunc
	}()

	var calls int
	once = sync.Once{}
	instancedIP = ""
	getInstanceipFunc = func() (string, error) {
		calls++
		return "127.0.0.1", nil
	}

	assert.Equal(t, "127.0.0.1", singleGetInstance())
	assert.Equal(t, "127.0.0.1", singleGetInstance())
	assert.Equal(t, 1, calls)
}

func TestReadAndRestoreRequestBody(t *testing.T) {
	const body = `{"query_list":[{"reference_name":"a"}]}`

	req := httptest.NewRequest(http.MethodPost, "/query/ts", strings.NewReader(body))
	got, err := readAndRestoreRequestBody(req)
	require.NoError(t, err)
	assert.Equal(t, body, string(got))

	restored, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	assert.Equal(t, body, string(restored))
}
