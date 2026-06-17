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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shirou/gopsutil/v3/net"
	oleltrace "go.opentelemetry.io/otel/trace"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/log"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metadata"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/metric"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/unify-query/trace"
)

// Params
type Params struct {
	SlowQueryThreshold time.Duration
}

var (
	once              sync.Once
	instancedIP       string
	getInstanceipFunc = getInstanceip
)

// get instance ip
func getInstanceip() (string, error) {
	interfaceStatList, err := net.Interfaces()
	if err != nil {
		log.Errorf(context.TODO(), "failed to get instance ip,error:%v", err)
		return "", err
	}
	addrList := interfaceStatList[len(interfaceStatList)-1]
	if len(addrList.Addrs) > 0 {
		return addrList.Addrs[len(addrList.Addrs)-1].Addr, nil
	}
	return "", nil
}

// get instance ip single pass
func singleGetInstance() string {
	once.Do(func() {
		instancedIP, _ = getInstanceipFunc()
	})
	return instancedIP
}

// Timer 进行请求处理时间记录
func Timer(p *Params) gin.HandlerFunc {
	return func(c *gin.Context) {
		var (
			ctx         = c.Request.Context()
			span        oleltrace.Span
			start       = time.Now()
			instanceIP  = singleGetInstance()
			source      = c.Request.Header.Get(metadata.BkQuerySourceHeader)
			spaceUid    = c.Request.Header.Get(metadata.SpaceUIDHeader)
			requestBody []byte
		)
		ctx, span = trace.IntoContext(ctx, trace.TracerName, "http-api")

		var err error
		requestBody, err = readAndRestoreRequestBody(c.Request)
		if err != nil {
			log.Errorf(ctx, "read http request body for access log failed: %s", err)
		}

		// 把用户名注入到 metadata 中
		metadata.SetUser(ctx, source, spaceUid)

		metric.APIRequestInc(ctx, c.Request.URL.Path, metric.StatusReceived, spaceUid)

		if span != nil {
			defer func() {
				trace.InsertStringIntoSpan("instance-ip", instanceIP, span)

				sub := time.Since(start)
				metric.APIRequestSecond(ctx, sub, c.Request.URL.Path, spaceUid)

				// 记录慢查询
				if p.SlowQueryThreshold > 0 && sub.Milliseconds() > p.SlowQueryThreshold.Milliseconds() {
					log.Errorf(ctx,
						fmt.Sprintf(
							"slow query log request: %s, duration: %s",
							c.Request.URL.Path, sub.String(),
						),
					)
				}
				trace.InsertIntIntoSpan("http-api-query-cost", int(sub.Milliseconds()), span)

				status := metadata.GetStatus(ctx)
				var statusCode, statusMessage string
				if status != nil {
					statusCode = status.Code
					statusMessage = status.Message
					trace.InsertStringIntoSpan("http-api-status-code", status.Code, span)
					trace.InsertStringIntoSpan("http-api-status-message", status.Message, span)
				}

				log.Infof(
					ctx,
					"http request completed method=%s path=%s status=%d duration=%s space_uid=%s source=%s client_ip=%s body_size=%d body=%q status_code=%s status_message=%s",
					c.Request.Method,
					c.Request.URL.Path,
					c.Writer.Status(),
					sub.String(),
					spaceUid,
					source,
					c.ClientIP(),
					len(requestBody),
					string(requestBody),
					statusCode,
					statusMessage,
				)

				span.End()
			}()
		}

		c.Next()
	}
}

func readAndRestoreRequestBody(req *http.Request) ([]byte, error) {
	if req == nil || req.Body == nil {
		return nil, nil
	}

	body, err := io.ReadAll(req.Body)
	req.Body = io.NopCloser(bytes.NewBuffer(body))
	return body, err
}
