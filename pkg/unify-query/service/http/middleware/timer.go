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
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

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
	once     sync.Once
	localIPs []string
)

// get instance ip single pass
func getIPs() []string {
	once.Do(func() {
		interfaces, _ := net.Interfaces()
		for _, i := range interfaces {
			adders, err := i.Addrs()
			if err != nil {
				continue
			}

			for _, addr := range adders {
				var ip net.IP
				switch v := addr.(type) {
				case *net.IPNet:
					ip = v.IP
				case *net.IPAddr:
					ip = v.IP
				}

				if ip == nil || ip.IsLoopback() {
					continue
				}
				ip = ip.To4()
				if ip == nil {
					continue
				}
				localIPs = append(localIPs, ip.String())
			}
		}
	})
	return localIPs
}

// Timer 进行请求处理时间记录
func Timer(p *Params) gin.HandlerFunc {
	return func(c *gin.Context) {
		var (
			start     = time.Now()
			ips       = getIPs()
			source    = c.Request.Header.Get(metadata.BkQuerySourceHeader)
			spaceUid  = c.Request.Header.Get(metadata.SpaceUIDHeader)
			skipSpace = c.Request.Header.Get(metadata.SkipSpaceHeader)
			ctx       = c.Request.Context()
			err       error
		)

		ctx = metadata.InitHashID(ctx)
		c.Request = c.Request.WithContext(ctx)

		ctx, span := trace.NewSpan(ctx, "http-api")

		// 把用户名注入到 metadata 中
		metadata.SetUser(ctx, source, spaceUid, skipSpace)

		metric.APIRequestInc(ctx, c.Request.URL.Path, metric.StatusReceived, spaceUid)

		if span != nil {
			defer func() {

				span.Set("local-ips", ips)

				sub := time.Since(start)
				metric.APIRequestSecond(ctx, sub, c.Request.URL.Path, spaceUid)

				// 记录慢查询
				if p.SlowQueryThreshold > 0 && sub.Milliseconds() > p.SlowQueryThreshold.Milliseconds() {
					log.Warnf(ctx,
						fmt.Sprintf(
							"slow query log request: %s, duration: %s",
							c.Request.URL.Path, sub.String(),
						),
					)
				}
				span.Set("http-api-query-cost", int(sub.Milliseconds()))

				status := metadata.GetStatus(ctx)
				if status != nil {
					span.Set("http-api-status-code", status.Code)
					span.Set("http-api-status-message", status.Message)
				}

				span.End(&err)
			}()
		}

		c.Next()
	}
}
