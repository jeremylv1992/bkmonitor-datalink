// Tencent is pleased to support the open source community by making
// 蓝鲸智云 - 日志平台 (BlueKing - Log) available.
// Copyright (C) 2017-2021 THL A29 Limited, a Tencent company. All rights reserved.
// Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://opensource.org/licenses/MIT
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
//

package controllers

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PodContainerIDIndexField Pod 缓存按容器 ID 建立的索引名
const PodContainerIDIndexField = "status.containerStatuses.containerID"

// PodContainerIDIndexFunc 提取 Pod 状态中全部容器（含 init 与临时容器）的 ID，供缓存按容器 ID 反查 Pod
func PodContainerIDIndexFunc(obj client.Object) []string {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}

	var containerIDs []string
	for _, statuses := range [][]corev1.ContainerStatus{
		pod.Status.InitContainerStatuses,
		pod.Status.ContainerStatuses,
		pod.Status.EphemeralContainerStatuses,
	} {
		for _, status := range statuses {
			containerID := trimContainerIDScheme(status.ContainerID)
			if containerID != "" {
				containerIDs = append(containerIDs, containerID)
			}
		}
	}
	return containerIDs
}

// trimContainerIDScheme 去掉 Pod 状态中容器 ID 的运行时前缀，如 containerd://<id> -> <id>
func trimContainerIDScheme(containerID string) string {
	if _, id, found := strings.Cut(containerID, "://"); found {
		return id
	}
	return containerID
}
