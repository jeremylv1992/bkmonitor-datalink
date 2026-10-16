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
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/bk-log-sidecar/api/bk.tencent.com/v1alpha1"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/bk-log-sidecar/config"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/bk-log-sidecar/define"
)

const (
	testContainerID      = "998b712d133dc59d52dbf34a3afcb5fb"
	testVirtualNamespace = "kingeye"
	testVirtualPodName   = "web-7db96554cb-7wzbk"
	testHostNamespace    = "test-vcluster"
	testHostPodName      = "web-7db96554cb-7wzbk-x-kingeye-x-test-vcluster"
)

// readerCache 用 client.Reader 充当 BkLogSidecar 依赖的 cache.Cache，sidecar 只调用 Get / List
type readerCache struct {
	cache.Cache
	reader client.Reader
}

func (c readerCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.reader.Get(ctx, key, obj, opts...)
}

func (c readerCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.reader.List(ctx, list, opts...)
}

// stubRuntime 只提供运行时类型，避免单测连接真实容器运行时
type stubRuntime struct{}

func (stubRuntime) Containers(context.Context) ([]define.SimpleContainer, error) { return nil, nil }

func (stubRuntime) Inspect(context.Context, string) (define.Container, error) {
	return define.Container{}, nil
}

func (stubRuntime) Subscribe(context.Context) (<-chan *define.ContainerEvent, <-chan error) {
	return nil, nil
}

func (stubRuntime) Type() define.RuntimeType { return define.RuntimeTypeContainerd }

func newTestSidecar(t *testing.T, objs ...client.Object) (*BkLogSidecar, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, PodContainerIDIndexField, PodContainerIDIndexFunc).
		Build()

	sidecar := &BkLogSidecar{
		log:        ctrl.Log.WithName("bkLogSidecar"),
		kubeClient: readerCache{reader: fakeClient},
		runtime:    stubRuntime{},
		stopCh:     make(chan struct{}),
	}
	t.Cleanup(func() { close(sidecar.stopCh) })
	return sidecar, fakeClient
}

// newPod 构造 Pod，containerID 为空表示容器 ID 尚未写入 Pod 状态
func newPod(namespace, name, containerID string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "web:1.0"}}},
	}
	if containerID != "" {
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "web", ContainerID: "containerd://" + containerID}}
	}
	return pod
}

func newStdLogConfig(namespace string) *v1alpha1.BkLogConfig {
	return &v1alpha1.BkLogConfig{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "std-log"},
		Spec: v1alpha1.BkLogConfigSpec{
			DataId:            1573770,
			LogConfigType:     config.StdLogConfig,
			AllContainer:      true,
			NamespaceSelector: v1alpha1.NamespaceSelector{MatchNames: []string{namespace}},
		},
	}
}

// newContainer 构造运行时上的容器，标签由宿主 kubelet 写入
func newContainer(podNamespace, podName string) *define.Container {
	return &define.Container{
		ID: testContainerID,
		Labels: map[string]string{
			config.ContainerLabelK8sPodNamespace:  podNamespace,
			config.ContainerLabelK8sPodName:       podName,
			config.ContainerLabelK8sContainerName: "web",
		},
		LogPath: "/var/log/pods/test-vcluster_" + podName + "_uid/web/0.log",
	}
}

func TestPodContainerIDIndexFunc(t *testing.T) {
	pod := &corev1.Pod{Status: corev1.PodStatus{
		InitContainerStatuses: []corev1.ContainerStatus{{Name: "init", ContainerID: "containerd://init-id"}},
		ContainerStatuses: []corev1.ContainerStatus{
			{Name: "web", ContainerID: "docker://web-id"},
			{Name: "waiting", ContainerID: ""},
		},
		EphemeralContainerStatuses: []corev1.ContainerStatus{{Name: "debug", ContainerID: "debug-id"}},
	}}

	assert.Equal(t, []string{"init-id", "web-id", "debug-id"}, PodContainerIDIndexFunc(pod))
	assert.Nil(t, PodContainerIDIndexFunc(&corev1.Node{}))
}

func TestMatchBklogConfigsByPodName(t *testing.T) {
	sidecar, _ := newTestSidecar(t,
		newPod(testVirtualNamespace, testVirtualPodName, testContainerID),
		newStdLogConfig(testVirtualNamespace),
	)

	configs, pod, podNotFound := sidecar.matchBklogConfigs(newContainer(testVirtualNamespace, testVirtualPodName))

	require.Len(t, configs, 1)
	assert.Equal(t, testVirtualPodName, pod.Name)
	assert.False(t, podNotFound)
}

func TestMatchBklogConfigsByContainerIDInVcluster(t *testing.T) {
	sidecar, _ := newTestSidecar(t,
		newPod(testVirtualNamespace, testVirtualPodName, testContainerID),
		newStdLogConfig(testVirtualNamespace),
	)

	configs, pod, podNotFound := sidecar.matchBklogConfigs(newContainer(testHostNamespace, testHostPodName))

	require.Len(t, configs, 1)
	assert.Equal(t, testVirtualNamespace, pod.Namespace)
	assert.Equal(t, testVirtualPodName, pod.Name)
	assert.False(t, podNotFound)
}

func TestMatchBklogConfigsPodStatusNotSynced(t *testing.T) {
	sidecar, _ := newTestSidecar(t,
		newPod(testVirtualNamespace, testVirtualPodName, ""),
		newStdLogConfig(testVirtualNamespace),
	)

	configs, _, podNotFound := sidecar.matchBklogConfigs(newContainer(testHostNamespace, testHostPodName))

	assert.Empty(t, configs)
	assert.True(t, podNotFound)
}

func TestMatchBklogConfigsSkipsNonPodContainer(t *testing.T) {
	sidecar, _ := newTestSidecar(t, newStdLogConfig(testVirtualNamespace))

	nonK8sContainer := &define.Container{ID: testContainerID, Labels: map[string]string{}}
	configs, _, podNotFound := sidecar.matchBklogConfigs(nonK8sContainer)
	assert.Empty(t, configs)
	assert.False(t, podNotFound)

	sandboxContainer := newContainer(testHostNamespace, testHostPodName)
	sandboxContainer.Labels[config.ContainerLabelK8sContainerName] = "POD"
	configs, _, podNotFound = sidecar.matchBklogConfigs(sandboxContainer)
	assert.Empty(t, configs)
	assert.False(t, podNotFound)
}

// setPodMatchRetryDelays 替换重试间隔并在用例结束后恢复
func setPodMatchRetryDelays(t *testing.T, delays []time.Duration) {
	t.Helper()
	origin := PodMatchRetryDelays
	PodMatchRetryDelays = delays
	t.Cleanup(func() { PodMatchRetryDelays = origin })
}

// setBkunifylogbeatConfigDir 把采集配置输出目录指向临时目录并在用例结束后恢复
func setBkunifylogbeatConfigDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	origin := config.BkunifylogbeatConfig
	config.BkunifylogbeatConfig = dir
	t.Cleanup(func() { config.BkunifylogbeatConfig = origin })
	return dir
}

func waitRetry(t *testing.T, sidecar *BkLogSidecar, event *define.ContainerEvent, beforeWait func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		sidecar.retryStartedContainer(event)
		close(done)
	}()
	beforeWait()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("retryStartedContainer not finished")
	}
}

func TestRetryStartedContainerAfterPodStatusSynced(t *testing.T) {
	setPodMatchRetryDelays(t, []time.Duration{20 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond})
	configDir := setBkunifylogbeatConfigDir(t)

	pod := newPod(testVirtualNamespace, testVirtualPodName, "")
	sidecar, fakeClient := newTestSidecar(t, pod, newStdLogConfig(testVirtualNamespace))
	container := newContainer(testHostNamespace, testHostPodName)
	sidecar.containerCache.Store(container.ID, container)
	event := &define.ContainerEvent{ContainerID: container.ID, Type: define.ContainerEventCreate}

	require.True(t, sidecar.applyStartedContainer(event), "pod status not synced yet, should retry")

	// 模拟 vcluster 在容器启动后才把容器 ID 回写到 Pod 状态
	waitRetry(t, sidecar, event, func() {
		var current corev1.Pod
		require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKeyFromObject(pod), &current))
		current.Status = newPod(testVirtualNamespace, testVirtualPodName, testContainerID).Status
		require.NoError(t, fakeClient.Update(context.Background(), &current))
	})

	configName := testContainerID + "_" + config.StdLogConfig + "_default_std-log"
	_, ok := sidecar.actualBkLogConfigCache.Load(configName)
	assert.True(t, ok)
	content, err := os.ReadFile(filepath.Join(configDir, configName+".conf"))
	require.NoError(t, err)
	assert.NotEmpty(t, content)
}

func TestRetryStartedContainerGivesUp(t *testing.T) {
	setPodMatchRetryDelays(t, []time.Duration{time.Millisecond, time.Millisecond})
	setBkunifylogbeatConfigDir(t)

	sidecar, _ := newTestSidecar(t,
		newPod(testVirtualNamespace, testVirtualPodName, ""),
		newStdLogConfig(testVirtualNamespace),
	)
	container := newContainer(testHostNamespace, testHostPodName)
	sidecar.containerCache.Store(container.ID, container)
	event := &define.ContainerEvent{ContainerID: container.ID, Type: define.ContainerEventCreate}

	waitRetry(t, sidecar, event, func() {})

	empty := true
	sidecar.actualBkLogConfigCache.Range(func(_, _ interface{}) bool {
		empty = false
		return false
	})
	assert.True(t, empty)
}

func TestApplyStartedContainerWithoutRetry(t *testing.T) {
	setBkunifylogbeatConfigDir(t)
	sidecar, _ := newTestSidecar(t,
		newPod(testVirtualNamespace, testVirtualPodName, testContainerID),
		newStdLogConfig(testVirtualNamespace),
	)

	unknown := &define.ContainerEvent{ContainerID: "unknown", Type: define.ContainerEventCreate}
	sidecar.runtime = failingInspectRuntime{}
	assert.False(t, sidecar.applyStartedContainer(unknown), "container not exists, should not retry")

	container := newContainer(testHostNamespace, testHostPodName)
	sidecar.containerCache.Store(container.ID, container)
	sidecar.runtime = stubRuntime{}
	event := &define.ContainerEvent{ContainerID: container.ID, Type: define.ContainerEventCreate}
	assert.False(t, sidecar.applyStartedContainer(event), "pod matched by container id, should not retry")
}

// failingInspectRuntime 模拟运行时查不到容器
type failingInspectRuntime struct{ stubRuntime }

func (failingInspectRuntime) Inspect(context.Context, string) (define.Container, error) {
	return define.Container{}, os.ErrNotExist
}
