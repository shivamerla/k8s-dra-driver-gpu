/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package fakeslices builds the ResourceSlices that the kubelet plugins would
// publish for fake (KWOK) nodes, so that the scheduler and the
// compute-domain-controller see the same devices as on GB200 nodes. A test in
// cmd/compute-domain-kubelet-plugin keeps the ComputeDomain devices identical
// to what the plugin publishes.
package fakeslices

import (
	"fmt"
	"slices"
	"strings"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/imex"
)

const (
	ComputeDomainDriverName = "compute-domain.nvidia.com"
	// GPUDriverName stands in for the GPU kubelet plugin, which does not run
	// on fake nodes.
	GPUDriverName = "gpu.nvidia.com"
	// GPUDeviceClassName selects the fake GPUs. It is created by scalebench,
	// not by the chart, so that the GPU kubelet plugin can stay disabled.
	GPUDeviceClassName = "scale-gpu.nvidia.com"

	// ManagedByLabelKey marks the ResourceSlices scalebench owns.
	ManagedByLabelKey   = "scale.nvidia.com/managed-by"
	ManagedByLabelValue = "scalebench"

	channelType = "channel"
	daemonType  = "daemon"
)

// ComputeDomainOptions is the kubelet plugin state that decides which
// ComputeDomain devices a node publishes.
type ComputeDomainOptions struct {
	// Reservations is the ComputeDomainChannelReservations feature gate.
	Reservations bool
	// SharedIMEXDaemons is true with static IMEX daemons, where
	// no owner taint and no binding conditions are published.
	SharedIMEXDaemons bool
	// CliqueID is the NVLink clique of the node.
	CliqueID string
	// Owner is the ComputeDomain that owns the node, from its node label,
	// with per-ComputeDomain IMEX daemons.
	Owner string
}

// ComputeDomainDevices returns the devices the ComputeDomain kubelet plugin
// publishes: channel 0, the shared channel device with channel reservations,
// and the daemon device.
func ComputeDomainDevices(opts ComputeDomainOptions) []resourceapi.Device {
	devices := []resourceapi.Device{
		channelDevice(0),
	}
	if opts.Reservations {
		devices = append(devices, sharedChannelDevice(opts))
	}
	devices = append(devices, resourceapi.Device{
		Name: fmt.Sprintf("%s-0", daemonType),
		Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"type": {StringValue: ptr.To(daemonType)},
			"id":   {IntValue: ptr.To(int64(0))},
		},
	})
	slices.SortFunc(devices, func(a, b resourceapi.Device) int { return strings.Compare(a.Name, b.Name) })
	return devices
}

func channelDevice(id int) resourceapi.Device {
	return resourceapi.Device{
		Name: fmt.Sprintf("%s-%d", channelType, id),
		Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"type": {StringValue: ptr.To(channelType)},
			"id":   {IntValue: ptr.To(int64(id))},
		},
	}
}

func sharedChannelDevice(opts ComputeDomainOptions) resourceapi.Device {
	dev := resourceapi.Device{
		Name: imex.ChannelDeviceName,
		Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"type": {StringValue: ptr.To(imex.ChannelDeviceType)},
		},
		AllowMultipleAllocations: ptr.To(true),
	}
	// Only per-ComputeDomain IMEX daemons make pods wait for admission.
	if !opts.SharedIMEXDaemons {
		dev.BindingConditions = []string{imex.ChannelAdmittedCondition}
		dev.BindingFailureConditions = []string{imex.ChannelRejectedCondition}
	}
	if opts.CliqueID != "" {
		dev.Attributes[imex.ChannelCliqueAttribute] = resourceapi.DeviceAttribute{StringValue: ptr.To(opts.CliqueID)}
	}
	if opts.Owner != "" && !opts.SharedIMEXDaemons {
		dev.Taints = []resourceapi.DeviceTaint{{
			Key:    imex.ChannelOwnerTaintKey,
			Value:  opts.Owner,
			Effect: resourceapi.DeviceTaintEffectNoSchedule,
		}}
	}
	return dev
}

// GPUDevices returns n GB200-like GPUs. They only need to cost the scheduler
// what real GPUs cost, so they carry the attributes selectors commonly use.
func GPUDevices(node string, n int) []resourceapi.Device {
	devices := make([]resourceapi.Device, 0, n)
	for i := range n {
		devices = append(devices, resourceapi.Device{
			Name: fmt.Sprintf("gpu-%d", i),
			Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				"type":          {StringValue: ptr.To("gpu")},
				"index":         {IntValue: ptr.To(int64(i))},
				"uuid":          {StringValue: ptr.To(fmt.Sprintf("GPU-%s-%d", node, i))},
				"productName":   {StringValue: ptr.To("NVIDIA GB200")},
				"architecture":  {StringValue: ptr.To("Blackwell")},
				"brand":         {StringValue: ptr.To("Nvidia")},
				"driverVersion": {VersionValue: ptr.To("580.0.0")},
			},
			Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
				"memory": {Value: resource.MustParse("189471Mi")},
			},
		})
	}
	return devices
}

// SliceName is the name of the one slice of a driver on a node.
func SliceName(node, driver string) string {
	return fmt.Sprintf("%s-%s", node, strings.ReplaceAll(driver, ".", "-"))
}

// Slice returns the one ResourceSlice of a driver on a node. Its pool is the
// node, as the kubelet plugins name it.
func Slice(node, driver string, generation int64, devices []resourceapi.Device) *resourceapi.ResourceSlice {
	return &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:   SliceName(node, driver),
			Labels: map[string]string{ManagedByLabelKey: ManagedByLabelValue},
		},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   driver,
			NodeName: ptr.To(node),
			Pool: resourceapi.ResourcePool{
				Name:               node,
				Generation:         generation,
				ResourceSliceCount: 1,
			},
			Devices: devices,
		},
	}
}
