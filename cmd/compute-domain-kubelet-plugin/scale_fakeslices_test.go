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

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/dra-driver-nvidia-gpu/pkg/imex"
	"sigs.k8s.io/dra-driver-nvidia-gpu/tests/scale/fakeslices"
)

// TestScaleFakeSlicesMatchPublishedDevices keeps the devices that the scale
// tests publish for fake nodes identical to what the kubelet plugin publishes,
// so that scale results hold for real nodes.
func TestScaleFakeSlicesMatchPublishedDevices(t *testing.T) {
	staticConfig := func() *Config {
		return &Config{flags: &Flags{}, imexConfig: imex.Config{DaemonLifecycle: imex.DaemonLifecycleStatic}}
	}
	tests := map[string]struct {
		config       func() *Config
		reservations bool
		state        channelDeviceState
	}{
		"per-ComputeDomain": {
			config: driverManagedConfig,
		},
		"per-ComputeDomain with reservations": {
			config:       driverManagedConfig,
			reservations: true,
			state:        channelDeviceState{cliqueID: "uuid.1"},
		},
		"per-ComputeDomain with reservations and an owner": {
			config:       driverManagedConfig,
			reservations: true,
			state:        channelDeviceState{cliqueID: "uuid.1", owner: "cd-uid"},
		},
		"static": {
			config:       staticConfig,
			reservations: true,
			state:        channelDeviceState{cliqueID: "uuid.1", owner: "ignored"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			setChannelReservationsGate(t, tc.reservations)
			config := tc.config()
			allocatable, _ := testChannels(4)
			allocatable["daemon-0"] = &AllocatableDevice{Daemon: &ComputeDomainDaemonInfo{ID: 0}}

			fake := fakeslices.ComputeDomainDevices(fakeslices.ComputeDomainOptions{
				Reservations:      tc.reservations,
				SharedIMEXDaemons: config.imexConfig.SharedIMEXDaemons(),
				CliqueID:          tc.state.cliqueID,
				Owner:             tc.state.owner,
			})
			assert.Equal(t, computeDomainPublishedDevices(allocatable, config, tc.state), fake)
		})
	}
}
