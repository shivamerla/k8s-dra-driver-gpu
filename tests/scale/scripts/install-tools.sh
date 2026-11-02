#!/usr/bin/env bash
# Copyright The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Installs what the scale tests need on Ubuntu 24.04 (amd64 or arm64):
# Docker, kind, kubectl, helm, Go, make and git, and raises the inotify and
# open file limits that kind needs for a large cluster. Run it as a user
# with sudo; log out and back in afterwards to use Docker without sudo.

set -euo pipefail

: "${KIND_VERSION:=v0.33.0}"
: "${KUBECTL_VERSION:=v1.37.0}"
: "${GO_VERSION:=1.26.0}"

case "$(uname -m)" in
  x86_64) ARCH=amd64 ;;
  aarch64) ARCH=arm64 ;;
  *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

log "base packages"
sudo apt-get update -q
sudo apt-get install -yq ca-certificates curl git make build-essential

if ! command -v docker > /dev/null; then
  log "Docker"
  sudo install -m 0755 -d /etc/apt/keyrings
  sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  echo "deb [arch=${ARCH} signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "${VERSION_CODENAME}") stable" |
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo apt-get update -q
  sudo apt-get install -yq docker-ce docker-ce-cli containerd.io docker-buildx-plugin
fi
sudo usermod -aG docker "${USER}"
# The driver image is built with buildx, which Ubuntu's docker.io leaves out.
if ! sudo docker buildx version > /dev/null 2>&1; then
  log "Docker buildx"
  if dpkg -s docker-ce > /dev/null 2>&1; then
    sudo apt-get install -yq docker-buildx-plugin
  else
    sudo apt-get install -yq docker-buildx
  fi
fi

log "kind ${KIND_VERSION}"
sudo curl -fsSLo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-${ARCH}"
sudo chmod +x /usr/local/bin/kind

log "kubectl ${KUBECTL_VERSION}"
sudo curl -fsSLo /usr/local/bin/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl"
sudo chmod +x /usr/local/bin/kubectl

if ! command -v helm > /dev/null; then
  log "helm"
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi

log "Go ${GO_VERSION}"
curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${ARCH}.tar.gz" | sudo tar -C /usr/local -xz --one-top-level=go.new --strip-components=1
sudo rm -rf /usr/local/go && sudo mv /usr/local/go.new /usr/local/go
grep -q /usr/local/go/bin ~/.profile || echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' >> ~/.profile

log "kernel and open file limits"
sudo tee /etc/sysctl.d/99-kind-scale.conf > /dev/null << EOF
fs.inotify.max_user_watches = 1048576
fs.inotify.max_user_instances = 8192
fs.file-max = 2097152
EOF
sudo sysctl --system > /dev/null
sudo tee /etc/security/limits.d/99-kind-scale.conf > /dev/null << EOF
* soft nofile 1048576
* hard nofile 1048576
EOF

log "installed:"
docker --version
docker buildx version
kind version
kubectl version --client
helm version --short
/usr/local/go/bin/go version
log "log out and back in (or run 'newgrp docker') to use Docker and the new limits"
