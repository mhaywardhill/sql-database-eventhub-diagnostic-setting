#!/usr/bin/env bash
set -euo pipefail

if command -v az >/dev/null 2>&1; then
  echo "Azure CLI already installed: $(az version --query '"'"'azure-cli'"'"' -o tsv 2>/dev/null || echo installed)"
  exit 0
fi

export DEBIAN_FRONTEND=noninteractive

sudo apt-get update
sudo apt-get install -y ca-certificates curl lsb-release gnupg

curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

az version --query '"'"'azure-cli'"'"' -o tsv
