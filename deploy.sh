#!/usr/bin/env bash

set -euo pipefail

AZURE_LOCATION="${AZURE_LOCATION:-uksouth}"

# ---------------------------------------------------------------------------
# Configuration — override via environment variables or edit defaults below
# ---------------------------------------------------------------------------
deployment_name="${DEPLOYMENT_NAME:-eventhub-sql-diag-$(date +%Y%m%d%H%M%S)}"

if [[ -z "${RG_NAME:-}" ]]; then
  echo "RG_NAME is required."
  exit 1
fi

if [[ -z "${ENTRA_ADMIN_LOGIN:-}" ]]; then
  echo "ENTRA_ADMIN_LOGIN is required."
  exit 1
fi

if [[ -z "${ENTRA_ADMIN_OBJECT_ID:-}" ]]; then
  echo "ENTRA_ADMIN_OBJECT_ID is required."
  exit 1
fi

# ---------------------------------------------------------------------------
# Ensure logged in to Azure
# ---------------------------------------------------------------------------
echo "Checking Azure CLI login..."
az account show --output none 2>/dev/null || {
  echo "Not logged in. Running 'az login'..."
  az login
}

echo "Subscription: $(az account show --query '{name:name, id:id}' -o tsv)"
active_tenant_id="$(az account show --query tenantId -o tsv)"
ENTRA_ADMIN_TENANT_ID="${ENTRA_ADMIN_TENANT_ID:-$active_tenant_id}"
echo "Tenant ID: $ENTRA_ADMIN_TENANT_ID"

# ---------------------------------------------------------------------------
# Create resource group
# ---------------------------------------------------------------------------
az group create \
  --name "$RG_NAME" \
  --location "$AZURE_LOCATION" \
  --output table

# ---------------------------------------------------------------------------
# Deploy Bicep template
# ---------------------------------------------------------------------------
echo "Starting deployment '${deployment_name}'..."
az deployment group create \
  --resource-group "$RG_NAME" \
  --name "$deployment_name" \
  --template-file main.bicep \
  --parameters \
    location="$AZURE_LOCATION" \
    entraAdministratorLogin="$ENTRA_ADMIN_LOGIN" \
    entraAdministratorObjectId="$ENTRA_ADMIN_OBJECT_ID" \
    entraAdministratorTenantId="$ENTRA_ADMIN_TENANT_ID"