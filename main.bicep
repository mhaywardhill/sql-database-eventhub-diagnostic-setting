@description('Azure region for all resources.')
param location string = 'uksouth'

@description('Name of the Azure SQL logical server. Must be globally unique.')
@minLength(1)
@maxLength(63)
param sqlServerName string = 'sql-${uniqueString(resourceGroup().id)}'

@description('Name of the Azure SQL database.')
@minLength(1)
@maxLength(128)
param sqlDatabaseName string = 'sqldb-app'

@description('Microsoft Entra administrator login (display name or UPN) for SQL Server.')
@minLength(1)
param entraAdministratorLogin string

@description('Object ID (GUID) of the Microsoft Entra administrator principal.')
param entraAdministratorObjectId string

@description('Tenant ID for the Microsoft Entra administrator principal.')
param entraAdministratorTenantId string = tenant().tenantId

@description('SKU name for the SQL database (for example S0, Basic, GP_S_Gen5_1).')
param sqlDatabaseSkuName string = 'S0'

@description('Name of the Event Hub namespace. Must be globally unique.')
@minLength(6)
@maxLength(50)
param eventHubNamespaceName string = 'ehns-${uniqueString(resourceGroup().id)}'

@description('SKU tier for Event Hub namespace. Diagnostic settings to Event Hub require Standard or above.')
@allowed([
  'Standard'
  'Premium'
])
param eventHubNamespaceSku string = 'Standard'

@description('Throughput units / processing units for the Event Hub namespace.')
@minValue(1)
param eventHubNamespaceCapacity int = 1

@description('Name of the Event Hub that receives SQL diagnostics.')
@minLength(1)
@maxLength(50)
param eventHubName string = 'sql-db-diagnostics'

@description('Partition count for the Event Hub.')
@minValue(1)
@maxValue(32)
param eventHubPartitionCount int = 2

@description('Message retention in days for the Event Hub.')
@minValue(1)
@maxValue(7)
param eventHubMessageRetentionInDays int = 1

@description('Namespace-level authorization rule name used by SQL diagnostic settings to publish to Event Hub.')
param eventHubAuthorizationRuleName string = 'sql-diag-send'

@description('Metric categories to enable on the database diagnostic setting.')
@allowed([
  'Basic'
  'InstanceAndAppAdvanced'
  'WorkloadManagement'
])
param metricCategories array = ['Basic']

@description('Name of the database diagnostic setting.')
param diagnosticSettingName string = 'sql-db-diag-to-eventhub'

resource sqlServer 'Microsoft.Sql/servers@2023-08-01-preview' = {
  name: sqlServerName
  location: location
  properties: {
    publicNetworkAccess: 'Enabled'
    minimalTlsVersion: '1.2'
    administrators: {
      administratorType: 'ActiveDirectory'
      login: entraAdministratorLogin
      sid: entraAdministratorObjectId
      tenantId: entraAdministratorTenantId
      azureADOnlyAuthentication: true
    }
  }
}

resource sqlDatabase 'Microsoft.Sql/servers/databases@2023-08-01-preview' = {
  name: sqlDatabaseName
  parent: sqlServer
  location: location
  sku: {
    name: sqlDatabaseSkuName
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
  }
}

resource eventHubNamespace 'Microsoft.EventHub/namespaces@2022-10-01-preview' = {
  name: eventHubNamespaceName
  location: location
  sku: {
    name: eventHubNamespaceSku
    tier: eventHubNamespaceSku
    capacity: eventHubNamespaceCapacity
  }
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2022-10-01-preview' = {
  name: eventHubName
  parent: eventHubNamespace
  properties: {
    partitionCount: eventHubPartitionCount
    messageRetentionInDays: eventHubMessageRetentionInDays
  }
}

resource eventHubNamespaceAuthorizationRule 'Microsoft.EventHub/namespaces/AuthorizationRules@2022-10-01-preview' = {
  name: eventHubAuthorizationRuleName
  parent: eventHubNamespace
  properties: {
    rights: [
      'Send'
    ]
  }
}

resource eventHubListenRule 'Microsoft.EventHub/namespaces/AuthorizationRules@2022-10-01-preview' = {
  name: 'sql-diag-listen'
  parent: eventHubNamespace
  properties: {
    rights: [
      'Listen'
    ]
  }
}

resource sqlDatabaseDiagnosticSetting 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: diagnosticSettingName
  scope: sqlDatabase
  properties: {
    eventHubAuthorizationRuleId: eventHubNamespaceAuthorizationRule.id
    eventHubName: eventHub.name
    logs: []
    metrics: [for category in metricCategories: {
      category: category
      enabled: true
    }]
  }
}

output sqlServerResourceId string = sqlServer.id
output sqlDatabaseResourceId string = sqlDatabase.id
output eventHubNamespaceResourceId string = eventHubNamespace.id
output eventHubResourceId string = eventHub.id
output eventHubAuthorizationRuleResourceId string = eventHubNamespaceAuthorizationRule.id
output eventHubListenRuleResourceId string = eventHubListenRule.id
output diagnosticSettingResourceId string = sqlDatabaseDiagnosticSetting.id
