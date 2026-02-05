// infra.bicep
param location string = resourceGroup().location
param tenantId string = subscription().tenantId
param adminGroupObjectId string = ''

@minLength(3) 
param appName string = 'masimo${uniqueString(resourceGroup().id)}'

// 1. Event Hub Namespace & Hub
resource ehNamespace 'Microsoft.EventHub/namespaces@2021-11-01' = {
  name: '${appName}-eh-ns'
  location: location
  sku: { name: 'Standard', tier: 'Standard' }
  properties: {
    disableLocalAuth: false
  }
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2021-11-01' = {
  parent: ehNamespace
  name: 'telemetry-stream'
  properties: { messageRetentionInDays: 1, partitionCount: 2 }
}

// Use namespace-level auth rule for better compatibility
resource nsAuthRule 'Microsoft.EventHub/namespaces/authorizationRules@2021-11-01' = {
  parent: ehNamespace
  name: 'emulator-access'
  properties: { rights: ['Send', 'Listen'] }
}

// 2. Azure Container Registry
resource acr 'Microsoft.ContainerRegistry/registries@2023-01-01-preview' = {
  name: '${appName}acr'
  location: location
  sku: { name: 'Basic' }
  properties: { adminUserEnabled: true }
}

// 3. Key Vault (Created Empty initially)
resource keyVault 'Microsoft.KeyVault/vaults@2023-02-01' = {
  name: '${appName}-kv'
  location: location
  properties: {
    sku: { family: 'A', name: 'standard' }
    tenantId: tenantId
    enableRbacAuthorization: true  // Use RBAC instead of access policies
    accessPolicies: [] 
  }
}

// 4. Store the Secret
resource secret 'Microsoft.KeyVault/vaults/secrets@2023-02-01' = {
  parent: keyVault
  name: 'EventHubConnStr'
  properties: {
    value: nsAuthRule.listKeys().primaryConnectionString
  }
}

// ============================================
// RBAC for Admin Security Group
// ============================================

// ACR Pull role for admin group (pull images)
resource acrPullRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(adminGroupObjectId)) {
  name: guid(acr.id, adminGroupObjectId, '7f951dda-4ed3-4680-a7ca-43fe172d538d')
  scope: acr
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d') // AcrPull
    principalId: adminGroupObjectId
    principalType: 'Group'
  }
}

// ACR Push role for admin group (push images)
resource acrPushRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(adminGroupObjectId)) {
  name: guid(acr.id, adminGroupObjectId, '8311e382-0749-4cb8-b61a-304f252e45ec')
  scope: acr
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '8311e382-0749-4cb8-b61a-304f252e45ec') // AcrPush
    principalId: adminGroupObjectId
    principalType: 'Group'
  }
}

// Key Vault Secrets User for admin group (read secrets)
resource kvSecretsUserRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(adminGroupObjectId)) {
  name: guid(keyVault.id, adminGroupObjectId, '4633458b-17de-408a-b874-0445c86b69e6')
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6') // Key Vault Secrets User
    principalId: adminGroupObjectId
    principalType: 'Group'
  }
}

// Key Vault Administrator for admin group (full control)
resource kvAdminRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(adminGroupObjectId)) {
  name: guid(keyVault.id, adminGroupObjectId, '00482a5a-887f-4fb3-b363-3b7fe8e74483')
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '00482a5a-887f-4fb3-b363-3b7fe8e74483') // Key Vault Administrator
    principalId: adminGroupObjectId
    principalType: 'Group'
  }
}

// Azure Event Hubs Data Owner for admin group
resource ehDataOwnerRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(adminGroupObjectId)) {
  name: guid(ehNamespace.id, adminGroupObjectId, 'f526a384-b230-433a-b45c-95f59c4a2dec')
  scope: ehNamespace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'f526a384-b230-433a-b45c-95f59c4a2dec') // Azure Event Hubs Data Owner
    principalId: adminGroupObjectId
    principalType: 'Group'
  }
}

// --- OUTPUTS ---
output acrLoginServer string = acr.properties.loginServer
output acrName string = acr.name
output eventHubName string = eventHub.name
output eventHubNamespace string = ehNamespace.name
output keyVaultName string = keyVault.name
