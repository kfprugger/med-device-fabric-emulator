// fhir-infra.bicep
// Deploys Azure Health Data Services workspace with FHIR service

param location string = resourceGroup().location
param workspaceName string = 'hdws${uniqueString(resourceGroup().id)}'
param fhirServiceName string = 'fhir${uniqueString(resourceGroup().id)}'
param storageAccountName string = 'stfhir${uniqueString(resourceGroup().id)}'
param adminGroupObjectId string = ''

// Storage Account for Synthea output (using Blob storage with Managed Identity)
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot'
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowSharedKeyAccess: true  // Required for some operations
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
}

// Blob service for Synthea FHIR output
resource blobServices 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
}

// Blob container for Synthea output
resource syntheaContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobServices
  name: 'synthea-output'
  properties: {
    publicAccess: 'None'
  }
}

// Health Data Services Workspace
resource healthWorkspace 'Microsoft.HealthcareApis/workspaces@2023-11-01' = {
  name: workspaceName
  location: location
  properties: {}
}

// FHIR Service (R4)
resource fhirService 'Microsoft.HealthcareApis/workspaces/fhirservices@2023-11-01' = {
  parent: healthWorkspace
  name: fhirServiceName
  location: location
  kind: 'fhir-R4'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    authenticationConfiguration: {
      authority: '${environment().authentication.loginEndpoint}${subscription().tenantId}'
      audience: 'https://${workspaceName}-${fhirServiceName}.fhir.azurehealthcareapis.com'
      smartProxyEnabled: false
    }
    corsConfiguration: {
      origins: ['*']
      headers: ['*']
      methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']
      allowCredentials: false
    }
  }
}

// ============================================
// RBAC for Admin Security Group
// ============================================

// FHIR Data Contributor role for admin group (read/write FHIR data)
resource fhirDataContributorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(adminGroupObjectId)) {
  name: guid(fhirService.id, adminGroupObjectId, '5a1fc7df-4bf1-4951-a576-89034ee01acd')
  scope: fhirService
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '5a1fc7df-4bf1-4951-a576-89034ee01acd') // FHIR Data Contributor
    principalId: adminGroupObjectId
    principalType: 'Group'
  }
}

// FHIR Data Reader role for admin group (redundant with contributor but explicit)
resource fhirDataReaderRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(adminGroupObjectId)) {
  name: guid(fhirService.id, adminGroupObjectId, '4c8d0bbc-75d3-4935-991f-5f3c56d81508')
  scope: fhirService
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4c8d0bbc-75d3-4935-991f-5f3c56d81508') // FHIR Data Reader
    principalId: adminGroupObjectId
    principalType: 'Group'
  }
}

// Storage Blob Data Contributor for admin group (read/write blob data)
resource storageBlobContributorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(adminGroupObjectId)) {
  name: guid(storageAccount.id, adminGroupObjectId, 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe') // Storage Blob Data Contributor
    principalId: adminGroupObjectId
    principalType: 'Group'
  }
}

// Storage Account Contributor for admin group (manage storage account)
resource storageAccountContributorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(adminGroupObjectId)) {
  name: guid(storageAccount.id, adminGroupObjectId, '17d1049b-9a84-46fb-8f53-869881c3d3ab')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '17d1049b-9a84-46fb-8f53-869881c3d3ab') // Storage Account Contributor
    principalId: adminGroupObjectId
    principalType: 'Group'
  }
}

// Reader role on Health Data Services Workspace for admin group
resource healthWorkspaceReaderRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(adminGroupObjectId)) {
  name: guid(healthWorkspace.id, adminGroupObjectId, 'acdd72a7-3385-48ef-bd42-f606fba81ae7')
  scope: healthWorkspace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'acdd72a7-3385-48ef-bd42-f606fba81ae7') // Reader
    principalId: adminGroupObjectId
    principalType: 'Group'
  }
}

// Outputs
output workspaceName string = healthWorkspace.name
output fhirServiceName string = fhirService.name
output fhirServiceUrl string = 'https://${workspaceName}-${fhirServiceName}.fhir.azurehealthcareapis.com'
output storageAccountName string = storageAccount.name
output containerName string = syntheaContainer.name
