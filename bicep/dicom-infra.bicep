// dicom-infra.bicep
// Deploys DICOM service into the EXISTING Health Data Services workspace from fhir-infra.bicep
// Also grants RBAC for admin group and ACI managed identity

param location string = resourceGroup().location
param workspaceName string          // from fhir-infra.bicep output
param dicomServiceName string = 'dicom${uniqueString(resourceGroup().id)}'
param adminGroupObjectId string = ''
param aciIdentityPrincipalId string = ''

// Reference the existing HDS workspace (DO NOT recreate it)
resource healthWorkspace 'Microsoft.HealthcareApis/workspaces@2023-11-01' existing = {
  name: workspaceName
}

// DICOM Service
resource dicomService 'Microsoft.HealthcareApis/workspaces/dicomservices@2023-11-01' = {
  parent: healthWorkspace
  name: dicomServiceName
  location: location
  properties: {
    authenticationConfiguration: {
      authority: '${environment().authentication.loginEndpoint}${subscription().tenantId}'
      audiences: ['https://${workspaceName}-${dicomServiceName}.dicom.azurehealthcareapis.com']
    }
  }
}

// ============================================
// RBAC: DICOM Data Owner (58a3b984-7adf-4c20-983a-32417c86fbc8)
// ============================================

// Admin security group
resource dicomDataOwnerAdmin 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(adminGroupObjectId)) {
  name: guid(dicomService.id, adminGroupObjectId, '58a3b984-7adf-4c20-983a-32417c86fbc8')
  scope: dicomService
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '58a3b984-7adf-4c20-983a-32417c86fbc8')
    principalId: adminGroupObjectId
    principalType: 'Group'
  }
}

// ACI managed identity (upload access)
resource dicomDataOwnerAci 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(aciIdentityPrincipalId)) {
  name: guid(dicomService.id, aciIdentityPrincipalId, '58a3b984-7adf-4c20-983a-32417c86fbc8')
  scope: dicomService
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '58a3b984-7adf-4c20-983a-32417c86fbc8')
    principalId: aciIdentityPrincipalId
    principalType: 'ServicePrincipal'
  }
}

output dicomServiceName string = dicomService.name
output dicomServiceUrl string = 'https://${workspaceName}-${dicomServiceName}.dicom.azurehealthcareapis.com'
