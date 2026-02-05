// fhir-loader-job.bicep
// Azure Container Instance job to upload Synthea data to FHIR and create device linkages
// Uses Managed Identity for both blob storage and FHIR service access

param location string = resourceGroup().location
param acrName string
param imageName string
param storageAccountName string
param containerName string
param fhirServiceUrl string

// Get reference to storage account for RBAC
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}

// Get reference to ACR for credentials
resource acr 'Microsoft.ContainerRegistry/registries@2023-01-01-preview' existing = {
  name: acrName
}

// Container Group for FHIR Loader job with System-Assigned Managed Identity
resource fhirLoaderJob 'Microsoft.ContainerInstance/containerGroups@2023-05-01' = {
  name: 'fhir-loader-job'
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    containers: [
      {
        name: 'fhir-loader'
        properties: {
          image: imageName
          resources: {
            requests: {
              cpu: 2
              memoryInGB: 4
            }
          }
          environmentVariables: [
            {
              name: 'FHIR_SERVICE_URL'
              value: fhirServiceUrl
            }
            {
              name: 'STORAGE_ACCOUNT'
              value: storageAccountName
            }
            {
              name: 'CONTAINER_NAME'
              value: containerName
            }
            {
              name: 'DEVICE_COUNT'
              value: '100'
            }
          ]
        }
      }
    ]
    osType: 'Linux'
    restartPolicy: 'Never'
    imageRegistryCredentials: [
      {
        server: acr.properties.loginServer
        username: acr.listCredentials().username
        password: acr.listCredentials().passwords[0].value
      }
    ]
  }
}

// Grant "Storage Blob Data Reader" role to read Synthea output
// Role ID: 2a2b9908-6ea1-4ae2-8e65-a410df84e7d1
resource storageBlobDataReaderRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, fhirLoaderJob.id, '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1')
    principalId: fhirLoaderJob.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// Output the principal ID for FHIR RBAC assignment
output containerGroupName string = fhirLoaderJob.name
output containerGroupId string = fhirLoaderJob.id
output principalId string = fhirLoaderJob.identity.principalId
