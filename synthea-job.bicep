// synthea-job.bicep
// Azure Container Instance job to run Synthea and generate synthetic patients
// Uses Managed Identity to upload to Blob Storage (no storage keys needed)

param location string = resourceGroup().location
param acrName string
param imageName string
param storageAccountName string
param containerName string
param patientCount int = 10000

// Get reference to storage account for RBAC
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}

// Get reference to ACR for credentials
resource acr 'Microsoft.ContainerRegistry/registries@2023-01-01-preview' existing = {
  name: acrName
}

// Container Group for Synthea job with System-Assigned Managed Identity
resource syntheaJob 'Microsoft.ContainerInstance/containerGroups@2023-05-01' = {
  name: 'synthea-generator-job'
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    containers: [
      {
        name: 'synthea'
        properties: {
          image: imageName
          resources: {
            requests: {
              cpu: 4
              memoryInGB: 8
            }
          }
          environmentVariables: [
            {
              name: 'PATIENT_COUNT'
              value: string(patientCount)
            }
            {
              name: 'STORAGE_ACCOUNT'
              value: storageAccountName
            }
            {
              name: 'CONTAINER_NAME'
              value: containerName
            }
          ]
        }
      }
    ]
    osType: 'Linux'
    restartPolicy: 'Never'  // Run once and exit
    imageRegistryCredentials: [
      {
        server: acr.properties.loginServer
        username: acr.listCredentials().username
        password: acr.listCredentials().passwords[0].value
      }
    ]
  }
}

// Grant "Storage Blob Data Contributor" role to the container's managed identity
// Role ID: ba92f5b4-2d11-453d-a403-e96b0029c9fe
resource storageBlobDataContributorRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, syntheaJob.id, 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: syntheaJob.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

output containerGroupName string = syntheaJob.name
output containerGroupId string = syntheaJob.id
