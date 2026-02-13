// synthea-job.bicep
// Azure Container Instance job to run Synthea and generate synthetic patients
// Uses a pre-provisioned User-Assigned Managed Identity for blob storage access (no RBAC propagation delay)

param location string = resourceGroup().location
param acrName string
param imageName string
param storageAccountName string
param containerName string
param patientCount int = 10000
param aciIdentityId string
param aciIdentityClientId string

// Get reference to ACR for credentials
resource acr 'Microsoft.ContainerRegistry/registries@2023-01-01-preview' existing = {
  name: acrName
}

// Container Group for Synthea job with User-Assigned Managed Identity
resource syntheaJob 'Microsoft.ContainerInstance/containerGroups@2023-05-01' = {
  name: 'synthea-generator-job'
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${aciIdentityId}': {}
    }
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
            {
              name: 'AZURE_CLIENT_ID'
              value: aciIdentityClientId
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

output containerGroupName string = syntheaJob.name
output containerGroupId string = syntheaJob.id
