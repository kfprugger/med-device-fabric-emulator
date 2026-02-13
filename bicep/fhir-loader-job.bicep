// fhir-loader-job.bicep
// Azure Container Instance job to upload Synthea data to FHIR and create device linkages
// Uses a pre-provisioned User-Assigned Managed Identity for blob storage and FHIR service access (no RBAC propagation delay)

param location string = resourceGroup().location
param acrName string
param imageName string
param storageAccountName string
param containerName string
param fhirServiceUrl string
param aciIdentityId string
param aciIdentityClientId string

// Get reference to ACR for credentials
resource acr 'Microsoft.ContainerRegistry/registries@2023-01-01-preview' existing = {
  name: acrName
}

// Container Group for FHIR Loader job with User-Assigned Managed Identity
resource fhirLoaderJob 'Microsoft.ContainerInstance/containerGroups@2023-05-01' = {
  name: 'fhir-loader-job'
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
            {
              name: 'AZURE_CLIENT_ID'
              value: aciIdentityClientId
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

output containerGroupName string = fhirLoaderJob.name
output containerGroupId string = fhirLoaderJob.id
