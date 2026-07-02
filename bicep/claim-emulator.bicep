// claim-emulator.bicep
param location string = resourceGroup().location
param acrName string
param imageName string
param eventHubName string = 'claim-stream'
param eventHubNamespace string
param eventRatePerMinute int = 60
param patientCount int = 1000
param fraudVelocityProviderId string = 'TEST-PROVIDER'
param fraudAmountMultiplier int = 6

param resourceTags object = {}

// 1. Get Reference to Registry (for credentials)
resource acr 'Microsoft.ContainerRegistry/registries@2023-01-01-preview' existing = {
  name: acrName
}

// 2. Get Reference to Event Hub Namespace (for RBAC)
resource ehNamespace 'Microsoft.EventHub/namespaces@2021-11-01' existing = {
  name: eventHubNamespace
}

// 3. Create Container with System Identity
resource containerGroup 'Microsoft.ContainerInstance/containerGroups@2023-05-01' = {
  name: 'claim-emulator-grp'
  location: location
  tags: resourceTags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    containers: [
      {
        name: 'claim-emulator'
        properties: {
          image: imageName
          resources: { requests: { cpu: 1, memoryInGB: 1 } }
          environmentVariables: [
            {
              name: 'EVENT_HUB_NAME'
              value: eventHubName
            }
            {
              name: 'EVENT_HUB_NAMESPACE'
              value: eventHubNamespace
            }
            {
              name: 'CLAIM_EVENT_RATE_PER_MINUTE'
              value: string(eventRatePerMinute)
            }
            {
              name: 'PATIENT_COUNT'
              value: string(patientCount)
            }
            {
              name: 'FRAUD_VELOCITY_PROVIDER_ID'
              value: fraudVelocityProviderId
            }
            {
              name: 'FRAUD_AMOUNT_MULTIPLIER'
              value: string(fraudAmountMultiplier)
            }
          ]
        }
      }
    ]
    osType: 'Linux'
    restartPolicy: 'Always'
    imageRegistryCredentials: [
      {
        server: acr.properties.loginServer
        username: acr.listCredentials().username
        password: acr.listCredentials().passwords[0].value
      }
    ]
  }
}

// 4. Grant "Azure Event Hubs Data Sender" role to the container's managed identity
// Role ID: 2b629674-e913-4c01-ae53-ef4638d8f975
resource eventHubSenderRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(ehNamespace.id, containerGroup.id, '2b629674-e913-4c01-ae53-ef4638d8f975')
  scope: ehNamespace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '2b629674-e913-4c01-ae53-ef4638d8f975')
    principalId: containerGroup.identity.principalId
    principalType: 'ServicePrincipal'
  }
}
