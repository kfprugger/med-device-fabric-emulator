// dicom-loader-job.bicep
// Azure Container Instance job to download TCIA DICOM studies, re-tag with Synthea
// patient identifiers, and upload .dcm files to ADLS Gen2 (dicom-output container).
//
// Uses the same pre-provisioned User-Assigned Managed Identity (id-aci-fhir-jobs)
// as fhir-loader-job.bicep. The identity must have:
//   - FHIR Data Contributor on the FHIR service (to query patients + create ImagingStudy)
//   - Storage Blob Data Contributor on the storage account (to write .dcm blobs)
//   - AcrPull on the container registry (to pull the dicom-loader image)
//
// This is a run-once job (restartPolicy: Never). Delete and re-create to re-run.

param location string = resourceGroup().location
param acrName string                    // ACR name for image pull credentials
param imageName string                  // Full image reference (e.g. myacr.azurecr.io/dicom-loader:v1)
param storageAccountName string         // ADLS Gen2 storage account for .dcm output
param fhirServiceUrl string             // FHIR service endpoint URL
param aciIdentityId string              // Resource ID of the User-Assigned Managed Identity
param aciIdentityClientId string        // Client ID of the User-Assigned Managed Identity
param tciaCollection string = 'LIDC-IDRI'  // TCIA collection to download (default: chest CT)
param studyCount string = '100'         // Max number of studies to process
param resourceTags object = {}

// Get reference to ACR for credentials
resource acr 'Microsoft.ContainerRegistry/registries@2023-01-01-preview' existing = {
  name: acrName
}

// Container Group for DICOM Loader job with User-Assigned Managed Identity
resource dicomLoaderJob 'Microsoft.ContainerInstance/containerGroups@2023-05-01' = {
  name: 'dicom-loader-job'
  location: location
  tags: resourceTags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${aciIdentityId}': {}
    }
  }
  properties: {
    containers: [
      {
        name: 'dicom-loader'
        properties: {
          image: imageName
          resources: {
            requests: {
              cpu: 2          // DICOM processing is CPU-intensive (pydicom re-tagging)
              memoryInGB: 4   // TCIA downloads can be 50-500MB per study
            }
          }
          // Environment variables consumed by dicom-loader/load_dicom.py
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
              name: 'DICOM_CONTAINER'
              value: 'dicom-output'
            }
            {
              name: 'AZURE_CLIENT_ID'
              value: aciIdentityClientId
            }
            {
              name: 'TCIA_COLLECTION'
              value: tciaCollection
            }
            {
              name: 'STUDY_COUNT'
              value: studyCount
            }
          ]
        }
      }
    ]
    osType: 'Linux'
    restartPolicy: 'Never'    // Run-once job: container exits after processing
    // Pull image from private ACR using admin credentials
    imageRegistryCredentials: [
      {
        server: acr.properties.loginServer
        username: acr.listCredentials().username
        password: acr.listCredentials().passwords[0].value
      }
    ]
  }
}
