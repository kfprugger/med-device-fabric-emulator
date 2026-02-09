# Session Log — February 9, 2026

## Overview

This session completed the FHIR-to-Fabric data bridge: configuring FHIR `$export` to Azure Blob Storage, creating a Fabric cloud connection using Workspace Identity, and setting up an OneLake shortcut in the Bronze lakehouse to surface FHIR NDJSON data for downstream analytics.

---

## 1. FHIR `$export` to Azure Blob Storage

### What was done

- Discovered existing FHIR service and storage account via ARM API
- Granted the FHIR service managed identity (`95fc29ac-d5b7-47dc-ace8-9087e13d6744`) **Storage Blob Data Contributor** on `stfhiriecaacmlqodcs`
- Updated FHIR service export configuration via `PUT` (ARM REST API) to set `storageAccountName = stfhiriecaacmlqodcs`
- Created `fhir-export` blob container
- Triggered `$export` operation → polled until complete
- **Result**: 565 NDJSON files exported to `stfhiriecaacmlqodcs/fhir-export/20260207T030715-1/`

### Resource types exported

| Resource Type       | Files | Approx Records |
|---------------------|-------|----------------|
| Patient             | 1     | 7,800          |
| Observation         | 281   | ~2.8M          |
| Procedure           | 102   | ~1M            |
| DiagnosticReport    | 69    | ~690K          |
| Encounter           | 37    | ~370K          |
| MedicationRequest   | 26    | ~260K          |
| Condition           | 25    | ~250K          |
| Organization        | 1     | 286            |
| Practitioner        | 1     | 270            |
| + others            | 22    | varies         |

### Key learnings

- FHIR service `PUT` update requires the full `authenticationConfiguration` block (authority, audience, smartProxyEnabled), otherwise returns "Provided authority is not valid AAD"
- FHIR service provisioning takes ~2 minutes: `Accepted → Updating → Succeeded` — poll with 15s intervals
- `az healthcareapis` CLI extension can fail with WinError 5 (quota extension permission error) — workaround: use ARM REST API directly

---

## 2. Fabric Cloud Connection (Azure Blob Storage + Workspace Identity)

### What was done

- Confirmed workspace identity already provisioned:
  - Application ID: `aeebd326-42aa-4895-ae96-e5cf35f09318`
  - Service Principal ID: `fbae8adf-8924-4abd-bfd9-c426fb5a6496`
- Granted workspace identity **Storage Blob Data Contributor** on `stfhiriecaacmlqodcs`
- Created Fabric cloud connection via `POST /v1/connections`

### Connection details

| Property       | Value |
|----------------|-------|
| Name           | `FHIR-Export-Storage-Connection` |
| ID             | `8c8d4d26-dfd1-4cf0-8413-be777a36a585` |
| Type           | `AzureBlobs` |
| Connectivity   | `ShareableCloud` |
| Auth           | `WorkspaceIdentity` |
| Privacy Level  | `Organizational` |
| Endpoint       | `https://stfhiriecaacmlqodcs.blob.core.windows.net/` |

### Key learnings — Fabric Connections API

The Connections API was the most challenging part. Several iterations were needed:

1. **Missing `creationMethod`**: The `POST /v1/connections` body requires `creationMethod` inside `connectionDetails`. Without it → 400 error: *"The CreationMethod field is required"*

2. **Wrong parameter names**: Each connector type has its own parameter names. For SQL it's `server`/`database`, but for AzureBlobs it's completely different → 400 error: *"Parameters contains extra property 'server'"*

3. **Discovery endpoint**: The correct parameter names are found via:
   ```
   GET https://api.fabric.microsoft.com/v1/connections/supportedConnectionTypes
   ```
   Filter by `type == "AzureBlobs"` to find:
   ```json
   {
     "type": "AzureBlobs",
     "creationMethods": [{
       "name": "AzureBlobs",
       "parameters": [
         { "name": "account", "dataType": "Text", "required": true },
         { "name": "domain",  "dataType": "Text", "required": true }
       ]
     }]
   }
   ```

4. **Working request body**:
   ```json
   {
     "connectivityType": "ShareableCloud",
     "displayName": "FHIR-Export-Storage-Connection",
     "connectionDetails": {
       "type": "AzureBlobs",
       "creationMethod": "AzureBlobs",
       "parameters": [
         { "dataType": "Text", "name": "account", "value": "stfhiriecaacmlqodcs" },
         { "dataType": "Text", "name": "domain",  "value": "blob.core.windows.net" }
       ]
     },
     "privacyLevel": "Organizational",
     "credentialDetails": {
       "singleSignOnType": "None",
       "connectionEncryption": "NotEncrypted",
       "skipTestConnection": false,
       "credentials": { "credentialType": "WorkspaceIdentity" }
     }
   }
   ```

---

## 3. OneLake Shortcut in Bronze Lakehouse

### What was done

- Located Bronze lakehouse: `healthcare1_msft_bronze` (ID: `00726fd2-dca3-469e-8f51-47ab94d62ef4`)
- Created OneLake shortcut via `POST /v1/workspaces/{wsId}/items/{lhId}/shortcuts`

### Shortcut details

| Property        | Value |
|-----------------|-------|
| Name            | `FHIR-Export-NDJSON` |
| Lakehouse       | `healthcare1_msft_bronze` |
| Path            | `Files/External/Clinical/FHIR-NDJSON/AHDS-FHIR` |
| Target Type     | `AzureBlobStorage` |
| Target Location | `https://stfhiriecaacmlqodcs.blob.core.windows.net` |
| Target Subpath  | `/fhir-export` |
| Connection ID   | `8c8d4d26-dfd1-4cf0-8413-be777a36a585` |

### Working request body

```json
{
  "path": "Files/External/Clinical/FHIR-NDJSON/AHDS-FHIR",
  "name": "FHIR-Export-NDJSON",
  "target": {
    "azureBlobStorage": {
      "location": "https://stfhiriecaacmlqodcs.blob.core.windows.net",
      "subpath": "/fhir-export",
      "connectionId": "8c8d4d26-dfd1-4cf0-8413-be777a36a585"
    }
  }
}
```

**Note**: Use `?shortcutConflictPolicy=CreateOrOverwrite` query param to update an existing shortcut.

---

## 4. Fabric Notebook `scipy` Fix

### Issue

AHDS export pipeline notebook failed with:
```
ImportError: cannot import name '_promote' from 'scipy.spatial.transform._rotation'
```

### Root cause

Fabric Runtime 1.3 ships with a `scipy` version where `_promote` was removed/renamed from the internal `_rotation` module.

### Fix

**Permanent (Workspace Environment)**:
1. In Fabric workspace → **Environment** item → **Public libraries**
2. Add `scipy==1.11.4`
3. **Publish** the environment
4. Set the pipeline notebook to use this environment

**Temporary (inline)**:
```python
%pip install scipy==1.11.4
```

---

## Resource Reference

| Resource | Value |
|----------|-------|
| Subscription | `9bbee190-dc61-4c58-ab47-1275cb04018f` |
| Resource Group | `rg-medtech-sys-identity` |
| HDS Workspace | `hdwsiecaacmlqodcs` |
| FHIR Service | `fhiriecaacmlqodcs` |
| FHIR URL | `https://hdwsiecaacmlqodcs-fhiriecaacmlqodcs.fhir.azurehealthcareapis.com` |
| Storage Account | `stfhiriecaacmlqodcs` |
| Export Container | `fhir-export` |
| Export Folder | `20260207T030715-1/` |
| Fabric Workspace | `c1b55ec1-3b50-4a2c-ace1-46d788c85a4f` |
| Bronze Lakehouse | `00726fd2-dca3-469e-8f51-47ab94d62ef4` |
| Connection ID | `8c8d4d26-dfd1-4cf0-8413-be777a36a585` |
| Workspace Identity App ID | `aeebd326-42aa-4895-ae96-e5cf35f09318` |
| Workspace Identity SP ID | `fbae8adf-8924-4abd-bfd9-c426fb5a6496` |
| Tenant ID | `8d038e6a-9b7d-4cb8-bbcf-e84dff156478` |
