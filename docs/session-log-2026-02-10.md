# Session Log — February 10, 2026

## Overview

This session corrected the Fabric-to-Azure Storage integration from Azure Blob Storage to ADLS Gen 2, upgraded the existing storage account to HNS, and updated all related infrastructure code.

---

## 1. Bicep Template — ADLS Gen 2 + Storage Blob Data Reader

### What was done

- Updated `fhir-infra.bicep` to create the storage account with `isHnsEnabled: true` (ADLS Gen 2 with Hierarchical Namespace)
- Added `Storage Blob Data Reader` role assignment for the admin security group (`sg-azure-admins`) alongside the existing `Storage Blob Data Contributor`

### Key changes

```bicep
// Before
properties: {
  accessTier: 'Hot'
  // ... no isHnsEnabled
}

// After
properties: {
  accessTier: 'Hot'
  isHnsEnabled: true  // Enables ADLS Gen 2
}
```

New role assignment added:
```
Storage Blob Data Reader (2a2b9908-6ea1-4ae2-8e65-a410df84e7d1) → sg-azure-admins
```

---

## 2. Storage Account HNS Migration (Blob → ADLS Gen 2)

### What was done

- Attempted in-place HNS migration of `stfhiriecaacmlqodcs`
- Migration validation failed due to **Blob Index Tags** on all 565 FHIR `$export` NDJSON files (Malware Scanning tags from Microsoft Defender for Storage)
- Cleared all blob index tags via Azure Storage REST API (`PUT ?comp=tags` with empty `<TagSet>`)
- Re-validated and then executed the HNS migration successfully

### Key learnings — HNS Migration

1. **Blob Tags are incompatible with HNS**: Azure FHIR `$export` + Microsoft Defender for Storage sets `Malware Scanning scan result` and `Malware Scanning scan time UTC` tags on exported blobs. These must be cleared before HNS migration.

2. **`az storage blob tag set --tags '{}'` does NOT clear tags**: Despite running successfully, this CLI command was silently failing due to insufficient permissions (requires `Storage Blob Data Owner`, not just `Contributor`).

3. **REST API approach for clearing tags**: The reliable way to clear blob tags:
   ```powershell
   $token = az account get-access-token --resource https://storage.azure.com --query accessToken -o tsv
   $headers = @{ "Authorization" = "Bearer $token"; "x-ms-version" = "2021-04-10" }
   $body = '<?xml version="1.0" encoding="utf-8"?><Tags><TagSet></TagSet></Tags>'
   Invoke-RestMethod -Uri "https://<account>.blob.core.windows.net/<container>/<blob>?comp=tags" -Method PUT -Headers $headers -Body $body -ContentType "application/xml"
   ```

4. **Role requirements**: Blob tag operations require `Storage Blob Data Owner` role — `Storage Blob Data Contributor` is insufficient.

5. **Key-based auth may be blocked**: Azure Policy can enforce `allowSharedKeyAccess = false` at the subscription level, overriding Bicep/ARM settings; use `--auth-mode login` with adequate RBAC.

6. **HNS migration validation creates `hnsonerror` container**: Delete it between retries, otherwise stale error files cause confusion.

---

## 3. Fabric Connection — Replaced AzureBlobs with AzureDataLakeStorage

### What was done

- Deleted old Fabric connection (`8c8d4d26-dfd1-4cf0-8413-be777a36a585`, type `AzureBlobs`)
- Discovered ADLS connection parameters via `GET /v1/connections/supportedConnectionTypes` filtered by `type == "AzureDataLakeStorage"`
- Created new Fabric connection using ADLS Gen 2 with Workspace Identity

### Old connection (deleted)

| Property | Value |
|----------|-------|
| ID | `8c8d4d26-dfd1-4cf0-8413-be777a36a585` |
| Type | `AzureBlobs` |
| Endpoint | `https://stfhiriecaacmlqodcs.blob.core.windows.net/` |

### New connection

| Property | Value |
|----------|-------|
| Name | `FHIR-Export-ADLS-Connection` |
| ID | `6269f226-c2fe-4d85-80eb-b46d4f37cfaf` |
| Type | `AzureDataLakeStorage` |
| Connectivity | `ShareableCloud` |
| Auth | `WorkspaceIdentity` |
| Privacy Level | `Organizational` |
| Server | `https://stfhiriecaacmlqodcs.dfs.core.windows.net` |
| Path | `/fhir-export` |

### Connection type differences — AzureBlobs vs AzureDataLakeStorage

| | AzureBlobs | AzureDataLakeStorage |
|--|-----------|---------------------|
| Parameters | `account`, `domain` | `server`, `path` |
| Endpoint | `blob.core.windows.net` | `dfs.core.windows.net` |
| Shortcut target key | `azureBlobStorage` | `adlsGen2` |
| HNS required | No | Yes |

### Working request body (ADLS Gen 2)

```json
{
  "connectivityType": "ShareableCloud",
  "displayName": "FHIR-Export-ADLS-Connection",
  "connectionDetails": {
    "type": "AzureDataLakeStorage",
    "creationMethod": "AzureDataLakeStorage",
    "parameters": [
      { "dataType": "Text", "name": "server", "value": "https://stfhiriecaacmlqodcs.dfs.core.windows.net" },
      { "dataType": "Text", "name": "path", "value": "/fhir-export" }
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

## 4. OneLake Shortcut — Replaced AzureBlobStorage with AdlsGen2

### What was done

- Deleted two old shortcuts (both named `fhir-export`, at `/Files/Ingest/...` and `/Files/External/...`)
- Created new shortcut using `adlsGen2` target type

### Old shortcuts (deleted)

Both had target type `AzureBlobStorage` with `blob.core.windows.net` location.

### New shortcut

| Property | Value |
|----------|-------|
| Name | `fhir-export` |
| Lakehouse | `healthcare1_msft_bronze` |
| Path | `Files/External/Clinical/FHIR-NDJSON/AHDS-FHIR` |
| Target Type | `AdlsGen2` |
| Location | `https://stfhiriecaacmlqodcs.dfs.core.windows.net` |
| Subpath | `/fhir-export` |
| Connection ID | `6269f226-c2fe-4d85-80eb-b46d4f37cfaf` |

### Working request body

```json
{
  "path": "Files/External/Clinical/FHIR-NDJSON/AHDS-FHIR",
  "name": "fhir-export",
  "target": {
    "adlsGen2": {
      "location": "https://stfhiriecaacmlqodcs.dfs.core.windows.net",
      "subpath": "/fhir-export",
      "connectionId": "6269f226-c2fe-4d85-80eb-b46d4f37cfaf"
    }
  }
}
```

---

## 5. RBAC Updates

### Storage Blob Data Reader for admin security group

```bash
az role assignment create \
  --role "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1" \
  --assignee-object-id 63262afc-c15d-4bd7-9153-2a44e0a44936 \
  --assignee-principal-type Group \
  --scope /subscriptions/9bbee190-.../storageAccounts/stfhiriecaacmlqodcs
```

---

## Resource Reference (Updated)

| Resource | Value |
|----------|-------|
| Storage Account | `stfhiriecaacmlqodcs` |
| Storage Type | **ADLS Gen 2 (HNS enabled)** |
| DFS Endpoint | `https://stfhiriecaacmlqodcs.dfs.core.windows.net` |
| Fabric Connection ID | `6269f226-c2fe-4d85-80eb-b46d4f37cfaf` |
| Fabric Connection Type | `AzureDataLakeStorage` |
| Shortcut Target Type | `AdlsGen2` |
| Previous Connection ID | `8c8d4d26-dfd1-4cf0-8413-be777a36a585` (deleted) |
