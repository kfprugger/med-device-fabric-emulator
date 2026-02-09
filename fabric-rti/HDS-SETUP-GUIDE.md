# Healthcare Data Solutions — Clinical Foundations Setup Guide

This guide walks through deploying **Healthcare Data Solutions (HDS) Clinical Foundations** in Microsoft Fabric and connecting it to the existing Azure FHIR Service for enriched clinical alerts.

## Prerequisites

| Requirement | Status |
|-------------|--------|
| Azure FHIR Service deployed | ✅ `deploy.ps1` / `deploy-fhir.ps1` |
| FHIR data loaded (Synthea patients + devices) | ✅ FHIR Loader job |
| Fabric workspace created | ✅ `deploy-fabric-rti.ps1` |
| Eventhouse + Eventstream created | ✅ `deploy-fabric-rti.ps1` |
| Contributor + User Access Admin roles on Azure RG | Required for Marketplace offer |
| Fabric capacity (trial or paid) | Required |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│ Azure                                                   │
│  FHIR Service ──($export)──▶ ADLS Gen2 (NDJSON files)  │
└───────────────────────────────────┬─────────────────────┘
                                    │
                        OneLake Shortcut (BYOS)
                                    │
┌───────────────────────────────────▼─────────────────────┐
│ Microsoft Fabric — Healthcare Data Solutions             │
│                                                         │
│  Bronze Lakehouse          Silver Lakehouse              │
│  ┌─────────────────┐       ┌──────────────────────────┐ │
│  │ ClinicalFhir    │──────▶│ Patient     (7,800 rows) │ │
│  │ (staging table)  │       │ Device      (100 rows)   │ │
│  │                  │       │ Condition   (244K rows)  │ │
│  │                  │       │ Observation (2.8M rows)  │ │
│  │                  │       │ Encounter   (363K rows)  │ │
│  │                  │       │ MedicationRequest (250K) │ │
│  └─────────────────┘       └──────────────────────────┘ │
│                                       │                  │
│                          External Table / Shortcut       │
│                                       │                  │
│  Eventhouse (MasimoKQLDB)             │                  │
│  ┌────────────────────────────────────▼─────┐           │
│  │ TelemetryRaw (real-time)                  │           │
│  │ + JOIN SilverPatient, SilverCondition     │           │
│  │ = Enriched Clinical Alerts                │           │
│  └──────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────┘
```

## Step 1: Deploy Healthcare Data Solutions

1. Sign in to the [Fabric portal](https://app.fabric.microsoft.com)
2. Navigate to your workspace (default: **med-device-real-time**)
3. Select **New item** → search for **Healthcare data solutions**
4. Complete the **Setup wizard**:
   - Accept the terms
   - Select your workspace
   - Click **Create**

This creates the HDS environment in your workspace.

## Step 2: Deploy Healthcare Data Foundations

Healthcare Data Foundations is the prerequisite capability that provides the medallion lakehouse architecture.

1. From the HDS home page, select **Healthcare data foundations**
2. Click **Deploy to workspace**
3. Wait for deployment to complete (a few minutes)

### Artifacts Created

| Artifact | Type | Purpose |
|----------|------|---------|
| `healthcare#_environment` | Environment | Spark 3.4 runtime with required libraries |
| `healthcare#_msft_config_notebook` | Notebook | Global configuration values |
| `healthcare#_msft_bronze_silver_flatten` | Notebook | Flattens ClinicalFhir → Silver tables |
| `healthcare#_msft_fhir_ndjson_bronze_ingestion` | Notebook | Ingests NDJSON files into Bronze |
| `healthcare#_msft_raw_process_movement` | Notebook | Moves processed files |
| `healthcare#_msft_fhir_flattening_sample` | Notebook | Sample flattening patterns |
| `healthcare#_msft_clinical_data_foundation_ingestion` | Pipeline | End-to-end ingestion pipeline |
| Admin Lakehouse | Lakehouse | Config + execution tracking |
| Bronze Lakehouse | Lakehouse | Raw/staged FHIR data |
| Silver Lakehouse | Lakehouse | Flattened FHIR R4 tables |

## Step 3: Deploy AHDS Data Export (Azure Marketplace)

This step creates the Azure-side components that export FHIR data to storage.

1. Go to the [Azure portal](https://portal.azure.com) → **Create a resource**
2. Search for **Healthcare data solutions in Microsoft Fabric**
3. Click **Create** and configure:

| Parameter | Value |
|-----------|-------|
| Resource Group | `rg-medtech-sys-identity` |
| Region | `eastus` |
| FHIR Server URI | `https://hdwsiecaacmlqodcs-fhiriecaacmlqodcs.fhir.azurehealthcareapis.com` |
| Export Start Time | Set to your desired start |

4. Wait for deployment to complete
5. Verify resources created:
   - Azure Function App (`msft-func-datamanager-export-*`)
   - Key Vault (for function keys)
   - Storage Account (FHIR export destination)

### Required RBAC Assignments

| Role | Principal | Scope |
|------|-----------|-------|
| **FHIR Data Exporter** | Function App (Managed Identity) | FHIR Service |
| **Storage Blob Data Contributor** | FHIR Service (Managed Identity) | Export Storage Account |

## Step 4: Deploy AHDS Data Export Capability in Fabric

1. Return to the HDS home page in Fabric
2. Select the **Azure Health Data Services - Data export** tile
3. Click **Deploy to workspace**
4. Configure:
   - **Azure Key Vault**: Name of the Key Vault deployed in Step 3
   - **Maximum polling days**: `3` (recommended)
5. Click **Deploy**

### Artifacts Created

| Artifact | Type | Purpose |
|----------|------|---------|
| `healthcare#_msft_ahds_fhirservice_export` | Notebook | Triggers FHIR $export API |
| `healthcare#_msft_clinical_ahds_fhirservice_export` | Pipeline | End-to-end export + transform |

## Step 5: Create OneLake Shortcut

Link the Azure storage (where FHIR $export writes NDJSON files) to the Bronze Lakehouse.

1. Open the **Bronze Lakehouse** in your workspace
2. Navigate to **Files** → right-click → **New shortcut**
3. Select **Azure Data Lake Storage Gen2**
4. Enter the storage account URL from the Marketplace deployment
5. Create the shortcut at this path:
   ```
   Files/External/Clinical/FHIR-NDJSON/AHDS-FHIR/<ShortcutName>
   ```

## Step 6: Update Configuration

1. Open **Admin Lakehouse** → `Files/system-configurations/deploymentParametersConfiguration.json`
2. Find the `source_path_pattern` under `healthcare#_msft_fhir_ndjson_bronze_ingestion`
3. Update it to use the shortcut path:
   ```
   abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<bronze_lakehouse_id>/Files/External/Clinical/FHIR-NDJSON/AHDS-FHIR
   ```

## Step 7: Run the Clinical Pipeline

1. Open `healthcare#_msft_clinical_ahds_fhirservice_export` pipeline
2. Click **Run**
3. The pipeline will:
   - Export FHIR data via `$export` API to ADLS Gen2
   - Ingest NDJSON files into Bronze `ClinicalFhir` table
   - Flatten data into Silver lakehouse FHIR R4 tables

### Expected Silver Lakehouse Data

| Table | Expected Rows | Description |
|-------|--------------|-------------|
| Patient | ~7,800 | Demographics, identifiers, names |
| Device | ~100 | Masimo Radius-7 pulse oximeters |
| Condition | ~244,800 | Diagnoses with SNOMED CT codes |
| Observation | ~2,801,600 | Vital signs, lab results |
| Encounter | ~363,600 | Admissions, visits |
| MedicationRequest | ~250,000 | Medication orders |
| Procedure | ~1,019,500 | Surgical/clinical procedures |
| Immunization | ~116,800 | Vaccination records |
| Basic | ~100 | Device-patient associations |

## Step 8: Connect Silver Lakehouse to Eventhouse

Create external tables in the KQL Database so clinical alert functions can query FHIR patient data.

1. Open `MasimoKQLDB` in the Eventhouse
2. Get the Silver Lakehouse ID from its URL in the Fabric portal
3. Run the external table creation scripts from:
   ```
   fabric-rti/kql/04-hds-enrichment-example.kql
   ```
   (Uncomment the `.create external table` commands and replace placeholders)

4. Verify the external tables work:
   ```kql
   external_table('SilverPatient') | take 5
   external_table('SilverCondition') | take 5
   ```

5. Once verified, uncomment and run the enriched `fn_ClinicalAlerts` function from the same file.

## Step 9: Verify Enriched Alerts

After everything is connected, run:

```kql
fn_ClinicalAlerts(5)
```

You should see alerts enriched with:
- ✅ Patient name and ID
- ✅ Qualifying conditions (COPD, CHF, Asthma, etc.)
- ✅ Severity escalation based on patient risk factors

## Troubleshooting

### FHIR Export Returns 401 Unauthorized
Ensure the Function App managed identity has **FHIR Data Exporter** role on the FHIR service.

### FHIR Export Returns 409 Conflict
Only one export can run at a time. Wait for the previous export to complete.

### Silver Lakehouse Tables Are Empty
1. Check the Bronze `ClinicalFhir` table for data
2. Verify the `source_path_pattern` configuration points to the correct shortcut
3. Check Admin Lakehouse `BusinessEvents` table for error details

### External Table Query Fails
1. Verify the `abfss://` path matches your workspace ID and lakehouse ID
2. Ensure the Eventhouse has permissions to read the Silver Lakehouse
3. Both items must be in the same workspace for cross-item queries

## Data Model Reference

The Silver Lakehouse uses the **FHIR R4** data model with these additions:

| Column | Description |
|--------|-------------|
| `msftCreatedDatetime` | When the record was first created in Silver |
| `msftModifiedDatetime` | Last modification timestamp |
| `msftFilePath` | Source file path in Bronze lakehouse |
| `msftSourceSystem` | Source system identifier |
| `idOrig` | Original FHIR resource ID (before SHA256 re-keying) |
| `identifierOrig` | Original identifiers preserved |
