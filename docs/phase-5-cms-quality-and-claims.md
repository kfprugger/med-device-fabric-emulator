# Stage 6 — CMS Quality & Performance

⏱️ **Typical Duration:** ~10 minutes | 🛠️ **Fabric Workloads:** Healthcare Data Solutions, Power BI | 🔑 **Min Roles:** Azure Owner, Fabric Admin

---

> [!NOTE]
> **Deployment Prerequisites:**
> Before running this phase, ensure Stage 2 is complete, Synthea is configured to generate `Claim`, `ExplanationOfBenefit`, and `Coverage` resources, and the Silver Lakehouse tables are populated. Refer to the centralized [📋 Prerequisites & Requirements](file:///Users/joey/git/med-device-fabric-emulator/README.md#📋-prerequisites--requirements) in the root repository folder.

---

Stage 6 implements the CMS Quality & Performance standard, adding **claims analytics** and **CMS quality measurement** capabilities to the platform. It materializes Silver FHIR data into a Gold star schema, computes 7 CMS electronic Clinical Quality Measures (eCQMs) and 3 HEDIS medication adherence scores, and deploys a 6-page CMS Quality Scorecard Power BI report.

## Architecture

```
Silver Lakehouse (FHIR R4)                      Gold Reporting Lakehouse
┌──────────────────────┐                         ┌──────────────────────────┐
│ ExplanationOfBenefit │─── materialize ─────────▶│ fact_claim               │
│ Coverage             │─── materialize ─────────▶│ dim_payer                │
│ Condition            │─── materialize ─────────▶│ dim_diagnosis            │
│                      │─── materialize ─────────▶│ fact_diagnosis           │
│ Patient              │                          │                          │
│ Observation          │─── quality compute ─────▶│ agg_quality_measures     │
│ MedicationRequest    │                          │ agg_quality_summary      │
│ Immunization         │─── adherence calc ──────▶│ agg_medication_adherence │
│ Encounter            │─── gap analysis ────────▶│ care_gaps                │
└──────────────────────┘                         └──────────┬───────────────┘
                                                            │
                                                            ▼
                                                 ┌──────────────────────┐
                                                 │ CMS Quality Scorecard│
                                                 │ (Direct Lake Report) │
                                                 │  6 pages, 14 DAX     │
                                                 └──────────────────────┘
```

## Data Flow

1. **Synthea** generates patients with `Claim`, `ExplanationOfBenefit`, and `Coverage` FHIR resources (enabled via `synthea.properties`)
2. **FHIR Loader** uploads all resources to FHIR Service (no filtering by type)
3. **FHIR $export** → ADLS Gen2 → Bronze Lakehouse → Silver Lakehouse (standard HDS pipeline)
4. **Materialization notebook** ([`phase-5/materialize_claims_quality.py`](../phase-5/materialize_claims_quality.py)) transforms Silver → Gold star schema
5. **CMS Quality Scorecard** report binds to Gold Lakehouse via Direct Lake

## CMS Quality Measures Computed

| Measure ID | Name | What It Checks |
|-----------|------|---------------|
| **CMS122v12** | Diabetes: Hemoglobin A1c Poor Control | Diabetic patients 18-75 with HbA1c > 9% or no test |
| **CMS165v12** | Controlling High Blood Pressure | Hypertensive patients 18-85 with BP ≥ 140/90 |
| **CMS69v12** | Preventive Care: BMI Screening | Adults 18+ with BMI recorded |
| **CMS127v12** | Pneumococcal Vaccination Status | Patients 65+ with pneumococcal vaccine |
| **CMS147v13** | Preventive Care: Influenza Immunization | Patients 1+ with flu vaccine |
| **CMS134v12** | Diabetes: Medical Attention for Nephropathy | Diabetic patients with albumin test or ACE/ARB |
| **CMS144v12** | Heart Failure: Beta-Blocker Therapy | CHF patients 18+ on beta-blocker |

## HEDIS Medication Adherence (PDC)

| Class | Drug Examples | CMS Star Rating Weight |
|-------|---------------|----------------------|
| **PDC-DR** (Diabetes) | metformin, glipizide, insulin, empagliflozin | Triple-weighted |
| **PDC-RASA** (RAS Antagonists) | lisinopril, losartan, valsartan | Triple-weighted |
| **PDC-STA** (Statins) | atorvastatin, rosuvastatin, simvastatin | Triple-weighted |

PDC (Proportion of Days Covered) ≥ 80% = **Adherent**; < 80% = **Non-Adherent**

## Gold Lakehouse Tables

| Table | Type | Row Estimate | Description |
|-------|------|-------------|-------------|
| `dim_payer` | Dimension | ~8 | Medicare, Medicaid, Commercial, Uninsured |
| `dim_diagnosis` | Dimension | ~1,250 | ICD-10 / SNOMED codes with chronic flag |
| `fact_claim` | Fact | ~48,000 | Claims from ExplanationOfBenefit with amounts |
| `fact_diagnosis` | Fact | ~125,000 | Encounter-level diagnoses |
| `agg_quality_measures` | Aggregate | ~35,000 | Patient × measure results |
| `agg_quality_summary` | Aggregate | 7 | Measure-level rates vs benchmarks |
| `agg_medication_adherence` | Aggregate | ~6,200 | PDC scores by drug class |
| `care_gaps` | Aggregate | ~12,400 | Open gaps with recommended actions |

## Power BI Report Pages

| Page | Title | Key Visuals |
|------|-------|-------------|
| 1 | Quality Measures Overview | KPI cards, measure rates vs benchmarks, bar chart |
| 2 | Measure Deep-Dive | Slicer per measure, decomposition tree, patient list |
| 3 | Claims Analytics | Billed/paid/denial KPIs, waterfall, payer breakdown |
| 4 | Medication Adherence | PDC gauges (3 classes), adherent vs non-adherent |
| 5 | Care Gap Closure | Priority list, gap status by measure |
| 6 | Payer Performance | Quality rate by payer, denial vs quality scatter |

## Payer-Specific Quality Stratification

Every patient is mapped to a primary payer through the latest active `Coverage` record. The `payer_category` column is denormalized onto **`fact_claim`**, **`agg_quality_measures`**, and **`agg_quality_summary`** so reports can compare Medicare / Medicaid / Commercial / Uninsured side-by-side without complex joins.

### Stratified DAX Measures (in `_Measures`)

| Domain | Measures |
|--------|----------|
| Quality rate | `Quality Rate (Medicare)`, `Quality Rate (Medicaid)`, `Quality Rate (Commercial)`, `Quality Rate (Uninsured)` |
| Revenue | `Total Paid (Medicare)`, `Total Paid (Medicaid)`, `Total Paid (Commercial)` |
| Collection efficiency | `Collection Rate (Medicare)`, `Collection Rate (Medicaid)`, `Collection Rate (Commercial)` |
| Denial risk | `Denial Rate (Medicare)`, `Denial Rate (Medicaid)`, `Denial Rate (Commercial)` |
| Population size | `Patients Measured (Medicare)`, `Patients Measured (Medicaid)`, `Patients Measured (Commercial)` |

### Suggested visuals for **Payer Performance** (page 6)
1. Clustered column: `Quality Rate (Medicare/Medicaid/Commercial)` × measure_id
2. Scatter: `Denial Rate` vs `Quality Rate` per payer (bubble size = `Total Paid`)
3. Slicer: `dim_payer[payer_category]` with cross-page filtering enabled
4. KPI cards: `Total Paid` per payer with YoY trend
5. Matrix: rows = measure_name, columns = payer_category, values = quality_rate (with conditional formatting against benchmark)

## Ontology Extensions

Phase 5 adds 5 entities to the `ClinicalDeviceOntology` (bringing total to 14):

| Entity | Source Table | Key Relationships |
|--------|-------------|-------------------|
| Claim | `fact_claim` | Patient → hasClaim → Claim |
| Payer | `dim_payer` | Claim → paidBy → Payer |
| Diagnosis | `dim_diagnosis` | PatientDiagnosis → isDiagnosis → Diagnosis |
| PatientDiagnosis | `fact_diagnosis` | Patient → hasDiagnosis → PatientDiagnosis |
| MedAdherence | `agg_medication_adherence` | Patient → hasAdherence → MedAdherence |

## Deployment

### Via Orchestrator UI
Toggle **Phase 5: CMS Quality & Claims** → "CMS Quality Scorecard" checkbox.

### Via CLI
```powershell
# Full deployment (all phases)
.\Deploy-All.ps1 -FabricWorkspaceName "med-device-rti-hds" -AdminSecurityGroup "sg-admins"

# Phase 5 only
.\Deploy-All.ps1 -FabricWorkspaceName "med-device-rti-hds" -Phase5

# Skip Phase 5
.\Deploy-All.ps1 ... -SkipQualityMeasures
```

## Inspiration

The claims data model and ontology entities are inspired by the [Fabric-Payer-Provider-HealthCare-Demo](https://github.com/rasgiza/Fabric-Payer-Provider-HealthCare-Demo) by Kwame Sefah, which implements a full payer/provider analytics solution with similar entities (Claim, Payer, Diagnosis, Prescription, MedicationAdherence, CommunityHealth).

---

### 🏁 Stage 6 Success Verification Checklist

Ensure all of the following components are verified before finalizing your deployment:

> [!IMPORTANT]
> **Stage 6 Verification Checkpoints:**
> - [ ] **Claims Data Loaded:** Confirm Silver Lakehouse contains raw claims tables, such as `ExplanationOfBenefit` and `Coverage`.
> - [ ] **Star Schema Materialized:** Spark notebook ran successfully and populated the 8 Gold star schema reporting tables inside the Gold lakehouse.
> - [ ] **Data Volume Check:** Run SQL queries to ensure `dbo.fact_claim` and chronic diagnoses in `dbo.dim_diagnosis` are populated.
> - [ ] **Quality Metrics computed:** Confirm measure aggregation tables like `dbo.agg_quality_summary` show results for eCQMs (CMS122, CMS165, etc.).
> - [ ] **PDC Medication Adherence calculated:** Verification that `dbo.agg_medication_adherence` contains computed values for statins, diabetes meds, and RAS antagonists.
> - [ ] **CMS Quality Scorecard Live:** Power BI scorecard report is successfully deployed, using Direct Lake connections to Gold tables.
> - [ ] **Payer Stratification Active:** Confirm Payer performance visuals compute stratified quality metrics correctly across Commercial, Medicare, and Medicaid.
> - [ ] **Extended Ontology Online:** The Fabric IQ `ClinicalDeviceOntology` is extended to 14 total entities (including Claims, Payers, and Diagnoses) with successful bindings to your Data Agents.

---

**Previous:** [← Stage 4 & 5 — Connected Semantic Intelligence & Bedside Alerting](phase-4-ontology-and-activator.md) · **Overview:** [← README](../README.md)
