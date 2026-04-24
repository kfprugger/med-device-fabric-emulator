# Phase 5 — CMS Quality & Claims

> **Prerequisites**: Phase 1 + Phase 2 deployed (Silver Lakehouse with FHIR tables populated). Synthea must be configured to generate `Claim`, `ExplanationOfBenefit`, and `Coverage` resources.

## Overview

Phase 5 adds **claims analytics** and **CMS quality measurement** capabilities to the platform. It materializes Silver FHIR data into a Gold star schema, computes 7 CMS electronic Clinical Quality Measures (eCQMs) and 3 HEDIS medication adherence scores, and deploys a 6-page CMS Quality Scorecard Power BI report.

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
4. **Materialization notebook** (`materialize_claims_quality.py`) transforms Silver → Gold star schema
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
