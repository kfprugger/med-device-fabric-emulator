# CMS Quality & Claims Integration Plan

## Executive Summary

This plan adds **claims/insurance data** and **CMS quality measurement** capabilities to the Medical Device FHIR Integration Platform. We leverage Synthea's built-in claims generation (ExplanationOfBenefit, Claim, Coverage), add new Fabric tables, extend the ontology with payer/provider entities inspired by the [Fabric-Payer-Provider-HealthCare-Demo](https://github.com/rasgiza/Fabric-Payer-Provider-HealthCare-Demo), and create a Power BI report for CMS quality analytics.

---

## Part 1: Claims Data Generation — What Synthea Already Does

### Synthea Natively Generates Claims

Synthea **already generates** claims-related FHIR R4 resources — we just aren't including them. The current `synthea.properties` explicitly filters resources via `exporter.fhir.included_resources`, excluding claims types.

**Currently included:**
```
Patient,Encounter,Condition,Observation,Procedure,MedicationRequest,
Immunization,DiagnosticReport,CarePlan,CareTeam,AllergyIntolerance,
Organization,Practitioner,PractitionerRole,Location
```

**What Synthea generates but we're excluding:**
| FHIR Resource | What It Contains |
|--------------|-----------------|
| `Claim` | Individual healthcare claim submitted for an encounter (billing codes, amounts) |
| `ExplanationOfBenefit` (EOB) | Adjudicated claim — what was billed, what the payer covered, what the patient owes (the richest resource) |
| `Coverage` | Patient insurance coverage details (payer, plan, period, subscriber) |

### How to Enable Claims Generation

**Change 1** — Update `synthea/synthea.properties`:
```properties
# Add claims resources to the included list
exporter.fhir.included_resources = Patient,Encounter,Condition,Observation,Procedure,MedicationRequest,Immunization,DiagnosticReport,CarePlan,CareTeam,AllergyIntolerance,Organization,Practitioner,PractitionerRole,Location,Claim,ExplanationOfBenefit,Coverage
```

That's it for generation. Synthea's built-in health economics engine handles:
- **Cost assignment**: Triangular distribution from procedure/medication/encounter cost tables
- **Payer assignment**: Patients are assigned to Medicare, Medicaid, or private payers based on demographics/eligibility
- **Claims adjudication**: Each encounter generates a Claim → ExplanationOfBenefit with billed/allowed/paid amounts
- **Coverage periods**: Coverage resources track insurance enrollment periods

### What the EOB Contains (Key Fields for Quality)

Each `ExplanationOfBenefit` links:
- `patient` → Patient reference
- `provider` → Organization/Practitioner reference
- `facility` → Location reference
- `insurance.coverage` → Coverage reference (payer)
- `item[]` → Line items with CPT/HCPCS codes, service dates, costs
- `item[].adjudication[]` → Billed, allowed, paid, copay, deductible amounts
- `diagnosis[]` → ICD-10 codes (principal + secondary)
- `procedure[]` → Procedure codes performed
- `total[]` → Claim-level totals (billed, paid, patient responsibility)
- `type` → institutional | professional | pharmacy | oral
- `outcome` → complete | partial | error

---

## Part 2: CMS Quality Measures — What They Actually Check

### What is CQL / eCQM?

**Clinical Quality Language (CQL)** is the ANSI-certified expression language used by CMS to define **electronic Clinical Quality Measures (eCQMs)**. Since 2019, CMS requires all eCQM specifications to use CQL.

Each eCQM defines:
- **Initial Population (IP)**: Who is eligible to be measured (age, condition, encounter type)
- **Denominator**: Subset of IP that should receive the care
- **Numerator**: Subset of denominator that actually received the correct care
- **Exclusions/Exceptions**: Valid reasons for not meeting the measure

**Quality Rate = Numerator / Denominator × 100%**

### CMS Quality Measures Relevant to Our Patient Population

Given our Synthea modules (asthma, COPD, CHF, COVID-19, metabolic syndrome, lung cancer), these CMS/HEDIS measures are directly applicable:

#### Tier 1 — Measures We Can Compute from Existing + Claims Data

| Measure ID | Name | What It Checks | Data Needed |
|-----------|------|---------------|-------------|
| **CMS122v12** | Diabetes: Hemoglobin A1c Poor Control | % of diabetic patients 18-75 with HbA1c > 9% or no test | Condition (diabetes dx), Observation (HbA1c labs), Encounter, Coverage |
| **CMS165v12** | Controlling High Blood Pressure | % of hypertensive patients 18-85 with BP < 140/90 | Condition (HTN dx), Observation (BP readings), Encounter |
| **CMS134v12** | Diabetes: Medical Attention for Nephropathy | % of diabetic patients with nephropathy screening/treatment | Condition, Observation (urine albumin), MedicationRequest (ACE/ARB) |
| **CMS69v12** | Preventive Care: BMI Screening | % of patients 18+ with BMI recorded and follow-up plan | Observation (BMI), Encounter |
| **CMS127v12** | Pneumococcal Vaccination Status | % of patients 65+ who received pneumococcal vaccine | Immunization, Patient (age), Encounter |
| **CMS147v13** | Preventive Care: Influenza Immunization | % of patients 6mo+ who received flu vaccine | Immunization, Encounter |
| **CMS156v12** | Use of High-Risk Medications in Older Adults | % of patients 65+ prescribed ≥2 high-risk meds | MedicationRequest, Patient (age) |

#### Tier 2 — Measures Enhanced by Claims/EOB Data

| Measure ID | Name | Claims Value-Add |
|-----------|------|-----------------|
| **CMS159v12** | Depression Remission or Response | Claims show follow-up encounters happened and were billed |
| **CMS144v12** | Heart Failure: Beta-Blocker for LVSD | Claims confirm prescription fills (not just orders) via pharmacy claims |
| **CMS145v12** | Coronary Artery Disease: Beta-Blocker | Claims validate medication dispensing |
| **CMS135v12** | Heart Failure: ACE/ARB for LVSD | Claims confirm fill + adherence via PDC |

#### Tier 3 — HEDIS-Aligned Measures (Payer-Focused)

| HEDIS Measure | Name | Why It Matters |
|-------------|------|---------------|
| **PDC-DR** | Proportion of Days Covered — Diabetes | CMS Star Rating triple-weighted adherence measure |
| **PDC-RASA** | PDC — RAS Antagonists (ACE/ARB) | Star Rating triple-weighted |
| **PDC-STA** | PDC — Statins | Star Rating triple-weighted |
| **COL** | Colorectal Cancer Screening | Preventive care gap |
| **BCS** | Breast Cancer Screening | Preventive care gap |
| **CBP** | Controlling Blood Pressure | Overlaps CMS165 |
| **CDC** | Comprehensive Diabetes Care | Composite of CMS122/134 |

### How to Compute Quality Without a CQL Engine

Rather than running a full CQL evaluation engine, we can **translate the measure logic into SQL/KQL** since we know the exact data model. Each measure becomes a SQL query pattern:

```sql
-- Example: CMS122 — Diabetes HbA1c Poor Control
-- Denominator: Patients 18-75 with diabetes diagnosis + qualifying encounter in measurement year
-- Numerator: Those with most recent HbA1c > 9% OR no HbA1c test in measurement year

WITH denominator AS (
    SELECT DISTINCT p.patient_id
    FROM dim_patient p
    JOIN fact_claim c ON p.patient_key = c.patient_key
    JOIN fact_diagnosis d ON c.encounter_key = d.encounter_key
    WHERE d.icd_code LIKE 'E11%'  -- Type 2 Diabetes
      AND p.age BETWEEN 18 AND 75
      AND c.claim_date_key >= @measurement_year_start
),
latest_hba1c AS (
    SELECT patient_id, MAX(observation_value) as last_hba1c
    FROM fact_observation
    WHERE loinc_code = '4548-4'  -- HbA1c
      AND effective_date >= @measurement_year_start
    GROUP BY patient_id
)
SELECT 
    COUNT(CASE WHEN h.last_hba1c > 9.0 OR h.last_hba1c IS NULL THEN 1 END) as numerator,
    COUNT(*) as denominator,
    ROUND(100.0 * COUNT(CASE WHEN h.last_hba1c > 9.0 OR h.last_hba1c IS NULL THEN 1 END) / COUNT(*), 1) as rate
FROM denominator d
LEFT JOIN latest_hba1c h ON d.patient_id = h.patient_id
```

---

## Part 3: New Fabric Tables — Star Schema for Claims & Quality

### New Silver Layer Tables (FHIR → Normalized)

| Table | Source | Key Columns |
|-------|--------|-------------|
| `dbo.ExplanationOfBenefit` | FHIR EOB resources via $export | patient_ref, provider_ref, facility_ref, coverage_ref, type, outcome, total_billed, total_paid, service_date, diagnosis_codes[], procedure_codes[] |
| `dbo.Claim` | FHIR Claim resources | patient_ref, provider_ref, type, status, total_amount, created_date |
| `dbo.Coverage` | FHIR Coverage resources | patient_ref, payor_ref, type (Medicare/Medicaid/Private), period_start, period_end, subscriber_id |

### New Gold Layer Tables (Star Schema — Modeled After Payer-Provider Demo)

#### Dimension Tables

| Table | Key Columns | Notes |
|-------|-------------|-------|
| `dim_payer` | payer_key, payer_id, payer_name, payer_type (Medicare/Medicaid/Private/Uninsured) | Derived from Coverage.payor |
| `dim_diagnosis` | diagnosis_key, icd_code, icd_description, icd_category, is_chronic | Derived from Condition + EOB diagnosis codes |

#### Fact Tables

| Table | Key Columns | Notes |
|-------|-------------|-------|
| `fact_claim` | claim_key, claim_id, patient_key, encounter_key, payer_key, claim_type, claim_status, billed_amount, allowed_amount, paid_amount, patient_responsibility, service_date_key, denial_flag | Derived from ExplanationOfBenefit |
| `fact_claim_line` | line_key, claim_key, cpt_code, cpt_description, quantity, line_billed, line_paid, adjudication_status | EOB line items |
| `fact_diagnosis` | fact_diagnosis_key, encounter_key, patient_key, diagnosis_key, diagnosis_sequence (principal/secondary), diagnosis_type | From EOB.diagnosis[] + Condition |

#### Aggregate / Quality Tables

| Table | Key Columns | Notes |
|-------|-------------|-------|
| `agg_quality_measures` | patient_key, measure_id, measure_name, measurement_year, in_initial_population, in_denominator, in_numerator, in_exclusion, quality_met | Pre-computed CMS measure results per patient |
| `agg_quality_summary` | measure_id, measure_name, measurement_year, denominator_count, numerator_count, exclusion_count, quality_rate, benchmark_rate | Aggregated rates for reporting |
| `care_gaps` | patient_key, measure_id, gap_type, gap_status (open/closed), days_overdue, recommended_action | Actionable gaps per patient |
| `agg_medication_adherence` | patient_key, medication_class, pdc_score, adherence_category (Adherent ≥80% / Non-adherent), gap_days, total_fills | PDC calculation from pharmacy claims |

---

## Part 4: Ontology Extension — New Entities from Payer-Provider Demo

### Entities to Add (Inspired by rasgiza/Fabric-Payer-Provider-HealthCare-Demo)

Comparing our current 9-entity `ClinicalDeviceOntology` with the Payer-Provider demo's 12-entity ontology, these entities are directly relevant:

| New Entity | Source Table | Applicable? | Rationale |
|-----------|-------------|-------------|-----------|
| **Claim** | `fact_claim` | **YES** | Core claims entity with denial risk, amounts, payer link |
| **Payer** | `dim_payer` | **YES** | Insurance carrier dimension — enables payer-level quality analysis |
| **Diagnosis** | `dim_diagnosis` | **YES** | ICD-10 dimension — enables condition-based quality filtering |
| **Prescription** | `fact_claim_line` (pharmacy type) | **MAYBE** | We already have MedicationRequest; pharmacy claims add fill/adherence data |
| **MedicationAdherence** | `agg_medication_adherence` | **YES** | PDC scores for Star Rating measures — critical for CMS quality |
| **CommunityHealth (SDOH)** | Not in scope yet | **FUTURE** | Zip-code-level social determinants — powerful for equity analysis |
| **PatientDiagnosis** | `fact_diagnosis` | **YES** | Bridge entity linking patients ↔ encounters ↔ diagnoses with sequence/type |

### New Ontology Relationships

| Relationship | Source → Target | Join Logic |
|-------------|----------------|------------|
| **Patient hasClaim** | Patient → Claim | `Patient.patientId = Claim.patient_key` |
| **Claim paidBy** | Claim → Payer | `Claim.payer_key = Payer.payer_key` |
| **Claim hasDiagnosis** | Claim → Diagnosis | via `fact_diagnosis` bridge |
| **Patient coveredBy** | Patient → Payer | via `Coverage` → `dim_payer` |
| **Patient hasDiagnosis** | Patient → PatientDiagnosis | `patient_key` |
| **PatientDiagnosis isDiagnosis** | PatientDiagnosis → Diagnosis | `diagnosis_key` |
| **Patient hasAdherence** | Patient → MedicationAdherence | `patient_key` |
| **Encounter hasDiagnosis** | Encounter → PatientDiagnosis | `encounter_key` |

### Updated Ontology Summary

| # | Entity | Source | Existing? |
|---|--------|--------|-----------|
| 1 | Patient | Silver Lakehouse | ✅ Existing |
| 2 | Device | Silver Lakehouse | ✅ Existing |
| 3 | Encounter | Silver Lakehouse | ✅ Existing |
| 4 | Condition | Silver Lakehouse | ✅ Existing |
| 5 | MedicationRequest | Silver Lakehouse | ✅ Existing |
| 6 | Observation | Silver Lakehouse | ✅ Existing |
| 7 | DeviceAssociation | Silver Lakehouse | ✅ Existing |
| 8 | DeviceTelemetry | Eventhouse | ✅ Existing |
| 9 | ClinicalAlert | Eventhouse | ✅ Existing |
| 10 | **Claim** | Gold Lakehouse | 🆕 NEW |
| 11 | **Payer** | Gold Lakehouse | 🆕 NEW |
| 12 | **Diagnosis** | Gold Lakehouse | 🆕 NEW |
| 13 | **PatientDiagnosis** | Gold Lakehouse | 🆕 NEW |
| 14 | **MedicationAdherence** | Gold Lakehouse | 🆕 NEW |

**Total: 14 entities, 16 relationships** (up from 9 entities, 8 relationships)

---

## Part 5: Power BI Report — CMS Quality Dashboard

### Report Name: `CMS Quality Scorecard`

### Page 1: Quality Measures Overview
- **Card KPIs**: Overall quality rate, # measures met, # care gaps open, # patients measured
- **Table/Matrix**: All CMS measures with denominator, numerator, rate, benchmark comparison (red/yellow/green)
- **Bar Chart**: Quality rate by measure — sorted worst to best
- **Trend Line**: Quality rate over measurement periods (if multi-period data)

### Page 2: Measure Deep-Dive
- **Slicer**: Select individual CMS measure
- **Donut**: Numerator vs Denominator breakdown (met / not met / excluded)
- **Patient List**: Patients in denominator NOT in numerator (care gaps) — with demographics
- **Decomposition Tree**: Rate by gender → age group → payer → condition

### Page 3: Claims Analytics
- **Card KPIs**: Total billed, total paid, collection rate, denial rate, avg claim amount
- **Waterfall**: Revenue leakage (billed → allowed → paid → patient responsibility)
- **Stacked Bar**: Claims by payer with denial rates
- **Matrix**: Denial reasons by payer × claim type
- **Scatter**: Billed amount vs paid amount by provider (outlier detection)

### Page 4: Medication Adherence (Star Ratings)
- **Gauge Charts**: PDC rates for 3 triple-weighted measures (PDC-DR, PDC-RASA, PDC-STA)
- **Grouped Bar**: Adherence by medication class — Adherent (≥80%) vs Non-adherent
- **Patient List**: Non-adherent patients with gap days, medication, recommended action
- **Line Chart**: Adherence trend over time

### Page 5: Care Gap Closure
- **Map Visual**: Open care gaps by patient location (zip-code level)
- **Matrix**: Open gaps by measure × facility — heatmap coloring
- **Priority List**: Top care gaps ranked by days overdue × clinical severity
- **Stacked Bar**: Gap status (open/closed) by measure over time

### Page 6: Payer Performance
- **Bar Chart**: Quality rate by payer (Medicare vs Medicaid vs Commercial)
- **Matrix**: Measure performance by payer — conditional formatting
- **Scatter**: Denial rate vs quality rate by payer
- **KPI Cards**: Best/worst performing payer, payer with most care gaps

### Semantic Model Additions

New DAX measures for the `CMS Quality Scorecard` report:

```dax
// Quality Rate
Quality Rate = 
    DIVIDE(
        COUNTROWS(FILTER(agg_quality_measures, [quality_met] = 1)),
        COUNTROWS(FILTER(agg_quality_measures, [in_denominator] = 1)),
        0
    )

// Denial Rate
Denial Rate = 
    DIVIDE(
        COUNTROWS(FILTER(fact_claim, [denial_flag] = 1)),
        COUNTROWS(fact_claim),
        0
    )

// PDC Score (Proportion of Days Covered)
Avg PDC Score = AVERAGE(agg_medication_adherence[pdc_score])

// Open Care Gaps
Open Care Gaps = COUNTROWS(FILTER(care_gaps, [gap_status] = "open"))

// Collection Rate
Collection Rate = DIVIDE(SUM(fact_claim[paid_amount]), SUM(fact_claim[billed_amount]), 0)
```

---

## Part 6: Implementation Phases

### Phase A — Enable Claims in Synthea (Low Effort)
1. Update `synthea.properties` to include `Claim,ExplanationOfBenefit,Coverage`
2. Update `fhir-loader/load_fhir.py` to handle new resource types during bundle upload
3. Verify FHIR $export includes new resource types (it should by default)
4. Update HDS pipeline to process new resource types into Silver Lakehouse

### Phase B — Create Gold Star Schema (Medium Effort)
1. Create materialization notebook: Silver → Gold transform for claims tables
2. Build `dim_payer`, `dim_diagnosis`, `fact_claim`, `fact_claim_line`, `fact_diagnosis`
3. Compute quality measures → `agg_quality_measures`, `agg_quality_summary`
4. Compute medication adherence → `agg_medication_adherence`
5. Identify care gaps → `care_gaps`

### Phase C — Extend Ontology (Medium Effort)
1. Add 5 new entity types to `ClinicalDeviceOntology`
2. Add 8 new relationship types
3. Update Data Agents with new instructions covering claims/quality queries
4. Update `phase-4/deploy-ontology.ps1` with new entities

### Phase D — Power BI Report (Medium Effort)
1. Create `CMS Quality Scorecard` semantic model (Direct Lake)
2. Build 6-page report with measures described above
3. Add deployment script for the new report
4. Integrate into orchestrator UI deploy flow

### Phase E — Quality Computation Notebook (Medium Effort)
1. Create `NB_Compute_Quality_Measures.py` — translates CMS eCQM logic to SQL
2. Runs after Gold ETL, computes all Tier 1 + Tier 2 measures
3. Outputs to `agg_quality_measures` and `care_gaps` tables
4. Add to pipeline as post-Gold step

---

## Key Decisions Needed

1. **Re-run Synthea or post-process?** — Adding claims to Synthea properties requires re-generating all patient data (new container run). Alternatively, we could generate claims synthetically from existing encounter/procedure/medication data in a post-processing step (more control, no re-generation needed).

2. **Gold Lakehouse location** — Use existing Silver Lakehouse for claims tables, or create a new Gold Lakehouse (`lh_gold_quality`) similar to the Payer-Provider demo's `lh_gold_curated`?

3. **Ontology binding** — New claims entities bind to Gold Lakehouse, while existing entities bind to Silver Lakehouse + Eventhouse. This cross-lakehouse ontology is supported but needs testing.

4. **Quality measurement period** — Use Synthea's 10-year history window or restrict to most recent year?
