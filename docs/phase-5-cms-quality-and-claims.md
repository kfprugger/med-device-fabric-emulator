# Stage 6 — Population Health & Quality

⏱️ **Typical Duration:** ~15 minutes | 🛠️ **Fabric Workloads:** Healthcare Data Solutions, Power BI, Data Activator | 🔑 **Min Roles:** Azure Owner, Fabric Admin

---

> [!NOTE]
> **Deployment Prerequisites:**
> Before running this phase, ensure Stage 2 is complete, Synthea is configured to generate `Claim`, `ExplanationOfBenefit`, and `Coverage` resources, and the Silver Lakehouse tables are populated. Refer to the centralized [📋 Prerequisites & Requirements](file:///Users/joey/git/med-device-fabric-emulator/README.md#📋-prerequisites--requirements) in the root repository folder.

---

Stage 6 implements Population Health & Quality analytics, adding **claims analytics**, **CMS quality measurement**, **Star Rating simulation**, **HCC risk adjustment**, **readmission risk prediction**, and **cost & utilization analytics** to the platform. It materializes Silver FHIR data into a Gold star schema (23 tables), and deploys a 10-page Population Health & Quality Dashboard Power BI report.

## Architecture

```
Silver Lakehouse (FHIR R4)                      Gold Reporting Lakehouse (23 tables)
┌──────────────────────┐                         ┌──────────────────────────┐
│ ExplanationOfBenefit │─── materialize ─────────▶│ fact_claim               │
│ Coverage             │─── materialize ─────────▶│ dim_payer                │
│ Condition            │─── materialize ─────────▶│ dim_diagnosis            │
│                      │─── materialize ─────────▶│ fact_diagnosis           │
│                      │                          │                          │
│ Patient              │─── quality compute ─────▶│ agg_quality_measures     │
│ Observation          │                          │ agg_quality_summary      │
│ MedicationRequest    │─── adherence calc ──────▶│ agg_medication_adherence │
│ Immunization         │─── gap analysis ────────▶│ care_gaps                │
│                      │                          │                          │
│                      │─── star rating ─────────▶│ star_rating_detail       │
│                      │                          │ star_rating_simulation   │
│                      │                          │                          │
│                      │─── HCC risk adj ────────▶│ dim_hcc                  │
│                      │                          │ fact_patient_hcc         │
│                      │                          │ agg_risk_scores          │
│                      │                          │ agg_risk_summary         │
│                      │                          │ revenue_opportunity      │
│                      │                          │                          │
│ Encounter            │─── readmission ML ──────▶│ readmission_risk_scores  │
│                      │                          │ readmission_risk_summary │
│                      │                          │ readmission_model_perf   │
│                      │                          │                          │
│                      │─── utilization ─────────▶│ agg_utilization_summary  │
│                      │                          │ agg_utilization_by_payer │
│                      │                          │ agg_cost_by_category     │
│                      │                          │ agg_high_cost_claimants  │
│                      │                          │ agg_condition_pmpm       │
└──────────────────────┘                         └──────────┬───────────────┘
                                                            │
                                                            ▼
                                                 ┌──────────────────────┐
                                                 │ Pop Health & Quality │
                                                 │ (Direct Lake Report) │
                                                 │  10 pages, 58 DAX   │
                                                 └─────────┬────────────┘
                                                           │
                                                           ▼
                                                 ┌──────────────────────┐
                                                 │ Data Activator       │
                                                 │ ReadmissionRiskAlert │
                                                 │ (daily email digest) │
                                                 └──────────────────────┘
```

## Data Flow

1. **Synthea** generates patients with `Claim`, `ExplanationOfBenefit`, and `Coverage` FHIR resources (enabled via `synthea.properties`)
2. **FHIR Loader** uploads all resources to FHIR Service (no filtering by type)
3. **FHIR $export** → ADLS Gen2 → Bronze Lakehouse → Silver Lakehouse (standard HDS pipeline)
4. **Materialization notebook** ([`phase-5/materialize_claims_quality.py`](../phase-5/materialize_claims_quality.py)) transforms Silver → Gold star schema (Steps 1-11)
5. **Population Health & Quality Dashboard** report binds to Gold Lakehouse via Direct Lake
6. **Data Activator** triggers daily email alert for high-risk readmission patients

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

## Star Rating Simulator (Step 8)

Computes weighted CMS Star Ratings using 2025 cut points:

| Star | Performance Range | Weight |
|------|------------------|--------|
| ⭐⭐⭐⭐⭐ | ≥ 86% | — |
| ⭐⭐⭐⭐ | 75–86% | — |
| ⭐⭐⭐ | 64–75% | — |
| ⭐⭐ | 52–64% | — |
| ⭐ | < 52% | — |

Measures are weighted: **1×** (process measures like BMI Screening) or **3×** (intermediate outcomes like Diabetes HbA1c, PDC adherence). The simulator shows what-if scenarios: "if we close N gaps, what's the new star rating?"

## HCC Risk Adjustment (Step 9)

Implements CMS-HCC V28 model with:
- **~36 condition mappings** (SNOMED + ICD-10 → HCC codes) across Diabetes, Heart Failure, COPD, CKD, Stroke, Cancer, and more
- **Hierarchy rules** — keeps only the most severe HCC per disease group per patient
- **Demographic RAF coefficients** (22 age/sex bands, Community Non-Dual)
- **Revenue-at-risk** calculation ($1,000 PMPM county benchmark)
- **Revenue opportunity** — patients with coding gaps (≥2 HCCs but RAF < 1.5)

## Readmission Risk ML Model (Step 10)

Scikit-learn LogisticRegression trained on 12 clinical features:

| Feature | Source |
|---------|--------|
| Age, Sex | Patient |
| Length of Stay | Encounter |
| Comorbidity Count | Condition |
| Medication Count | MedicationRequest |
| Prior Admits (12mo) | Encounter |
| Prior ED Visits (6mo) | Encounter |
| Diabetes flag | Condition (SNOMED) |
| CHF flag | Condition (SNOMED) |
| COPD flag | Condition (SNOMED) |
| Medicare/Medicaid flags | Coverage |

Risk tiers: **Low** (<15%), **Medium** (15-30%), **High** (≥30%). Model performance metrics (AUC, accuracy, precision, recall) and feature coefficients are stored for transparency.

> [!IMPORTANT]
> **Data Activator Alert:** A daily email digest at 8:00 AM ET is sent to the configured `AlertEmail` address listing all patients flagged as HIGH readmission risk (≥30% probability).

## Cost & Utilization Analytics (Step 11)

Standard utilization metrics with benchmark comparisons:

| Metric | Benchmark |
|--------|-----------|
| PMPM (Per Member Per Month) | $950 |
| IP Admits / 1K | 300 |
| ED Visits / 1K | 500 |
| ALOS (Average Length of Stay) | 5.0 days |
| Bed Days / 1K | 1,500 |

Additional analytics:
- **High-cost claimants** — top 5% by total paid with condition profiles and stop-loss flags
- **Condition-specific PMPM** — cost per member for 7 chronic conditions (Diabetes, HF, COPD, Hypertension, CKD, Asthma, Depression)
- **Cost by service category** — Inpatient / Emergency / Outpatient / Pharmacy / Professional

## Gold Lakehouse Tables

| Table | Type | Step | Description |
|-------|------|------|-------------|
| `dim_payer` | Dimension | 1 | Medicare, Medicaid, Commercial, Uninsured |
| `dim_diagnosis` | Dimension | 2 | ICD-10 / SNOMED codes with chronic flag |
| `fact_claim` | Fact | 3 | Claims from ExplanationOfBenefit with amounts |
| `fact_diagnosis` | Fact | 4 | Encounter-level diagnoses |
| `agg_quality_measures` | Aggregate | 5 | Patient × measure results |
| `agg_quality_summary` | Aggregate | 6 | Measure-level rates vs benchmarks |
| `agg_medication_adherence` | Aggregate | 6 | PDC scores by drug class |
| `care_gaps` | Aggregate | 7 | Open gaps with recommended actions |
| `star_rating_detail` | Aggregate | 8 | Per-measure star rating with weights |
| `star_rating_simulation` | Aggregate | 8 | What-if gap closure scenarios |
| `dim_hcc` | Dimension | 9 | HCC reference (code, name, coefficient) |
| `fact_patient_hcc` | Fact | 9 | Patient-level HCC assignments |
| `agg_risk_scores` | Aggregate | 9 | Per-patient RAF scores with risk tier |
| `agg_risk_summary` | Aggregate | 9 | RAF summary by payer × risk tier |
| `revenue_opportunity` | Aggregate | 9 | Patients with suspected coding gaps |
| `readmission_risk_scores` | Fact | 10 | Scored encounters with risk probability |
| `readmission_risk_summary` | Aggregate | 10 | Summary by risk tier × payer |
| `readmission_model_performance` | Aggregate | 10 | Model metrics + feature coefficients |
| `agg_utilization_summary` | Aggregate | 11 | Monthly utilization with benchmarks |
| `agg_utilization_by_payer` | Aggregate | 11 | Utilization metrics by payer |
| `agg_cost_by_category` | Aggregate | 11 | Cost by service category |
| `agg_high_cost_claimants` | Aggregate | 11 | Top 5% patients by cost |
| `agg_condition_pmpm` | Aggregate | 11 | Condition-specific PMPM |

## Power BI Report Pages

| Page | Title | Key Visuals |
|------|-------|-------------|
| 1 | Quality Measures Overview | KPI cards, measure rates vs benchmarks, bar chart |
| 2 | Measure Deep-Dive | Slicer per measure, decomposition tree, patient list |
| 3 | Claims Analytics | Billed/paid/denial KPIs, waterfall, payer breakdown |
| 4 | Medication Adherence | PDC gauges (3 classes), adherent vs non-adherent |
| 5 | Care Gap Closure | Priority list, gap status by measure |
| 6 | Payer Performance | Quality rate by payer, denial vs quality scatter |
| 7 | Star Rating Simulator | Overall star KPI, per-measure stars, what-if slider |
| 8 | Risk Adjustment & RAF | RAF distribution, risk tier donut, revenue-at-risk |
| 9 | Readmission Risk | Risk tier funnel, feature importance, model AUC card |
| 10 | Cost & Utilization | PMPM trend, IP/ED/1K KPIs, high-cost claimant table |

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
| Star Rating | `Overall Star Rating`, `Weighted Score`, `Max Possible Score`, `Star Improvement Opportunity` |
| HCC Risk | `Average RAF Score`, `Average RAF (Medicare/Medicaid/Commercial)`, `High Risk Members`, `Total Revenue at Risk`, `Coding Gap Count`, `Total Annual Revenue`, `Average HCC Count` |
| Readmission | `High Risk Encounters`, `Avg Readmission Risk`, `Actual Readmission Rate`, `Model AUC`, `Model Accuracy` |
| Cost & Utilization | `PMPM`, `PMPM (Medicare/Medicaid/Commercial)`, `IP per 1K`, `ED per 1K`, `ALOS`, `Bed Days per 1K`, `High Cost Member Count`, `High Cost Total Paid`, `PMPM vs Benchmark` |

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
Toggle **Phase 5: Population Health & Quality** → "Population Health & Quality Dashboard" checkbox.

### Via CLI
```powershell
# Full deployment (all phases)
.\Deploy-All.ps1 -FabricWorkspaceName "med-device-rti-hds" -AdminSecurityGroup "sg-admins"

# Phase 5 only
.\Deploy-All.ps1 -FabricWorkspaceName "med-device-rti-hds" -Phase5

# Skip Phase 5
.\Deploy-All.ps1 ... -SkipQualityMeasures

# With Readmission Risk alerting
.\Deploy-All.ps1 ... -AlertEmail "care-team@contoso.com"
```

## Inspiration

The claims data model and ontology entities are inspired by the [Fabric-Payer-Provider-HealthCare-Demo](https://github.com/rasgiza/Fabric-Payer-Provider-HealthCare-Demo) by Kwame Sefah, which implements a full payer/provider analytics solution with similar entities (Claim, Payer, Diagnosis, Prescription, MedicationAdherence, CommunityHealth).

---

### 🏁 Stage 6 Success Verification Checklist

Ensure all of the following components are verified before finalizing your deployment:

> [!IMPORTANT]
> **Stage 6 Verification Checkpoints:**
> - [ ] **Claims Data Loaded:** Confirm Silver Lakehouse contains raw claims tables, such as `ExplanationOfBenefit` and `Coverage`.
> - [ ] **Star Schema Materialized:** Spark notebook ran successfully and populated the 23 Gold star schema reporting tables inside the Gold lakehouse.
> - [ ] **Data Volume Check:** Run SQL queries to ensure `dbo.fact_claim` and chronic diagnoses in `dbo.dim_diagnosis` are populated.
> - [ ] **Quality Metrics computed:** Confirm measure aggregation tables like `dbo.agg_quality_summary` show results for eCQMs (CMS122, CMS165, etc.).
> - [ ] **PDC Medication Adherence calculated:** Verification that `dbo.agg_medication_adherence` contains computed values for statins, diabetes meds, and RAS antagonists.
> - [ ] **Star Ratings computed:** Confirm `dbo.star_rating_detail` shows per-measure star ratings and overall weighted star.
> - [ ] **HCC Risk Adjustment:** Confirm `dbo.agg_risk_scores` contains patient-level RAF scores with risk tier assignments.
> - [ ] **Readmission Risk Model:** Confirm `dbo.readmission_risk_scores` shows scored encounters with risk tiers and `dbo.readmission_model_performance` shows AUC > 0.5.
> - [ ] **Cost & Utilization:** Confirm `dbo.agg_utilization_summary` shows PMPM and utilization metrics with benchmark comparisons.
> - [ ] **Population Health Dashboard Live:** Power BI 10-page report is successfully deployed, using Direct Lake connections to Gold tables.
> - [ ] **Payer Stratification Active:** Confirm Payer performance visuals compute stratified quality metrics correctly across Commercial, Medicare, and Medicaid.
> - [ ] **Readmission Alert Active:** Confirm Data Activator ReadmissionRiskAlert reflex sends daily email digest to configured alert email.
> - [ ] **Extended Ontology Online:** The Fabric IQ `ClinicalDeviceOntology` is extended to 14 total entities (including Claims, Payers, and Diagnoses) with successful bindings to your Data Agents.

---

**Previous:** [← Stage 4 & 5 — Connected Semantic Intelligence & Bedside Alerting](phase-4-ontology-and-activator.md) · **Overview:** [← README](../README.md)
