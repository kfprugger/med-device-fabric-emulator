"""
materialize_claims_quality.py — Gold Layer Materialization for Claims & CMS Quality

Transforms Silver Lakehouse FHIR tables (ExplanationOfBenefit, Claim, Coverage,
Condition, Observation, MedicationRequest, Immunization, Encounter, Patient)
into Gold star-schema tables in healthcare1_reporting_gold:

  Dimensions:  dim_payer, dim_diagnosis
  Facts:       fact_claim, fact_claim_line, fact_diagnosis
  Aggregates:  agg_quality_measures, agg_quality_summary, agg_medication_adherence, care_gaps

Designed to run as a Fabric Notebook attached to healthcare1_reporting_gold lakehouse.

Prerequisites:
  - Phase 1+2 deployed (Silver Lakehouse with FHIR tables)
  - Synthea re-run with Claim,ExplanationOfBenefit,Coverage resources enabled
  - healthcare1_reporting_gold lakehouse exists (Phase 3)
"""

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    LongType, DateType, TimestampType, BooleanType
)
from datetime import datetime, date
import json

spark = SparkSession.builder.getOrCreate()

# ============================================================================
# CONFIG — lakehouse names (auto-discovered by Fabric notebook runtime)
# ============================================================================

SILVER_LAKEHOUSE = "healthcare1_msft_silver"
GOLD_LAKEHOUSE = "healthcare1_reporting_gold"
MEASUREMENT_YEAR_START = "2016-01-01"  # 10-year lookback
MEASUREMENT_YEAR_END = "2026-12-31"

print(f"=== Claims & Quality Materialization ===")
print(f"Silver: {SILVER_LAKEHOUSE}")
print(f"Gold:   {GOLD_LAKEHOUSE}")
print(f"Measurement period: {MEASUREMENT_YEAR_START} to {MEASUREMENT_YEAR_END}")

# ============================================================================
# HELPER: Read Silver table
# ============================================================================

def read_silver(table_name):
    """Read a Silver Lakehouse FHIR table."""
    return spark.read.format("delta").table(f"{SILVER_LAKEHOUSE}.dbo.{table_name}")


# ============================================================================
# STEP 1: BUILD dim_payer — Insurance payer dimension
# ============================================================================

print("\n--- Step 1: dim_payer ---")

try:
    coverage_df = read_silver("Coverage")
    
    dim_payer = coverage_df.select(
        F.monotonically_increasing_id().alias("payer_key"),
        F.col("idOrig").alias("payer_id"),
        # Extract payor name from JSON
        F.get_json_object(F.col("payor_string"), "$[0].display").alias("payer_name"),
        # Classify payer type
        F.when(
            F.lower(F.get_json_object(F.col("type_string"), "$.coding[0].code")).contains("medicare"), "Medicare"
        ).when(
            F.lower(F.get_json_object(F.col("type_string"), "$.coding[0].code")).contains("medicaid"), "Medicaid"
        ).when(
            F.lower(F.get_json_object(F.col("type_string"), "$.coding[0].code")).isin(
                "self-pay", "self pay"
            ), "Uninsured"
        ).otherwise("Commercial").alias("payer_type"),
        F.col("period_start").cast("date").alias("coverage_start"),
        F.col("period_end").cast("date").alias("coverage_end"),
        F.get_json_object(F.col("beneficiary_string"), "$.reference").alias("patient_ref"),
        F.lit(1).alias("is_active"),
        F.current_timestamp().alias("load_timestamp")
    ).dropDuplicates(["payer_id"])
    
    dim_payer.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_LAKEHOUSE}.dbo.dim_payer")
    print(f"  ✓ dim_payer: {dim_payer.count()} rows")
except Exception as e:
    print(f"  ⚠ Coverage table not available yet — creating empty dim_payer: {e}")
    dim_payer_schema = StructType([
        StructField("payer_key", LongType()), StructField("payer_id", StringType()),
        StructField("payer_name", StringType()), StructField("payer_type", StringType()),
        StructField("coverage_start", DateType()), StructField("coverage_end", DateType()),
        StructField("patient_ref", StringType()), StructField("is_active", IntegerType()),
        StructField("load_timestamp", TimestampType())
    ])
    spark.createDataFrame([], dim_payer_schema).write.format("delta").mode("overwrite") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.dbo.dim_payer")


# ============================================================================
# STEP 2: BUILD dim_diagnosis — ICD-10 diagnosis dimension
# ============================================================================

print("\n--- Step 2: dim_diagnosis ---")

try:
    condition_df = read_silver("Condition")
    
    dim_diagnosis = condition_df.select(
        F.monotonically_increasing_id().alias("diagnosis_key"),
        # Extract ICD-10 or SNOMED code
        F.coalesce(
            F.get_json_object(F.col("code_string"), "$.coding[0].code"),
            F.lit("UNKNOWN")
        ).alias("icd_code"),
        F.coalesce(
            F.get_json_object(F.col("code_string"), "$.coding[0].display"),
            F.lit("Unknown Diagnosis")
        ).alias("icd_description"),
        # Derive category from code prefix
        F.when(F.col("code_string").contains('"system":"http://snomed.info/sct"'), "SNOMED-CT")
         .when(F.col("code_string").contains("icd"), "ICD-10")
         .otherwise("Other").alias("code_system"),
        # Chronic flag based on common chronic conditions
        F.when(
            F.lower(F.get_json_object(F.col("code_string"), "$.coding[0].display")).rlike(
                "diabetes|hypertension|asthma|copd|heart failure|chronic|obesity"
            ), 1
        ).otherwise(0).alias("is_chronic"),
        F.lit(1).alias("is_active"),
        F.current_timestamp().alias("load_timestamp")
    ).dropDuplicates(["icd_code"])
    
    dim_diagnosis.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_LAKEHOUSE}.dbo.dim_diagnosis")
    print(f"  ✓ dim_diagnosis: {dim_diagnosis.count()} rows")
except Exception as e:
    print(f"  ⚠ Condition table issue: {e}")


# ============================================================================
# STEP 3: BUILD fact_claim — Claims fact table from ExplanationOfBenefit
# ============================================================================

print("\n--- Step 3: fact_claim ---")

try:
    eob_df = read_silver("ExplanationOfBenefit")
    
    fact_claim = eob_df.select(
        F.monotonically_increasing_id().alias("claim_key"),
        F.col("idOrig").alias("claim_id"),
        # Patient reference
        F.get_json_object(F.col("patient_string"), "$.reference").alias("patient_ref"),
        # Provider reference
        F.get_json_object(F.col("provider_string"), "$.reference").alias("provider_ref"),
        # Facility reference
        F.get_json_object(F.col("facility_string"), "$.reference").alias("facility_ref"),
        # Insurance/payer reference
        F.get_json_object(F.col("insurance_string"), "$[0].coverage.reference").alias("coverage_ref"),
        # Claim type
        F.coalesce(
            F.get_json_object(F.col("type_string"), "$.coding[0].display"),
            F.get_json_object(F.col("type_string"), "$.coding[0].code"),
            F.lit("Unknown")
        ).alias("claim_type"),
        # Outcome/status
        F.coalesce(F.col("outcome"), F.lit("complete")).alias("claim_status"),
        # Service date
        F.coalesce(
            F.col("billablePeriod_start"),
            F.col("created")
        ).cast("date").alias("service_date"),
        # Amounts — extract from total array
        F.coalesce(
            F.get_json_object(F.col("total_string"), "$[0].amount.value").cast("double"),
            F.lit(0.0)
        ).alias("billed_amount"),
        F.coalesce(
            F.get_json_object(F.col("total_string"), "$[1].amount.value").cast("double"),
            F.get_json_object(F.col("total_string"), "$[0].amount.value").cast("double"),
            F.lit(0.0)
        ).alias("paid_amount"),
        # Payment amount (what payer paid)
        F.coalesce(
            F.get_json_object(F.col("payment_string"), "$.amount.value").cast("double"),
            F.lit(0.0)
        ).alias("payment_amount"),
        # Denial flag — outcome != complete
        F.when(F.col("outcome") != "complete", 1).otherwise(0).alias("denial_flag"),
        F.current_timestamp().alias("load_timestamp")
    )
    
    # Add computed columns
    fact_claim = fact_claim.withColumn(
        "patient_responsibility",
        F.col("billed_amount") - F.col("paid_amount")
    ).withColumn(
        "allowed_amount",
        F.col("billed_amount")  # Synthea doesn't have separate allowed; use billed
    )
    
    fact_claim.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_LAKEHOUSE}.dbo.fact_claim")
    print(f"  ✓ fact_claim: {fact_claim.count()} rows")
except Exception as e:
    print(f"  ⚠ ExplanationOfBenefit not available yet: {e}")
    print("  → Claims tables will be populated after Synthea re-run with claims enabled")


# ============================================================================
# STEP 4: BUILD fact_diagnosis — Encounter-level diagnoses
# ============================================================================

print("\n--- Step 4: fact_diagnosis ---")

try:
    condition_df = read_silver("Condition")
    
    fact_diagnosis = condition_df.select(
        F.monotonically_increasing_id().alias("fact_diagnosis_key"),
        F.col("idOrig").alias("diagnosis_id"),
        # Patient
        F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
        # Encounter
        F.get_json_object(F.col("encounter_string"), "$.reference").alias("encounter_ref"),
        # Diagnosis code
        F.coalesce(
            F.get_json_object(F.col("code_string"), "$.coding[0].code"),
            F.lit("UNKNOWN")
        ).alias("icd_code"),
        F.coalesce(
            F.get_json_object(F.col("code_string"), "$.coding[0].display"),
            F.lit("Unknown")
        ).alias("diagnosis_description"),
        # Sequence (primary vs secondary)
        F.lit("principal").alias("diagnosis_type"),
        F.lit(1).alias("diagnosis_sequence"),
        # Clinical status
        F.get_json_object(F.col("clinicalStatus_string"), "$.coding[0].code").alias("clinical_status"),
        # Onset date
        F.col("onsetDateTime").cast("date").alias("diagnosis_date"),
        F.current_timestamp().alias("load_timestamp")
    )
    
    fact_diagnosis.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_LAKEHOUSE}.dbo.fact_diagnosis")
    print(f"  ✓ fact_diagnosis: {fact_diagnosis.count()} rows")
except Exception as e:
    print(f"  ⚠ fact_diagnosis issue: {e}")


# ============================================================================
# STEP 5: COMPUTE CMS Quality Measures → agg_quality_measures
# ============================================================================

print("\n--- Step 5: CMS Quality Measures ---")

try:
    patient_df = read_silver("Patient")
    condition_df = read_silver("Condition")
    observation_df = read_silver("Observation")
    medication_df = read_silver("MedicationRequest")
    immunization_df = read_silver("Immunization")
    encounter_df = read_silver("Encounter")
    
    # Calculate patient ages
    patients = patient_df.select(
        F.col("idOrig").alias("patient_id"),
        F.col("birthDate").cast("date").alias("birth_date"),
        F.col("gender"),
        F.floor(F.datediff(F.current_date(), F.col("birthDate").cast("date")) / 365.25).alias("age")
    ).filter(F.col("birth_date").isNotNull())
    
    # Parse conditions per patient (SNOMED codes)
    patient_conditions = condition_df.select(
        F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
        F.get_json_object(F.col("code_string"), "$.coding[0].code").alias("condition_code"),
        F.get_json_object(F.col("code_string"), "$.coding[0].display").alias("condition_display"),
        F.get_json_object(F.col("clinicalStatus_string"), "$.coding[0].code").alias("clinical_status")
    ).withColumn("patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1))
    
    # Parse observations (LOINC codes for labs/vitals)
    patient_obs = observation_df.select(
        F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
        F.get_json_object(F.col("code_string"), "$.coding[0].code").alias("loinc_code"),
        F.get_json_object(F.col("code_string"), "$.coding[0].display").alias("obs_name"),
        F.col("valueQuantity_value").cast("double").alias("value"),
        F.col("valueQuantity_unit").alias("unit"),
        F.col("effectiveDateTime").cast("date").alias("obs_date")
    ).withColumn("patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1))
    
    # Parse immunizations
    patient_imm = immunization_df.select(
        F.get_json_object(F.col("patient_string"), "$.reference").alias("patient_ref"),
        F.get_json_object(F.col("vaccineCode_string"), "$.coding[0].code").alias("vaccine_code"),
        F.get_json_object(F.col("vaccineCode_string"), "$.coding[0].display").alias("vaccine_name"),
        F.col("occurrenceDateTime").cast("date").alias("imm_date")
    ).withColumn("patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1))
    
    # ---- CMS122: Diabetes HbA1c Poor Control ----
    # Denominator: Patients 18-75 with diabetes + qualifying encounter
    # Numerator: Most recent HbA1c > 9% OR no HbA1c
    
    diabetes_snomed = ["44054006", "73211009"]  # Type 1, Type 2 diabetes
    
    diabetic_patients = patient_conditions.filter(
        F.col("condition_code").isin(diabetes_snomed) &
        F.col("clinical_status").isin("active", "recurrence")
    ).select("patient_id").distinct()
    
    cms122_denom = patients.join(diabetic_patients, "patient_id").filter(
        (F.col("age") >= 18) & (F.col("age") <= 75)
    ).select("patient_id")
    
    # HbA1c: LOINC 4548-4
    latest_hba1c = patient_obs.filter(F.col("loinc_code") == "4548-4").withColumn(
        "rn", F.row_number().over(Window.partitionBy("patient_id").orderBy(F.desc("obs_date")))
    ).filter(F.col("rn") == 1).select("patient_id", F.col("value").alias("last_hba1c"))
    
    cms122_result = cms122_denom.join(latest_hba1c, "patient_id", "left").withColumn(
        "measure_id", F.lit("CMS122v12")
    ).withColumn(
        "measure_name", F.lit("Diabetes: Hemoglobin A1c Poor Control")
    ).withColumn(
        "in_initial_population", F.lit(True)
    ).withColumn(
        "in_denominator", F.lit(True)
    ).withColumn(
        # Inverse measure — numerator = POOR control (HbA1c > 9 or no test)
        "in_numerator", F.when(
            (F.col("last_hba1c") > 9.0) | F.col("last_hba1c").isNull(), True
        ).otherwise(False)
    ).withColumn(
        "in_exclusion", F.lit(False)
    ).withColumn(
        # quality_met = True when HbA1c IS controlled (inverse measure)
        "quality_met", F.when(
            F.col("last_hba1c").isNotNull() & (F.col("last_hba1c") <= 9.0), True
        ).otherwise(False)
    ).select("patient_id", "measure_id", "measure_name", "in_initial_population",
             "in_denominator", "in_numerator", "in_exclusion", "quality_met")
    
    # ---- CMS165: Controlling High Blood Pressure ----
    # Denominator: Patients 18-85 with hypertension
    # Numerator: Most recent BP < 140/90
    
    htn_snomed = ["59621000"]  # Essential hypertension
    
    htn_patients = patient_conditions.filter(
        F.col("condition_code").isin(htn_snomed) &
        F.col("clinical_status").isin("active", "recurrence")
    ).select("patient_id").distinct()
    
    cms165_denom = patients.join(htn_patients, "patient_id").filter(
        (F.col("age") >= 18) & (F.col("age") <= 85)
    ).select("patient_id")
    
    # Systolic BP: LOINC 8480-6, Diastolic: 8462-4
    latest_sbp = patient_obs.filter(F.col("loinc_code") == "8480-6").withColumn(
        "rn", F.row_number().over(Window.partitionBy("patient_id").orderBy(F.desc("obs_date")))
    ).filter(F.col("rn") == 1).select("patient_id", F.col("value").alias("systolic"))
    
    latest_dbp = patient_obs.filter(F.col("loinc_code") == "8462-4").withColumn(
        "rn", F.row_number().over(Window.partitionBy("patient_id").orderBy(F.desc("obs_date")))
    ).filter(F.col("rn") == 1).select("patient_id", F.col("value").alias("diastolic"))
    
    cms165_result = cms165_denom.join(latest_sbp, "patient_id", "left") \
        .join(latest_dbp, "patient_id", "left").withColumn(
        "measure_id", F.lit("CMS165v12")
    ).withColumn("measure_name", F.lit("Controlling High Blood Pressure")).withColumn(
        "in_initial_population", F.lit(True)
    ).withColumn("in_denominator", F.lit(True)).withColumn(
        "in_numerator", F.when(
            (F.col("systolic") < 140) & (F.col("diastolic") < 90), True
        ).otherwise(False)
    ).withColumn("in_exclusion", F.lit(False)).withColumn(
        "quality_met", F.when(
            (F.col("systolic") < 140) & (F.col("diastolic") < 90), True
        ).otherwise(False)
    ).select("patient_id", "measure_id", "measure_name", "in_initial_population",
             "in_denominator", "in_numerator", "in_exclusion", "quality_met")
    
    # ---- CMS69: BMI Screening ----
    # Denominator: Patients 18+
    # Numerator: BMI recorded in measurement period
    
    cms69_denom = patients.filter(F.col("age") >= 18).select("patient_id")
    
    bmi_obs = patient_obs.filter(F.col("loinc_code") == "39156-5").select("patient_id").distinct()
    
    cms69_result = cms69_denom.join(bmi_obs, "patient_id", "left").withColumn(
        "measure_id", F.lit("CMS69v12")
    ).withColumn("measure_name", F.lit("Preventive Care: BMI Screening")).withColumn(
        "in_initial_population", F.lit(True)
    ).withColumn("in_denominator", F.lit(True)).withColumn(
        "in_numerator", F.when(bmi_obs["patient_id"].isNotNull(), True).otherwise(False)
    ).withColumn("in_exclusion", F.lit(False)).withColumn(
        "quality_met", F.when(bmi_obs["patient_id"].isNotNull(), True).otherwise(False)
    ).select("patient_id", "measure_id", "measure_name", "in_initial_population",
             "in_denominator", "in_numerator", "in_exclusion", "quality_met")
    
    # ---- CMS127: Pneumococcal Vaccination ----
    # Denominator: Patients 65+
    # Numerator: Received pneumococcal vaccine
    
    cms127_denom = patients.filter(F.col("age") >= 65).select("patient_id")
    
    # CVX codes for pneumococcal vaccines
    pneumo_cvx = ["33", "100", "109", "133", "152", "215"]
    pneumo_patients = patient_imm.filter(
        F.col("vaccine_code").isin(pneumo_cvx)
    ).select("patient_id").distinct()
    
    cms127_result = cms127_denom.join(pneumo_patients, "patient_id", "left").withColumn(
        "measure_id", F.lit("CMS127v12")
    ).withColumn("measure_name", F.lit("Pneumococcal Vaccination Status")).withColumn(
        "in_initial_population", F.lit(True)
    ).withColumn("in_denominator", F.lit(True)).withColumn(
        "in_numerator", F.when(pneumo_patients["patient_id"].isNotNull(), True).otherwise(False)
    ).withColumn("in_exclusion", F.lit(False)).withColumn(
        "quality_met", F.when(pneumo_patients["patient_id"].isNotNull(), True).otherwise(False)
    ).select("patient_id", "measure_id", "measure_name", "in_initial_population",
             "in_denominator", "in_numerator", "in_exclusion", "quality_met")
    
    # ---- CMS147: Influenza Immunization ----
    # Denominator: Patients 6 months+ with encounter
    # Numerator: Received flu vaccine
    
    cms147_denom = patients.filter(F.col("age") >= 1).select("patient_id")
    
    flu_cvx = ["140", "141", "150", "155", "158", "161", "166", "171", "185", "186", "197", "205"]
    flu_patients = patient_imm.filter(
        F.col("vaccine_code").isin(flu_cvx)
    ).select("patient_id").distinct()
    
    cms147_result = cms147_denom.join(flu_patients, "patient_id", "left").withColumn(
        "measure_id", F.lit("CMS147v13")
    ).withColumn("measure_name", F.lit("Preventive Care: Influenza Immunization")).withColumn(
        "in_initial_population", F.lit(True)
    ).withColumn("in_denominator", F.lit(True)).withColumn(
        "in_numerator", F.when(flu_patients["patient_id"].isNotNull(), True).otherwise(False)
    ).withColumn("in_exclusion", F.lit(False)).withColumn(
        "quality_met", F.when(flu_patients["patient_id"].isNotNull(), True).otherwise(False)
    ).select("patient_id", "measure_id", "measure_name", "in_initial_population",
             "in_denominator", "in_numerator", "in_exclusion", "quality_met")
    
    # ---- CMS134: Diabetes Nephropathy Screening ----
    # Denominator: Diabetic patients 18-75
    # Numerator: Urine albumin test OR ACE/ARB medication
    
    cms134_denom = cms122_denom  # Same denominator as CMS122
    
    # Urine albumin LOINC codes
    albumin_loinc = ["14959-1", "14957-5", "13705-9", "1754-1", "1755-8"]
    albumin_patients = patient_obs.filter(
        F.col("loinc_code").isin(albumin_loinc)
    ).select("patient_id").distinct()
    
    # ACE/ARB medications (check for common drug names in medication text)
    acei_arb_patients = medication_df.select(
        F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
        F.get_json_object(F.col("medicationCodeableConcept_string"), "$.coding[0].display").alias("med_name")
    ).withColumn("patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1)).filter(
        F.lower(F.col("med_name")).rlike("lisinopril|enalapril|ramipril|losartan|valsartan|irbesartan|olmesartan|candesartan|benazepril|captopril|fosinopril|quinapril|trandolapril|perindopril|eprosartan|telmisartan|azilsartan")
    ).select("patient_id").distinct()
    
    nephro_screened = albumin_patients.union(acei_arb_patients).distinct()
    
    cms134_result = cms134_denom.join(nephro_screened, "patient_id", "left").withColumn(
        "measure_id", F.lit("CMS134v12")
    ).withColumn("measure_name", F.lit("Diabetes: Medical Attention for Nephropathy")).withColumn(
        "in_initial_population", F.lit(True)
    ).withColumn("in_denominator", F.lit(True)).withColumn(
        "in_numerator", F.when(nephro_screened["patient_id"].isNotNull(), True).otherwise(False)
    ).withColumn("in_exclusion", F.lit(False)).withColumn(
        "quality_met", F.when(nephro_screened["patient_id"].isNotNull(), True).otherwise(False)
    ).select("patient_id", "measure_id", "measure_name", "in_initial_population",
             "in_denominator", "in_numerator", "in_exclusion", "quality_met")
    
    # ---- CMS144: Heart Failure Beta-Blocker ----
    # Denominator: CHF patients 18+
    # Numerator: On beta-blocker therapy
    
    chf_snomed = ["42343007", "84114007"]  # CHF, Heart failure
    chf_patients = patient_conditions.filter(
        F.col("condition_code").isin(chf_snomed) &
        F.col("clinical_status").isin("active", "recurrence")
    ).select("patient_id").distinct()
    
    cms144_denom = patients.join(chf_patients, "patient_id").filter(
        F.col("age") >= 18
    ).select("patient_id")
    
    bb_patients = medication_df.select(
        F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
        F.get_json_object(F.col("medicationCodeableConcept_string"), "$.coding[0].display").alias("med_name")
    ).withColumn("patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1)).filter(
        F.lower(F.col("med_name")).rlike("metoprolol|carvedilol|bisoprolol|atenolol|propranolol|nebivolol|nadolol|labetalol")
    ).select("patient_id").distinct()
    
    cms144_result = cms144_denom.join(bb_patients, "patient_id", "left").withColumn(
        "measure_id", F.lit("CMS144v12")
    ).withColumn("measure_name", F.lit("Heart Failure: Beta-Blocker Therapy")).withColumn(
        "in_initial_population", F.lit(True)
    ).withColumn("in_denominator", F.lit(True)).withColumn(
        "in_numerator", F.when(bb_patients["patient_id"].isNotNull(), True).otherwise(False)
    ).withColumn("in_exclusion", F.lit(False)).withColumn(
        "quality_met", F.when(bb_patients["patient_id"].isNotNull(), True).otherwise(False)
    ).select("patient_id", "measure_id", "measure_name", "in_initial_population",
             "in_denominator", "in_numerator", "in_exclusion", "quality_met")
    
    # ---- UNION all measures ----
    all_measures = cms122_result.unionByName(cms165_result) \
        .unionByName(cms69_result).unionByName(cms127_result) \
        .unionByName(cms147_result).unionByName(cms134_result) \
        .unionByName(cms144_result)
    
    all_measures = all_measures.withColumn(
        "measurement_year", F.lit(datetime.now().year)
    )
    
    all_measures.write.format("delta").mode("overwrite").saveAsTable(
        f"{GOLD_LAKEHOUSE}.dbo.agg_quality_measures"
    )
    print(f"  ✓ agg_quality_measures: {all_measures.count()} rows across 7 measures")
    
    # ---- Build agg_quality_summary ----
    agg_summary = all_measures.groupBy("measure_id", "measure_name", "measurement_year").agg(
        F.sum(F.when(F.col("in_denominator"), 1).otherwise(0)).alias("denominator_count"),
        F.sum(F.when(F.col("in_numerator"), 1).otherwise(0)).alias("numerator_count"),
        F.sum(F.when(F.col("in_exclusion"), 1).otherwise(0)).alias("exclusion_count"),
        F.sum(F.when(F.col("quality_met"), 1).otherwise(0)).alias("quality_met_count")
    ).withColumn(
        "quality_rate", F.round(100.0 * F.col("quality_met_count") / F.col("denominator_count"), 1)
    ).withColumn(
        # National benchmarks (approximate)
        "benchmark_rate", F.when(F.col("measure_id") == "CMS122v12", 65.0)
            .when(F.col("measure_id") == "CMS165v12", 72.0)
            .when(F.col("measure_id") == "CMS69v12", 85.0)
            .when(F.col("measure_id") == "CMS127v12", 78.0)
            .when(F.col("measure_id") == "CMS147v13", 55.0)
            .when(F.col("measure_id") == "CMS134v12", 88.0)
            .when(F.col("measure_id") == "CMS144v12", 90.0)
            .otherwise(75.0)
    ).withColumn("load_timestamp", F.current_timestamp())
    
    agg_summary.write.format("delta").mode("overwrite").saveAsTable(
        f"{GOLD_LAKEHOUSE}.dbo.agg_quality_summary"
    )
    print(f"  ✓ agg_quality_summary: {agg_summary.count()} rows")
    
except Exception as e:
    print(f"  ⚠ Quality measures computation error: {e}")
    import traceback
    traceback.print_exc()


# ============================================================================
# STEP 6: COMPUTE Medication Adherence (PDC) → agg_medication_adherence
# ============================================================================

print("\n--- Step 6: Medication Adherence (PDC) ---")

try:
    medication_df = read_silver("MedicationRequest")
    
    # Parse medications with dates
    med_parsed = medication_df.select(
        F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
        F.get_json_object(F.col("medicationCodeableConcept_string"), "$.coding[0].display").alias("med_name"),
        F.get_json_object(F.col("medicationCodeableConcept_string"), "$.coding[0].code").alias("med_code"),
        F.col("authoredOn").cast("date").alias("authored_date"),
        F.col("status")
    ).withColumn("patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1))
    
    # Classify into HEDIS adherence drug classes
    med_classified = med_parsed.withColumn(
        "medication_class",
        F.when(F.lower(F.col("med_name")).rlike(
            "metformin|glipizide|glyburide|glimepiride|sitagliptin|pioglitazone|empagliflozin|dapagliflozin|liraglutide|semaglutide|insulin"
        ), "PDC-DR (Diabetes)")
        .when(F.lower(F.col("med_name")).rlike(
            "lisinopril|enalapril|ramipril|losartan|valsartan|irbesartan|olmesartan|candesartan|benazepril|captopril"
        ), "PDC-RASA (RAS Antagonists)")
        .when(F.lower(F.col("med_name")).rlike(
            "atorvastatin|simvastatin|rosuvastatin|pravastatin|lovastatin|fluvastatin|pitavastatin"
        ), "PDC-STA (Statins)")
        .otherwise(None)
    ).filter(F.col("medication_class").isNotNull())
    
    # Calculate PDC per patient per drug class
    # Simplified PDC: count distinct months with active prescription / 12
    med_adherence = med_classified.groupBy("patient_id", "medication_class").agg(
        F.countDistinct(F.month(F.col("authored_date"))).alias("months_with_rx"),
        F.count("*").alias("total_fills"),
        F.min("authored_date").alias("first_fill"),
        F.max("authored_date").alias("last_fill")
    ).withColumn(
        "pdc_score", F.least(
            F.round(F.col("months_with_rx") / 12.0, 2),
            F.lit(1.0)
        )
    ).withColumn(
        "adherence_category",
        F.when(F.col("pdc_score") >= 0.8, "Adherent").otherwise("Non-Adherent")
    ).withColumn(
        "gap_days", F.when(
            F.col("pdc_score") < 0.8,
            F.round((1.0 - F.col("pdc_score")) * 365, 0).cast("int")
        ).otherwise(0)
    ).withColumn(
        "is_chronic", F.lit(1)
    ).withColumn("load_timestamp", F.current_timestamp())
    
    med_adherence.write.format("delta").mode("overwrite").saveAsTable(
        f"{GOLD_LAKEHOUSE}.dbo.agg_medication_adherence"
    )
    print(f"  ✓ agg_medication_adherence: {med_adherence.count()} rows")
    
except Exception as e:
    print(f"  ⚠ Medication adherence error: {e}")


# ============================================================================
# STEP 7: BUILD care_gaps — Actionable gaps per patient
# ============================================================================

print("\n--- Step 7: Care Gaps ---")

try:
    quality_df = spark.read.format("delta").table(f"{GOLD_LAKEHOUSE}.dbo.agg_quality_measures")
    
    # Care gaps = patients in denominator but NOT meeting quality
    care_gaps = quality_df.filter(
        (F.col("in_denominator") == True) & (F.col("quality_met") == False)
    ).select(
        F.col("patient_id"),
        F.col("measure_id"),
        F.col("measure_name").alias("gap_type"),
        F.lit("open").alias("gap_status"),
        # Days overdue — simplified estimate
        F.lit(90).alias("days_overdue"),
        F.when(F.col("measure_id") == "CMS122v12", "Order HbA1c lab test; consider medication adjustment")
         .when(F.col("measure_id") == "CMS165v12", "Recheck blood pressure; consider medication titration")
         .when(F.col("measure_id") == "CMS69v12", "Record BMI and create follow-up plan")
         .when(F.col("measure_id") == "CMS127v12", "Administer pneumococcal vaccine (PCV20 or PPSV23)")
         .when(F.col("measure_id") == "CMS147v13", "Administer seasonal influenza vaccine")
         .when(F.col("measure_id") == "CMS134v12", "Order urine albumin test or start ACE/ARB therapy")
         .when(F.col("measure_id") == "CMS144v12", "Start beta-blocker therapy (carvedilol, metoprolol, bisoprolol)")
         .otherwise("Follow up with provider").alias("recommended_action"),
        F.current_timestamp().alias("load_timestamp")
    )
    
    care_gaps.write.format("delta").mode("overwrite").saveAsTable(
        f"{GOLD_LAKEHOUSE}.dbo.care_gaps"
    )
    print(f"  ✓ care_gaps: {care_gaps.count()} rows")
    
except Exception as e:
    print(f"  ⚠ Care gaps error: {e}")


# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 60)
print("=== Claims & Quality Materialization Complete ===")
print("=" * 60)

tables = [
    "dim_payer", "dim_diagnosis", "fact_claim", "fact_diagnosis",
    "agg_quality_measures", "agg_quality_summary",
    "agg_medication_adherence", "care_gaps"
]

for t in tables:
    try:
        count = spark.read.format("delta").table(f"{GOLD_LAKEHOUSE}.dbo.{t}").count()
        print(f"  {t}: {count:,} rows")
    except:
        print(f"  {t}: not yet created (needs Synthea re-run with claims)")

print("\nDone.")
