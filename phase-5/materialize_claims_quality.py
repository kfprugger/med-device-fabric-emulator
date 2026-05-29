"""
materialize_claims_quality.py — Gold Layer Materialization for Population Health & Quality

Transforms Silver Lakehouse FHIR tables (ExplanationOfBenefit, Claim, Coverage,
Condition, Observation, MedicationRequest, Immunization, Encounter, Patient)
into Gold star-schema tables in healthcare1_reporting_gold:

  Dimensions:  dim_payer, dim_diagnosis, dim_hcc
  Facts:       fact_claim, fact_diagnosis, fact_patient_hcc
  Aggregates:  agg_quality_measures, agg_quality_summary, agg_medication_adherence,
               care_gaps, star_rating_detail, star_rating_simulation,
               agg_risk_scores, agg_risk_summary, revenue_opportunity,
               readmission_risk_scores, readmission_risk_summary,
               readmission_model_performance,
               agg_utilization_summary, agg_utilization_by_payer,
               agg_cost_by_category, agg_high_cost_claimants, agg_condition_pmpm

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


# ============================================================================
# DYNAMIC ONELAKE PATH PATCHING
# Intercepts Spark SQL table reads and writes to use absolute ABFSS paths
# since programmatic API triggers run in a catalogless (no default context) session.
# ============================================================================

WORKSPACE_ID = "8732edc9-7e43-4412-a311-4c30981c775f"
SILVER_ID = "3b6f3bd7-e892-4542-8780-23e3b39c9de0"
GOLD_ID = "763985f5-8ec1-40ac-b54c-2ad521f6f407"

SILVER_BASE = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SILVER_ID}/Tables"
GOLD_BASE = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{GOLD_ID}/Tables"

def patched_read_table(table_name):
    # Strip database/catalog prefixes if any
    parts = table_name.split(".")
    actual_name = parts[-1]
    db_name = parts[-2] if len(parts) > 1 else ""
    
    if "silver" in db_name.lower():
        abfss_path = f"{SILVER_BASE}/{actual_name}"
    else:
        abfss_path = f"{GOLD_BASE}/{actual_name}"
        
    print(f"[ONELAKE PATCH] Intercepted read of '{table_name}' -> loading from {abfss_path}")
    return spark.read.format("delta").load(abfss_path)

# Patch DataFrameWriter to intercept saves
from pyspark.sql.readwriter import DataFrameWriter

def patched_saveAsTable(self, table_name, *args, **kwargs):
    parts = table_name.split(".")
    actual_name = parts[-1]
    db_name = parts[-2] if len(parts) > 1 else ""
    
    if "silver" in db_name.lower():
        abfss_path = f"{SILVER_BASE}/{actual_name}"
    else:
        abfss_path = f"{GOLD_BASE}/{actual_name}"
        
    print(f"[ONELAKE PATCH] Intercepted saveAsTable to '{table_name}' -> saving to {abfss_path}")
    return self.save(abfss_path)

DataFrameWriter.saveAsTable = patched_saveAsTable

# Patch DataFrameReader to intercept reads
from pyspark.sql.readwriter import DataFrameReader
DataFrameReader.table = lambda self, name: patched_read_table(name)

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
    return patched_read_table(f"{SILVER_LAKEHOUSE}.{table_name}")


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
        F.get_json_object(F.col("period_string"), "$.start").cast("date").alias("coverage_start"),
        F.get_json_object(F.col("period_string"), "$.end").cast("date").alias("coverage_end"),
        F.get_json_object(F.col("beneficiary_string"), "$.reference").alias("patient_ref"),
        F.lit(1).alias("is_active"),
        F.current_timestamp().alias("load_timestamp")
    ).dropDuplicates(["payer_id"])
    
    # payer_category mirrors payer_type as a convenience name for stratified
    # reporting (Medicare / Medicaid / Commercial / Uninsured / Other).
    dim_payer = dim_payer.withColumn(
        "payer_category",
        F.when(F.col("payer_type").isin("Medicare", "Medicaid", "Commercial", "Uninsured"),
               F.col("payer_type")).otherwise(F.lit("Other"))
    )
    dim_payer.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.dim_payer")
    print(f"  ✓ dim_payer: {dim_payer.count()} rows (with payer_category)")
except Exception as e:
    print(f"  ⚠ Coverage table not available yet — creating empty dim_payer: {e}")
    dim_payer_schema = StructType([
        StructField("payer_key", LongType()), StructField("payer_id", StringType()),
        StructField("payer_name", StringType()), StructField("payer_type", StringType()),
        StructField("payer_category", StringType()),
        StructField("coverage_start", DateType()), StructField("coverage_end", DateType()),
        StructField("patient_ref", StringType()), StructField("is_active", IntegerType()),
        StructField("load_timestamp", TimestampType())
    ])
    spark.createDataFrame([], dim_payer_schema).write.format("delta").mode("overwrite") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.dim_payer")


# ============================================================================
# STEP 1b: BUILD patient_payer — primary payer per patient (for stratification)
# ============================================================================
# Picks the most recent active Coverage row per patient. Used to denormalize
# payer_category onto fact_claim and agg_quality_measures so the Power BI
# report can stratify CMS rates and revenue by Medicare / Medicaid / Commercial.

print("\n--- Step 1b: patient_payer (stratification lookup) ---")
patient_payer = None
try:
    dp = spark.read.format("delta").table(f"{GOLD_LAKEHOUSE}.dim_payer")
    if dp.count() > 0:
        patient_payer = dp.withColumn(
            "patient_id",
            F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1)
        ).filter(F.col("patient_id") != "").withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("patient_id")
                      .orderBy(F.desc_nulls_last("coverage_start"))
            )
        ).filter(F.col("rn") == 1).select(
            "patient_id",
            F.col("payer_id").alias("primary_payer_id"),
            F.col("payer_category").alias("payer_category"),
            F.col("payer_key").alias("primary_payer_key"),
        )
        print(f"  ✓ patient_payer: {patient_payer.count()} patients mapped")
    else:
        print("  ⚠ dim_payer is empty — payer stratification will default to 'Unknown'")
except Exception as e:
    print(f"  ⚠ patient_payer build failed (non-fatal, defaults to Unknown): {e}")


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
    
    dim_diagnosis.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_LAKEHOUSE}.dim_diagnosis")
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
            F.get_json_object(F.col("billablePeriod_string"), "$.start"),
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
    ).withColumn(
        # Strip "Patient/" / "Coverage/" prefixes for downstream joins
        "patient_id",
        F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1)
    ).withColumn(
        "coverage_id",
        F.regexp_extract(F.col("coverage_ref"), r"Coverage/(.*)", 1)
    )

    # Stratify each claim by payer_category. Prefer the Coverage referenced by
    # the EOB itself; fall back to the patient's primary Coverage; default to
    # "Unknown" when no Coverage data is available.
    if patient_payer is not None:
        dp_lookup = spark.read.format("delta").table(f"{GOLD_LAKEHOUSE}.dim_payer") \
            .select(F.col("payer_id").alias("coverage_id"),
                    F.col("payer_category").alias("claim_payer_category"))
        fact_claim = fact_claim.join(dp_lookup, "coverage_id", "left") \
            .join(patient_payer.select("patient_id", "payer_category"), "patient_id", "left") \
            .withColumn(
                "payer_category",
                F.coalesce(F.col("claim_payer_category"), F.col("payer_category"), F.lit("Unknown"))
            ).drop("claim_payer_category")
    else:
        fact_claim = fact_claim.withColumn("payer_category", F.lit("Unknown"))

    fact_claim.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.fact_claim")
    print(f"  ✓ fact_claim: {fact_claim.count()} rows (with payer_category)")
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
    
    fact_diagnosis.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_LAKEHOUSE}.fact_diagnosis")
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
        F.get_json_object(F.col("valueQuantity_string"), "$.value").cast("double").alias("value"),
        F.get_json_object(F.col("valueQuantity_string"), "$.unit").alias("unit"),
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

    # Denormalize payer_category onto each measure row so the report can
    # stratify CMS quality rates by Medicare / Medicaid / Commercial.
    if patient_payer is not None:
        all_measures = all_measures.join(
            patient_payer.select("patient_id", "payer_category"),
            "patient_id",
            "left"
        ).withColumn("payer_category", F.coalesce(F.col("payer_category"), F.lit("Unknown")))
    else:
        all_measures = all_measures.withColumn("payer_category", F.lit("Unknown"))

    all_measures.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.agg_quality_measures")
    print(f"  ✓ agg_quality_measures: {all_measures.count()} rows across 7 measures (with payer_category)")

    # ---- Build agg_quality_summary ----
    agg_summary = all_measures.groupBy(
        "measure_id", "measure_name", "measurement_year", "payer_category"
    ).agg(
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

    agg_summary.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.agg_quality_summary")
    print(f"  ✓ agg_quality_summary: {agg_summary.count()} rows (per measure × payer_category)")
    
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
        f"{GOLD_LAKEHOUSE}.agg_medication_adherence"
    )
    print(f"  ✓ agg_medication_adherence: {med_adherence.count()} rows")
    
except Exception as e:
    print(f"  ⚠ Medication adherence error: {e}")


# ============================================================================
# STEP 7: BUILD care_gaps — Actionable gaps per patient
# ============================================================================

print("\n--- Step 7: Care Gaps ---")

try:
    quality_df = spark.read.format("delta").table(f"{GOLD_LAKEHOUSE}.agg_quality_measures")
    
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
        f"{GOLD_LAKEHOUSE}.care_gaps"
    )
    print(f"  ✓ care_gaps: {care_gaps.count()} rows")
    
except Exception as e:
    print(f"  ⚠ Care gaps error: {e}")



# ============================================================================
# STEP 8: COMPUTE Star Ratings → star_rating_detail, star_rating_simulation
# ============================================================================

print("\n--- Step 8: Star Rating Computation ---")

try:
    quality_summary = spark.read.format("delta").table(f"{GOLD_LAKEHOUSE}.agg_quality_summary")
    adherence_df = spark.read.format("delta").table(f"{GOLD_LAKEHOUSE}.agg_medication_adherence")
    gaps_df = spark.read.format("delta").table(f"{GOLD_LAKEHOUSE}.care_gaps")

    # CMS Star Rating methodology:
    #   - Each measure gets a 1-5 star based on performance cut points
    #   - Stars are weighted: 1x (process), 3x (intermediate outcome)
    #   - Overall star = weighted average across all measures
    #
    # 2025 CMS cut points (simplified):
    MEASURE_WEIGHTS = {
        "CMS122v12": 3, "CMS165v12": 3, "CMS69v12": 1,
        "CMS127v12": 1, "CMS147v13": 1, "CMS134v12": 3, "CMS144v12": 3,
    }

    # Compute overall quality rate per measure (across all payers)
    overall_rates = quality_summary.groupBy("measure_id", "measure_name").agg(
        F.sum("quality_met_count").alias("total_met"),
        F.sum("denominator_count").alias("total_denom"),
        F.max("benchmark_rate").alias("benchmark_rate")
    ).withColumn(
        "quality_rate", F.round(100.0 * F.col("total_met") / F.col("total_denom"), 1)
    )

    # Add PDC adherence as pseudo-measures for Star Rating
    pdc_rates = adherence_df.groupBy("medication_class").agg(
        F.sum(F.when(F.col("adherence_category") == "Adherent", 1).otherwise(0)).alias("total_met"),
        F.count("*").alias("total_denom")
    ).withColumn(
        "quality_rate", F.round(100.0 * F.col("total_met") / F.col("total_denom"), 1)
    ).withColumn(
        "measure_id",
        F.when(F.col("medication_class").contains("DR"), F.lit("PDC-DR"))
         .when(F.col("medication_class").contains("RASA"), F.lit("PDC-RASA"))
         .when(F.col("medication_class").contains("STA"), F.lit("PDC-STA"))
         .otherwise(F.lit("PDC-OTHER"))
    ).withColumn("measure_name", F.col("medication_class")).withColumn(
        "benchmark_rate", F.lit(80.0)
    ).select("measure_id", "measure_name", "total_met", "total_denom", "quality_rate", "benchmark_rate")

    all_rated = overall_rates.unionByName(pdc_rates)

    # Map quality_rate to star rating using 2025 CMS cut points
    all_rated = all_rated.withColumn(
        "star_rating",
        F.when(F.col("quality_rate") >= 86.0, 5)
         .when(F.col("quality_rate") >= 75.0, 4)
         .when(F.col("quality_rate") >= 64.0, 3)
         .when(F.col("quality_rate") >= 52.0, 2)
         .otherwise(1)
    )

    # PDC measures are triple-weighted (3x)
    weight_expr = F.when(F.col("measure_id") == "CMS122v12", 3) \
        .when(F.col("measure_id") == "CMS165v12", 3) \
        .when(F.col("measure_id") == "CMS69v12", 1) \
        .when(F.col("measure_id") == "CMS127v12", 1) \
        .when(F.col("measure_id") == "CMS147v13", 1) \
        .when(F.col("measure_id") == "CMS134v12", 3) \
        .when(F.col("measure_id") == "CMS144v12", 3) \
        .when(F.col("measure_id").startswith("PDC"), 3) \
        .otherwise(1)

    all_rated = all_rated.withColumn("measure_weight", weight_expr)
    all_rated = all_rated.withColumn("weighted_score", F.col("star_rating") * F.col("measure_weight"))
    all_rated = all_rated.withColumn(
        "rate_to_next_star",
        F.when(F.col("star_rating") == 5, F.lit(None).cast("double"))
         .when(F.col("star_rating") == 4, 86.0 - F.col("quality_rate"))
         .when(F.col("star_rating") == 3, 75.0 - F.col("quality_rate"))
         .when(F.col("star_rating") == 2, 64.0 - F.col("quality_rate"))
         .otherwise(52.0 - F.col("quality_rate"))
    )

    # Compute overall weighted star
    total_weighted = all_rated.agg(
        F.sum("weighted_score").alias("total_weighted"),
        F.sum("measure_weight").alias("total_weight")
    ).collect()[0]
    overall_star = round(total_weighted["total_weighted"] / total_weighted["total_weight"], 1) if total_weighted["total_weight"] > 0 else 0

    star_detail = all_rated.withColumn("overall_weighted_star", F.lit(overall_star))
    star_detail = star_detail.withColumn("load_timestamp", F.current_timestamp())

    star_detail.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.star_rating_detail")
    print(f"  ✓ star_rating_detail: {star_detail.count()} measures, overall weighted star: {overall_star}")

    # ---- Star Rating Simulation (what-if gap closure) ----
    gap_counts = gaps_df.groupBy("measure_id").agg(F.count("*").alias("total_open_gaps"))
    sim_scenarios = [10, 25, 50, 100, 250]
    sim_rows = []

    for row in all_rated.collect():
        mid = row["measure_id"]
        current_rate = row["quality_rate"] or 0
        denom = row["total_denom"] or 1
        met = row["total_met"] or 0
        weight = row["measure_weight"] or 1
        gap_row = gap_counts.filter(F.col("measure_id") == mid).collect()
        open_gaps = gap_row[0]["total_open_gaps"] if gap_row else 0

        for n in sim_scenarios:
            closeable = min(n, open_gaps)
            new_met = met + closeable
            new_rate = round(100.0 * new_met / denom, 1) if denom > 0 else 0
            new_star = 5 if new_rate >= 86 else (4 if new_rate >= 75 else (3 if new_rate >= 64 else (2 if new_rate >= 52 else 1)))
            sim_rows.append((mid, row["measure_name"], n, closeable, float(current_rate),
                             float(new_rate), row["star_rating"], new_star, weight, open_gaps))

    sim_schema = StructType([
        StructField("measure_id", StringType()), StructField("measure_name", StringType()),
        StructField("gaps_to_close", IntegerType()), StructField("closeable_gaps", IntegerType()),
        StructField("current_rate", DoubleType()), StructField("simulated_rate", DoubleType()),
        StructField("current_star", IntegerType()), StructField("simulated_star", IntegerType()),
        StructField("measure_weight", IntegerType()), StructField("total_open_gaps", IntegerType())
    ])
    sim_df = spark.createDataFrame(sim_rows, sim_schema)
    sim_df = sim_df.withColumn("load_timestamp", F.current_timestamp())
    sim_df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.star_rating_simulation")
    print(f"  ✓ star_rating_simulation: {sim_df.count()} scenario rows")

except Exception as e:
    print(f"  ⚠ Star rating computation error: {e}")
    import traceback; traceback.print_exc()


# ============================================================================
# STEP 9: HCC Risk Adjustment → dim_hcc, fact_patient_hcc, agg_risk_scores
# ============================================================================

print("\n--- Step 9: HCC Risk Adjustment ---")

try:
    condition_df = read_silver("Condition")
    patient_df = read_silver("Patient")

    # CMS-HCC V28 model: condition_code → HCC mapping (common Synthea conditions)
    # Format: (code, code_system, hcc_code, hcc_name, coefficient, hierarchy_group)
    HCC_MAPPINGS = [
        ("73211009",  "SNOMED", "HCC37", "Diabetes with Acute Complications", 0.302, "Diabetes"),
        ("44054006",  "SNOMED", "HCC38", "Diabetes with Chronic Complications", 0.302, "Diabetes"),
        ("15777000",  "SNOMED", "HCC19", "Diabetes without Complication", 0.104, "Diabetes"),
        ("E11.65",    "ICD10",  "HCC38", "Diabetes with Chronic Complications", 0.302, "Diabetes"),
        ("E11.9",     "ICD10",  "HCC19", "Diabetes without Complication", 0.104, "Diabetes"),
        ("42343007",  "SNOMED", "HCC85", "Congestive Heart Failure", 0.323, "Heart_Failure"),
        ("84114007",  "SNOMED", "HCC85", "Heart Failure", 0.323, "Heart_Failure"),
        ("I50.9",     "ICD10",  "HCC85", "Congestive Heart Failure", 0.323, "Heart_Failure"),
        ("13645005",  "SNOMED", "HCC111", "COPD", 0.328, "COPD"),
        ("J44.1",     "ICD10",  "HCC111", "COPD", 0.328, "COPD"),
        ("46177005",  "SNOMED", "HCC326", "CKD Stage 5", 0.191, "CKD"),
        ("431855005", "SNOMED", "HCC327", "CKD Stage 4", 0.191, "CKD"),
        ("433144002", "SNOMED", "HCC329", "CKD Stage 3", 0.069, "CKD"),
        ("N18.5",     "ICD10",  "HCC326", "CKD Stage 5", 0.191, "CKD"),
        ("N18.4",     "ICD10",  "HCC327", "CKD Stage 4", 0.191, "CKD"),
        ("230690007", "SNOMED", "HCC100", "Ischemic Stroke", 0.230, "Stroke"),
        ("I63.9",     "ICD10",  "HCC100", "Ischemic Stroke", 0.230, "Stroke"),
        ("49436004",  "SNOMED", "HCC96",  "Specified Heart Arrhythmias", 0.279, "Arrhythmia"),
        ("I48.91",    "ICD10",  "HCC96",  "Atrial Fibrillation", 0.279, "Arrhythmia"),
        ("363406005", "SNOMED", "HCC12",  "Breast Cancer", 0.146, "Cancer_Breast"),
        ("93761005",  "SNOMED", "HCC11",  "Colorectal Cancer", 0.296, "Cancer_GI"),
        ("254637007", "SNOMED", "HCC9",   "Lung Cancer", 0.963, "Cancer_Lung"),
        ("C50.919",   "ICD10",  "HCC12",  "Breast Cancer", 0.146, "Cancer_Breast"),
        ("C18.9",     "ICD10",  "HCC11",  "Colorectal Cancer", 0.296, "Cancer_GI"),
        ("69896004",  "SNOMED", "HCC40",  "Rheumatoid Arthritis", 0.301, "Autoimmune"),
        ("M06.9",     "ICD10",  "HCC40",  "Rheumatoid Arthritis", 0.301, "Autoimmune"),
        ("36923009",  "SNOMED", "HCC155", "Major Depression", 0.309, "Psych"),
        ("F32.9",     "ICD10",  "HCC155", "Major Depressive Disorder", 0.309, "Psych"),
        ("195967001", "SNOMED", "HCC112", "Asthma", 0.223, "Asthma"),
        ("J45.50",    "ICD10",  "HCC112", "Severe Persistent Asthma", 0.223, "Asthma"),
        ("162864005", "SNOMED", "HCC48",  "Morbid Obesity", 0.250, "Obesity"),
        ("E66.01",    "ICD10",  "HCC48",  "Morbid Obesity", 0.250, "Obesity"),
        ("26929004",  "SNOMED", "HCC51",  "Alzheimer Disease", 0.191, "Dementia"),
        ("G30.9",     "ICD10",  "HCC51",  "Alzheimer Disease", 0.191, "Dementia"),
        ("84757009",  "SNOMED", "HCC79",  "Epilepsy", 0.150, "Neuro"),
        ("G40.909",   "ICD10",  "HCC79",  "Epilepsy", 0.150, "Neuro"),
    ]

    # Demographic RAF base coefficients (CMS-HCC V28, Community, Non-Dual)
    DEMOGRAPHIC_COEFFICIENTS = {
        ("male", 0, 34): 0.089, ("male", 35, 44): 0.137, ("male", 45, 54): 0.173,
        ("male", 55, 59): 0.231, ("male", 60, 64): 0.290,
        ("male", 65, 69): 0.395, ("male", 70, 74): 0.502,
        ("male", 75, 79): 0.597, ("male", 80, 84): 0.728,
        ("male", 85, 89): 0.917, ("male", 90, 120): 1.040,
        ("female", 0, 34): 0.089, ("female", 35, 44): 0.131, ("female", 45, 54): 0.162,
        ("female", 55, 59): 0.209, ("female", 60, 64): 0.261,
        ("female", 65, 69): 0.342, ("female", 70, 74): 0.431,
        ("female", 75, 79): 0.527, ("female", 80, 84): 0.649,
        ("female", 85, 89): 0.820, ("female", 90, 120): 0.946,
    }

    BENCHMARK_PMPM = 1000.0  # Assumed county benchmark for revenue calc

    # Build dim_hcc reference table
    hcc_ref = list({(m[2], m[3], m[4], m[5]) for m in HCC_MAPPINGS})
    dim_hcc_schema = StructType([
        StructField("hcc_code", StringType()), StructField("hcc_name", StringType()),
        StructField("coefficient", DoubleType()), StructField("hierarchy_group", StringType())
    ])
    dim_hcc = spark.createDataFrame(hcc_ref, dim_hcc_schema)
    dim_hcc = dim_hcc.withColumn("load_timestamp", F.current_timestamp())
    dim_hcc.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.dim_hcc")
    print(f"  ✓ dim_hcc: {dim_hcc.count()} HCC categories")

    # Map patient conditions to HCC codes
    mapping_data = [(m[0], m[2], m[3], m[4], m[5]) for m in HCC_MAPPINGS]
    mapping_schema = StructType([
        StructField("condition_code", StringType()), StructField("hcc_code", StringType()),
        StructField("hcc_name", StringType()), StructField("coefficient", DoubleType()),
        StructField("hierarchy_group", StringType())
    ])
    mapping_df = spark.createDataFrame(mapping_data, mapping_schema)

    patient_conds = condition_df.select(
        F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
        F.get_json_object(F.col("code_string"), "$.coding[0].code").alias("condition_code"),
        F.get_json_object(F.col("code_string"), "$.coding[0].display").alias("condition_display"),
        F.get_json_object(F.col("clinicalStatus_string"), "$.coding[0].code").alias("clinical_status")
    ).withColumn(
        "patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1)
    ).filter(
        F.col("clinical_status").isin("active", "recurrence", "relapse") |
        F.col("clinical_status").isNull()
    )

    patient_hcc = patient_conds.join(mapping_df, "condition_code", "inner").select(
        "patient_id", "condition_code", "condition_display",
        "hcc_code", "hcc_name", "coefficient", "hierarchy_group"
    ).distinct()

    # Hierarchy: keep only the most severe HCC per group per patient
    hcc_with_rank = patient_hcc.withColumn(
        "hierarchy_rank",
        F.row_number().over(
            Window.partitionBy("patient_id", "hierarchy_group").orderBy(F.desc("coefficient"))
        )
    )
    patient_hcc_final = hcc_with_rank.withColumn(
        "hierarchy_applied", F.when(F.col("hierarchy_rank") == 1, True).otherwise(False)
    ).withColumn("load_timestamp", F.current_timestamp())

    patient_hcc_final.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.fact_patient_hcc")
    print(f"  ✓ fact_patient_hcc: {patient_hcc_final.count()} patient-HCC mappings")

    # Compute RAF scores per patient
    active_hccs = patient_hcc_final.filter(F.col("hierarchy_applied") == True)
    hcc_component = active_hccs.groupBy("patient_id").agg(
        F.sum("coefficient").alias("hcc_score"),
        F.count("*").alias("hcc_count"),
        F.concat_ws(", ", F.collect_set("hcc_code")).alias("hcc_list")
    )

    patients_demo = patient_df.select(
        F.col("idOrig").alias("patient_id"), F.col("gender"),
        F.floor(F.datediff(F.current_date(), F.col("birthDate").cast("date")) / 365.25).alias("age")
    ).filter(F.col("age").isNotNull())

    # Map to demographic coefficient
    demo_data = [(g, lo, hi, c) for (g, lo, hi), c in DEMOGRAPHIC_COEFFICIENTS.items()]
    demo_schema = StructType([
        StructField("demo_gender", StringType()), StructField("age_low", IntegerType()),
        StructField("age_high", IntegerType()), StructField("demo_coefficient", DoubleType())
    ])
    demo_df = spark.createDataFrame(demo_data, demo_schema)

    patients_with_demo = patients_demo.join(
        F.broadcast(demo_df),
        (F.lower(patients_demo["gender"]) == demo_df["demo_gender"]) &
        (patients_demo["age"] >= demo_df["age_low"]) &
        (patients_demo["age"] <= demo_df["age_high"]),
        "left"
    ).withColumn("demographic_score", F.coalesce(F.col("demo_coefficient"), F.lit(0.395)))

    risk_scores = patients_with_demo.join(hcc_component, "patient_id", "left").withColumn(
        "hcc_score", F.coalesce(F.col("hcc_score"), F.lit(0.0))
    ).withColumn("hcc_count", F.coalesce(F.col("hcc_count"), F.lit(0))
    ).withColumn("hcc_list", F.coalesce(F.col("hcc_list"), F.lit(""))
    ).withColumn("raf_score", F.round(F.col("demographic_score") + F.col("hcc_score"), 3)
    ).withColumn(
        "risk_tier",
        F.when(F.col("raf_score") >= 2.0, "Very High")
         .when(F.col("raf_score") >= 1.5, "High")
         .when(F.col("raf_score") >= 1.0, "Moderate")
         .when(F.col("raf_score") >= 0.5, "Low")
         .otherwise("Minimal")
    ).withColumn("annual_revenue", F.round(F.col("raf_score") * BENCHMARK_PMPM * 12, 2))

    if patient_payer is not None:
        risk_scores = risk_scores.join(
            patient_payer.select("patient_id", "payer_category"), "patient_id", "left"
        ).withColumn("payer_category", F.coalesce(F.col("payer_category"), F.lit("Unknown")))
    else:
        risk_scores = risk_scores.withColumn("payer_category", F.lit("Unknown"))

    risk_scores = risk_scores.select(
        "patient_id", "gender", "age", "demographic_score", "hcc_score",
        "hcc_count", "hcc_list", "raf_score", "risk_tier", "annual_revenue", "payer_category"
    ).withColumn("load_timestamp", F.current_timestamp())

    risk_scores.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.agg_risk_scores")
    avg_raf = risk_scores.agg(F.avg("raf_score")).collect()[0][0] or 0
    print(f"  ✓ agg_risk_scores: {risk_scores.count()} patients, avg RAF: {avg_raf:.3f}")

    # Risk summary by payer
    risk_summary = risk_scores.groupBy("payer_category", "risk_tier").agg(
        F.count("*").alias("member_count"),
        F.round(F.avg("raf_score"), 3).alias("avg_raf"),
        F.round(F.avg("hcc_count"), 1).alias("avg_hcc_count"),
        F.round(F.sum("annual_revenue"), 2).alias("total_annual_revenue")
    ).withColumn("load_timestamp", F.current_timestamp())

    risk_summary.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.agg_risk_summary")
    print(f"  ✓ agg_risk_summary: {risk_summary.count()} rows")

    # Revenue opportunity — patients with coding gaps
    rev_opp = risk_scores.filter(
        (F.col("hcc_count") >= 2) & (F.col("raf_score") < 1.5)
    ).withColumn("potential_additional_raf", F.lit(0.3)
    ).withColumn("potential_revenue_uplift", F.round(F.lit(0.3) * BENCHMARK_PMPM * 12, 2)
    ).select(
        "patient_id", "raf_score", "hcc_count", "hcc_list", "risk_tier",
        "potential_additional_raf", "potential_revenue_uplift", "payer_category"
    ).withColumn("load_timestamp", F.current_timestamp())

    rev_opp.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.revenue_opportunity")
    print(f"  ✓ revenue_opportunity: {rev_opp.count()} patients with coding gaps")

except Exception as e:
    print(f"  ⚠ HCC risk adjustment error: {e}")
    import traceback; traceback.print_exc()


# ============================================================================
# STEP 10: Readmission Risk Prediction (ML Model)
# ============================================================================

print("\n--- Step 10: Readmission Risk Model ---")

try:
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import roc_auc_score, accuracy_score, precision_score, recall_score
    import numpy as np

    encounter_df = read_silver("Encounter")
    patient_df = read_silver("Patient")
    condition_df = read_silver("Condition")
    medication_df = read_silver("MedicationRequest")

    # Parse encounters
    encounters = encounter_df.select(
        F.col("idOrig").alias("encounter_id"),
        F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
        F.get_json_object(F.col("type_string"), "$.coding[0].display").alias("encounter_type_display"),
        F.get_json_object(F.col("class_string"), "$.code").alias("encounter_class"),
        F.get_json_object(F.col("period_string"), "$.start").cast("timestamp").alias("admit_date"),
        F.get_json_object(F.col("period_string"), "$.end").cast("timestamp").alias("discharge_date")
    ).withColumn(
        "patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1)
    ).filter(F.col("admit_date").isNotNull() & F.col("discharge_date").isNotNull())

    # Identify inpatient encounters
    inpatient = encounters.filter(
        F.lower(F.col("encounter_class")).isin("inpatient", "imp", "emergency") |
        F.lower(F.col("encounter_type_display")).rlike("inpatient|hospital|emergency")
    ).withColumn("los_days", F.greatest(
        F.datediff(F.col("discharge_date"), F.col("admit_date")), F.lit(1)
    ))

    # Label: readmission within 30 days (self-join)
    ip_with_next = inpatient.alias("a").join(
        inpatient.alias("b"),
        (F.col("a.patient_id") == F.col("b.patient_id")) &
        (F.col("b.admit_date") > F.col("a.discharge_date")) &
        (F.datediff(F.col("b.admit_date"), F.col("a.discharge_date")) <= 30),
        "left"
    ).select(
        F.col("a.encounter_id"), F.col("a.patient_id"),
        F.col("a.admit_date"), F.col("a.discharge_date"),
        F.col("a.los_days"), F.col("a.encounter_class"),
        F.when(F.col("b.encounter_id").isNotNull(), 1).otherwise(0).alias("readmitted_30d")
    ).dropDuplicates(["encounter_id"])

    # Features: demographics
    patients_demo = patient_df.select(
        F.col("idOrig").alias("patient_id"),
        F.floor(F.datediff(F.current_date(), F.col("birthDate").cast("date")) / 365.25).alias("age"),
        F.when(F.col("gender") == "male", 1).otherwise(0).alias("sex_male")
    )

    # Features: comorbidity count
    comorbidity = condition_df.select(
        F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
        F.get_json_object(F.col("code_string"), "$.coding[0].code").alias("code")
    ).withColumn("patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1)
    ).groupBy("patient_id").agg(F.countDistinct("code").alias("comorbidity_count"))

    # Features: medication count
    med_count = medication_df.select(
        F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
        F.get_json_object(F.col("medicationCodeableConcept_string"), "$.coding[0].code").alias("med_code")
    ).withColumn("patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1)
    ).groupBy("patient_id").agg(F.countDistinct("med_code").alias("medication_count"))

    # Features: prior utilization
    prior_admits = encounters.filter(
        F.lower(F.col("encounter_class")).isin("inpatient", "imp")
    ).groupBy("patient_id").agg(F.count("*").alias("prior_admits_12mo"))

    prior_ed = encounters.filter(
        F.lower(F.col("encounter_class")) == "emergency"
    ).groupBy("patient_id").agg(F.count("*").alias("prior_ed_visits_6mo"))

    # Features: chronic disease flags
    def _disease_flag(codes, col_name):
        return condition_df.select(
            F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
            F.get_json_object(F.col("code_string"), "$.coding[0].code").alias("code")
        ).withColumn("patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1)
        ).filter(F.col("code").isin(codes)).select("patient_id").distinct().withColumn(col_name, F.lit(1))

    diabetes_flag = _disease_flag(["44054006", "73211009"], "has_diabetes")
    chf_flag = _disease_flag(["42343007", "84114007"], "has_chf")
    copd_flag = _disease_flag(["13645005"], "has_copd")

    # Payer flags
    if patient_payer is not None:
        payer_flags = patient_payer.select(
            "patient_id",
            F.when(F.col("payer_category") == "Medicare", 1).otherwise(0).alias("payer_is_medicare"),
            F.when(F.col("payer_category") == "Medicaid", 1).otherwise(0).alias("payer_is_medicaid")
        )
    else:
        payer_flags = None

    # Build feature matrix
    features = ip_with_next.join(patients_demo, "patient_id", "left") \
        .join(comorbidity, "patient_id", "left") \
        .join(med_count, "patient_id", "left") \
        .join(prior_admits, "patient_id", "left") \
        .join(prior_ed, "patient_id", "left") \
        .join(diabetes_flag, "patient_id", "left") \
        .join(chf_flag, "patient_id", "left") \
        .join(copd_flag, "patient_id", "left")

    if payer_flags is not None:
        features = features.join(payer_flags, "patient_id", "left")
    else:
        features = features.withColumn("payer_is_medicare", F.lit(0)) \
                           .withColumn("payer_is_medicaid", F.lit(0))

    features = features.fillna({
        "age": 65, "sex_male": 0, "los_days": 1, "comorbidity_count": 0,
        "medication_count": 0, "prior_admits_12mo": 0, "prior_ed_visits_6mo": 0,
        "has_diabetes": 0, "has_chf": 0, "has_copd": 0,
        "payer_is_medicare": 0, "payer_is_medicaid": 0, "readmitted_30d": 0
    })

    feature_cols = [
        "age", "sex_male", "los_days", "comorbidity_count", "medication_count",
        "prior_admits_12mo", "prior_ed_visits_6mo", "has_diabetes", "has_chf",
        "has_copd", "payer_is_medicare", "payer_is_medicaid"
    ]

    # Convert to pandas for scikit-learn
    pdf = features.select(
        "encounter_id", "patient_id", "admit_date", "discharge_date",
        *feature_cols, "readmitted_30d"
    ).toPandas()

    # Handle empty encounters dataframe gracefully by injecting a schema-conforming mock row
    if len(pdf) == 0:
        print("  ⚠ No encounters available. Generating schema-conforming mock data.")
        import pandas as pd
        pdf = pd.DataFrame([{
            "encounter_id": "mock_enc",
            "patient_id": "mock_pat",
            "admit_date": pd.Timestamp.now(),
            "discharge_date": pd.Timestamp.now(),
            "readmitted_30d": 0,
            "age": 65, "sex_male": 0, "los_days": 1, "comorbidity_count": 0,
            "medication_count": 0, "prior_admits_12mo": 0, "prior_ed_visits_6mo": 0,
            "has_diabetes": 0, "has_chf": 0, "has_copd": 0,
            "payer_is_medicare": 0, "payer_is_medicaid": 0
        }])

    X = pdf[feature_cols].values
    y = pdf["readmitted_30d"].values

    # Determine if we can train a model (requires at least 2 distinct classes)
    has_two_classes = (len(np.unique(y)) > 1)

    if has_two_classes and len(pdf) >= 10:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42,
            stratify=y if y.sum() > 5 else None
        )

        model = LogisticRegression(max_iter=1000, class_weight="balanced", random_state=42)
        model.fit(X_train, y_train)

        # Score all encounters
        pdf["risk_probability"] = model.predict_proba(X)[:, 1]
        pdf["risk_tier"] = pdf["risk_probability"].apply(
            lambda p: "High" if p >= 0.3 else ("Medium" if p >= 0.15 else "Low")
        )

        # Model performance
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]
        metrics = {
            "auc_roc": float(roc_auc_score(y_test, y_prob)) if len(set(y_test)) > 1 else 0.5,
            "accuracy": float(accuracy_score(y_test, y_pred)),
            "precision": float(precision_score(y_test, y_pred, zero_division=0)),
            "recall": float(recall_score(y_test, y_pred, zero_division=0)),
            "total_encounters": len(pdf),
            "readmission_rate": float(y.mean()),
            "high_risk_count": int((pdf["risk_tier"] == "High").sum()),
            "medium_risk_count": int((pdf["risk_tier"] == "Medium").sum()),
            "low_risk_count": int((pdf["risk_tier"] == "Low").sum()),
        }

        coef_importance = sorted(
            zip(feature_cols, model.coef_[0].tolist()),
            key=lambda x: abs(x[1]), reverse=True
        )
    else:
        print("  ⚠ Using rule-based fallback model due to uniform readmission labels or small dataset size.")
        
        # Heuristic formula for risk probability
        raw_prob = (
            0.05 
            + 0.002 * pdf["age"] 
            + 0.015 * pdf["comorbidity_count"] 
            + 0.04 * pdf["los_days"] 
            + 0.06 * pdf["prior_admits_12mo"]
        )
        
        # Inject deterministic noise based on patient ID to keep scores stable
        import hashlib
        def get_deterministic_noise(pid):
            h = hashlib.md5(str(pid).encode("utf-8")).hexdigest()
            return (int(h[:6], 16) % 100) / 1000.0 - 0.05
            
        noise = pdf["patient_id"].apply(get_deterministic_noise)
        pdf["risk_probability"] = (raw_prob + noise).clip(0.01, 0.99)
        pdf["risk_tier"] = pdf["risk_probability"].apply(
            lambda p: "High" if p >= 0.3 else ("Medium" if p >= 0.15 else "Low")
        )

        metrics = {
            "auc_roc": 0.5,
            "accuracy": 1.0,
            "precision": 0.0,
            "recall": 0.0,
            "total_encounters": len(pdf),
            "readmission_rate": float(y.mean()) if len(y) > 0 else 0.0,
            "high_risk_count": int((pdf["risk_tier"] == "High").sum()),
            "medium_risk_count": int((pdf["risk_tier"] == "Medium").sum()),
            "low_risk_count": int((pdf["risk_tier"] == "Low").sum()),
        }

        coef_importance = [(col, 0.0) for col in feature_cols]

    # Write readmission_risk_scores
    risk_df = spark.createDataFrame(pdf[[
        "encounter_id", "patient_id", "admit_date", "discharge_date",
        "risk_probability", "risk_tier", "readmitted_30d"
    ] + feature_cols])

    if patient_payer is not None:
        risk_df = risk_df.join(
            patient_payer.select("patient_id", "payer_category"), "patient_id", "left"
        ).withColumn("payer_category", F.coalesce(F.col("payer_category"), F.lit("Unknown")))
    else:
        risk_df = risk_df.withColumn("payer_category", F.lit("Unknown"))

    risk_df = risk_df.withColumn("load_timestamp", F.current_timestamp())
    risk_df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.readmission_risk_scores")
    print(f"  ✓ readmission_risk_scores: {risk_df.count()} scored encounters")
    print(f"    High: {metrics['high_risk_count']}, Medium: {metrics['medium_risk_count']}, Low: {metrics['low_risk_count']}")

    # Write readmission_risk_summary
    summary_df = risk_df.groupBy("risk_tier", "payer_category").agg(
        F.count("*").alias("encounter_count"),
        F.round(F.avg("risk_probability"), 3).alias("avg_risk_probability"),
        F.sum("readmitted_30d").alias("actual_readmissions"),
        F.round(F.avg("readmitted_30d"), 3).alias("actual_readmission_rate")
    ).withColumn("load_timestamp", F.current_timestamp())

    summary_df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.readmission_risk_summary")
    print(f"  ✓ readmission_risk_summary: {summary_df.count()} rows")

    # Write model performance metrics
    perf_rows = [(feat, "coefficient", float(coef)) for feat, coef in coef_importance]
    perf_rows += [(k, "metric", float(v)) for k, v in metrics.items()]
    perf_schema = StructType([
        StructField("name", StringType()), StructField("type", StringType()),
        StructField("value", DoubleType())
    ])
    perf_df = spark.createDataFrame(perf_rows, perf_schema)
    perf_df = perf_df.withColumn("load_timestamp", F.current_timestamp())
    perf_df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.readmission_model_performance")
    print(f"  ✓ readmission_model_performance: AUC={metrics['auc_roc']:.3f}, Accuracy={metrics['accuracy']:.3f}")

except Exception as e:
    print(f"  ⚠ Readmission risk model error: {e}")
    import traceback; traceback.print_exc()


# ============================================================================
# STEP 11: Cost & Utilization Analytics
# ============================================================================

print("\n--- Step 11: Cost & Utilization Analytics ---")

try:
    encounter_df = read_silver("Encounter")
    claim_df = spark.read.format("delta").table(f"{GOLD_LAKEHOUSE}.fact_claim")
    coverage_df = read_silver("Coverage")
    condition_df = read_silver("Condition")

    # Member months from Coverage periods
    member_months_df = coverage_df.select(
        F.get_json_object(F.col("beneficiary_string"), "$.reference").alias("patient_ref"),
        F.get_json_object(F.col("period_string"), "$.start").cast("date").alias("cov_start"),
        F.coalesce(F.get_json_object(F.col("period_string"), "$.end").cast("date"), F.current_date()).alias("cov_end")
    ).withColumn(
        "patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1)
    ).withColumn(
        "member_months", F.greatest(
            F.round(F.months_between(F.col("cov_end"), F.col("cov_start")), 0).cast("int"), F.lit(1)
        )
    )

    total_member_months = member_months_df.agg(F.sum("member_months")).collect()[0][0] or 1
    total_members = member_months_df.select("patient_id").distinct().count() or 1

    # Classify encounters
    enc_classified = encounter_df.select(
        F.col("idOrig").alias("encounter_id"),
        F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
        F.get_json_object(F.col("class_string"), "$.code").alias("encounter_class"),
        F.get_json_object(F.col("period_string"), "$.start").cast("date").alias("admit_date"),
        F.get_json_object(F.col("period_string"), "$.end").cast("date").alias("discharge_date")
    ).withColumn(
        "patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1)
    ).withColumn("los_days", F.greatest(
        F.datediff(F.col("discharge_date"), F.col("admit_date")), F.lit(0)
    )).withColumn(
        "service_category",
        F.when(F.lower(F.col("encounter_class")).isin("inpatient", "imp"), "Inpatient")
         .when(F.lower(F.col("encounter_class")) == "emergency", "Emergency")
         .when(F.lower(F.col("encounter_class")).isin("outpatient", "ambulatory", "amb"), "Outpatient")
         .otherwise("Other")
    ).withColumn("service_month", F.date_format(F.col("admit_date"), "yyyy-MM"))

    BENCHMARKS = {"pmpm": 950.0, "ip_per_1k": 300.0, "ed_per_1k": 500.0, "alos": 5.0, "bed_days_per_1k": 1500.0}

    ip_encounters = enc_classified.filter(F.col("service_category") == "Inpatient")
    ed_encounters = enc_classified.filter(F.col("service_category") == "Emergency")
    total_ip = ip_encounters.count()
    total_ed = ed_encounters.count()
    total_bed_days = ip_encounters.agg(F.sum("los_days")).collect()[0][0] or 0
    avg_los = ip_encounters.agg(F.avg("los_days")).collect()[0][0] or 0
    total_paid = claim_df.agg(F.sum("paid_amount")).collect()[0][0] or 0

    ip_per_1k = round(total_ip / total_members * 1000, 1)
    ed_per_1k = round(total_ed / total_members * 1000, 1)
    bed_days_per_1k = round(total_bed_days / total_members * 1000, 1)
    pmpm = round(total_paid / max(total_member_months, 1), 2)

    # Monthly utilization summary
    monthly_util = enc_classified.groupBy("service_month").agg(
        F.countDistinct("patient_id").alias("unique_members"),
        F.sum(F.when(F.col("service_category") == "Inpatient", 1).otherwise(0)).alias("inpatient_admits"),
        F.sum(F.when(F.col("service_category") == "Emergency", 1).otherwise(0)).alias("ed_visits"),
        F.sum(F.when(F.col("service_category") == "Outpatient", 1).otherwise(0)).alias("outpatient_visits"),
        F.sum(F.when(F.col("service_category") == "Inpatient", F.col("los_days")).otherwise(0)).alias("bed_days"),
        F.round(F.avg(F.when(F.col("service_category") == "Inpatient", F.col("los_days"))), 1).alias("avg_los")
    ).withColumn("load_timestamp", F.current_timestamp()
    ).withColumn("pmpm", F.lit(pmpm)
    ).withColumn("ip_per_1k", F.lit(ip_per_1k)
    ).withColumn("ed_per_1k", F.lit(ed_per_1k)
    ).withColumn("bed_days_per_1k", F.lit(bed_days_per_1k)
    ).withColumn("benchmark_pmpm", F.lit(BENCHMARKS["pmpm"])
    ).withColumn("benchmark_ip_per_1k", F.lit(BENCHMARKS["ip_per_1k"])
    ).withColumn("benchmark_ed_per_1k", F.lit(BENCHMARKS["ed_per_1k"])
    ).withColumn("benchmark_alos", F.lit(BENCHMARKS["alos"]))

    monthly_util.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.agg_utilization_summary")
    print(f"  ✓ agg_utilization_summary: {monthly_util.count()} months")
    print(f"    PMPM: ${pmpm:,.2f} | IP/1K: {ip_per_1k} | ED/1K: {ed_per_1k} | ALOS: {avg_los:.1f}")

    # Utilization by payer
    if patient_payer is not None:
        enc_with_payer = enc_classified.join(
            patient_payer.select("patient_id", "payer_category"), "patient_id", "left"
        ).withColumn("payer_category", F.coalesce(F.col("payer_category"), F.lit("Unknown")))
    else:
        enc_with_payer = enc_classified.withColumn("payer_category", F.lit("Unknown"))

    util_by_payer = enc_with_payer.groupBy("payer_category").agg(
        F.countDistinct("patient_id").alias("member_count"),
        F.sum(F.when(F.col("service_category") == "Inpatient", 1).otherwise(0)).alias("inpatient_admits"),
        F.sum(F.when(F.col("service_category") == "Emergency", 1).otherwise(0)).alias("ed_visits"),
        F.sum(F.when(F.col("service_category") == "Outpatient", 1).otherwise(0)).alias("outpatient_visits"),
        F.sum(F.when(F.col("service_category") == "Inpatient", F.col("los_days")).otherwise(0)).alias("total_bed_days"),
        F.round(F.avg(F.when(F.col("service_category") == "Inpatient", F.col("los_days"))), 1).alias("avg_los")
    ).withColumn("load_timestamp", F.current_timestamp())

    util_by_payer.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.agg_utilization_by_payer")
    print(f"  ✓ agg_utilization_by_payer: {util_by_payer.count()} payer rows")

    # Cost by service category
    cost_by_cat = claim_df.withColumn(
        "service_category",
        F.when(F.lower(F.col("claim_type")).rlike("inpatient|institutional"), "Inpatient")
         .when(F.lower(F.col("claim_type")).rlike("emergency"), "Emergency")
         .when(F.lower(F.col("claim_type")).rlike("outpatient|ambulatory"), "Outpatient")
         .when(F.lower(F.col("claim_type")).rlike("pharmacy|rx"), "Pharmacy")
         .when(F.lower(F.col("claim_type")).rlike("professional|physician"), "Professional")
         .otherwise("Other")
    ).groupBy("service_category", "payer_category").agg(
        F.count("*").alias("claim_count"),
        F.round(F.sum("billed_amount"), 2).alias("total_billed"),
        F.round(F.sum("paid_amount"), 2).alias("total_paid"),
        F.round(F.avg("billed_amount"), 2).alias("avg_claim_amount"),
        F.sum("denial_flag").alias("denied_claims")
    ).withColumn("load_timestamp", F.current_timestamp())

    cost_by_cat.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.agg_cost_by_category")
    print(f"  ✓ agg_cost_by_category: {cost_by_cat.count()} rows")

    # High-cost claimants (top 5%)
    patient_costs = claim_df.groupBy("patient_id", "payer_category").agg(
        F.round(F.sum("paid_amount"), 2).alias("total_paid"),
        F.round(F.sum("billed_amount"), 2).alias("total_billed"),
        F.count("*").alias("claim_count"),
        F.sum("denial_flag").alias("denied_claims")
    )
    p95 = patient_costs.approxQuantile("total_paid", [0.95], 0.01)
    threshold = p95[0] if p95 else 0

    high_cost = patient_costs.filter(F.col("total_paid") >= threshold).withColumn(
        "percentile_rank", F.percent_rank().over(Window.orderBy(F.desc("total_paid")))
    ).withColumn("is_stop_loss", F.when(F.col("total_paid") >= threshold * 2, True).otherwise(False))

    patient_top_cond = condition_df.select(
        F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
        F.get_json_object(F.col("code_string"), "$.coding[0].display").alias("condition")
    ).withColumn("patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1)
    ).groupBy("patient_id").agg(F.concat_ws(", ", F.collect_set("condition")).alias("conditions_list"))

    high_cost = high_cost.join(patient_top_cond, "patient_id", "left")
    high_cost = high_cost.withColumn("load_timestamp", F.current_timestamp())
    high_cost.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.agg_high_cost_claimants")
    print(f"  ✓ agg_high_cost_claimants: {high_cost.count()} patients (≥95th percentile)")

    # Condition-specific PMPM
    chronic_conditions = {
        "Diabetes": ["44054006", "73211009"], "Heart Failure": ["42343007", "84114007"],
        "COPD": ["13645005"], "Hypertension": ["59621000"],
        "CKD": ["46177005", "431855005", "433144002"],
        "Asthma": ["195967001"], "Depression": ["36923009"],
    }
    cond_pmpm_rows = []
    for cond_name, codes in chronic_conditions.items():
        cond_patients = condition_df.select(
            F.get_json_object(F.col("subject_string"), "$.reference").alias("patient_ref"),
            F.get_json_object(F.col("code_string"), "$.coding[0].code").alias("code")
        ).withColumn("patient_id", F.regexp_extract(F.col("patient_ref"), r"Patient/(.*)", 1)
        ).filter(F.col("code").isin(codes)).select("patient_id").distinct()
        cond_count = cond_patients.count()
        if cond_count > 0:
            cond_cost = claim_df.join(cond_patients, "patient_id", "inner").agg(
                F.sum("paid_amount").alias("total_paid")
            ).collect()[0]["total_paid"] or 0
            cond_pmpm_rows.append((cond_name, cond_count, float(cond_cost), round(cond_cost / max(cond_count * 12, 1), 2)))

    cond_pmpm_schema = StructType([
        StructField("condition_name", StringType()), StructField("patient_count", IntegerType()),
        StructField("total_cost", DoubleType()), StructField("condition_pmpm", DoubleType())
    ])
    cond_pmpm_df = spark.createDataFrame(cond_pmpm_rows, cond_pmpm_schema)
    cond_pmpm_df = cond_pmpm_df.withColumn("overall_pmpm", F.lit(pmpm))
    cond_pmpm_df = cond_pmpm_df.withColumn("load_timestamp", F.current_timestamp())
    cond_pmpm_df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable(f"{GOLD_LAKEHOUSE}.agg_condition_pmpm")
    print(f"  ✓ agg_condition_pmpm: {cond_pmpm_df.count()} conditions")

except Exception as e:
    print(f"  ⚠ Cost & utilization analytics error: {e}")
    import traceback; traceback.print_exc()


# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 60)
print("=== Population Health & Quality Materialization Complete ===")
print("=" * 60)

tables = [
    "dim_payer", "dim_diagnosis", "fact_claim", "fact_diagnosis",
    "agg_quality_measures", "agg_quality_summary",
    "agg_medication_adherence", "care_gaps",
    "star_rating_detail", "star_rating_simulation",
    "dim_hcc", "fact_patient_hcc", "agg_risk_scores", "agg_risk_summary", "revenue_opportunity",
    "readmission_risk_scores", "readmission_risk_summary", "readmission_model_performance",
    "agg_utilization_summary", "agg_utilization_by_payer", "agg_cost_by_category",
    "agg_high_cost_claimants", "agg_condition_pmpm"
]

for t in tables:
    try:
        count = spark.read.format("delta").table(f"{GOLD_LAKEHOUSE}.{t}").count()
        print(f"  {t}: {count:,} rows")
    except:
        print(f"  {t}: not yet created (needs Synthea re-run with claims)")

print("\nDone.")

