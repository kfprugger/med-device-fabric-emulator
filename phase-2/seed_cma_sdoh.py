#!/usr/bin/env python3
"""Seed small deterministic SDOH tables for Healthcare Data Solutions CMA demos.

Designed for Fabric notebook/Spark execution against healthcare1_msft_gold_cma.
The pure row builders are intentionally dependency-free for local unit tests.
"""
from __future__ import annotations

from datetime import datetime

ATLANTA_ZIP_FIPS = {
    "30032": ("13089", "DeKalb County"),
    "30033": ("13089", "DeKalb County"),
    "30080": ("13067", "Cobb County"),
    "30082": ("13067", "Cobb County"),
    "30144": ("13067", "Cobb County"),
    "30303": ("13121", "Fulton County"),
    "30305": ("13121", "Fulton County"),
    "30308": ("13121", "Fulton County"),
    "30309": ("13121", "Fulton County"),
    "30310": ("13121", "Fulton County"),
    "30311": ("13121", "Fulton County"),
    "30312": ("13121", "Fulton County"),
    "30313": ("13121", "Fulton County"),
    "30314": ("13121", "Fulton County"),
    "30315": ("13121", "Fulton County"),
    "30316": ("13121", "Fulton County"),
    "30318": ("13121", "Fulton County"),
    "30319": ("13089", "DeKalb County"),
    "30324": ("13121", "Fulton County"),
    "30326": ("13121", "Fulton County"),
    "30327": ("13121", "Fulton County"),
    "30328": ("13121", "Fulton County"),
    "30329": ("13089", "DeKalb County"),
    "30331": ("13121", "Fulton County"),
    "30339": ("13067", "Cobb County"),
    "30342": ("13121", "Fulton County"),
}

SDOH_CATEGORIES = [
    (1, "Economic Stability", "Synthetic economic stability indicators"),
    (2, "Health Care Access and Quality", "Synthetic access indicators"),
    (3, "Neighborhood and Built Environment", "Synthetic built-environment indicators"),
]

SDOH_MEASURES = [
    (1, 1, "Median household income", "median_income", "USD", "US dollars"),
    (2, 1, "Food insecurity index", "food_insecurity_index", "Index", "0-100 index"),
    (3, 2, "Primary care access score", "primary_care_access", "Index", "0-100 index"),
    (4, 3, "Transportation barrier score", "transportation_barrier", "Index", "0-100 index"),
    (5, 3, "Non-metro rehospitalization risk", "rehospitalization_nonmetro", "Index", "0-100 index"),
]

SDOH_SOURCE_BANNER = (
    "INFO: CMA SDOH seed uses no external SDOH sources. "
    "Values are deterministic synthetic indicators generated from Synthea patient ZIP codes."
)



def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def build_zip_to_fips_rows(zips: list[str] | None = None) -> list[dict]:
    selected = sorted(set(zips or ATLANTA_ZIP_FIPS.keys()))
    rows = []
    for zip_code in selected:
        county_fips, county = ATLANTA_ZIP_FIPS.get(zip_code, ("13121", "Fulton County"))
        rows.append({
            "id": f"GA-{zip_code}",
            "zip": zip_code,
            "stateAbbreviation": "GA",
            "countyFips": county_fips,
            "countyName": county,
            "SourceTable": "SyntheticSdohSeed",
            "SourceModifiedOn": _now(),
        })
    return rows


def build_sdoh_dimension_rows() -> dict[str, list[dict]]:
    now = _now()
    return {
        "sdoh_category": [
            {
                "category_id": category_id,
                "category_name": name,
                "category_description": description,
                "subcategory_id": category_id * 10,
                "subcategory_name": name,
                "subcategory_description": description,
                "SourceTable": "SyntheticSdohSeed",
                "SourceModifiedOn": now,
            }
            for category_id, name, description in SDOH_CATEGORIES
        ],
        "sdoh_unitofmeasure": [
            {"unit_of_measure_id": 1, "name": "US Dollars", "abbreviation": "USD", "description": "US dollars", "SourceTable": "SyntheticSdohSeed", "SourceModifiedOn": now},
            {"unit_of_measure_id": 2, "name": "Index", "abbreviation": "Index", "description": "Synthetic 0-100 score", "SourceTable": "SyntheticSdohSeed", "SourceModifiedOn": now},
        ],
        "sdoh_datasetmetadata": [
            {"sdoh_datasetmetadata_id": 1, "dataset_name": "Synthetic Atlanta SDOH Seed", "publisher_name": "BrakeKat synthetic emulator", "published_date": now[:10], "SourceTable": "SyntheticSdohSeed", "SourceModifiedOn": now}
        ],
        "sdoh_fips": [
            {"fips_code": fips, "locationType": "county", "SourceTable": "SyntheticSdohSeed", "SourceModifiedOn": now}
            for fips in sorted({fips for fips, _ in ATLANTA_ZIP_FIPS.values()})
        ],
    }


def build_social_determinant_rows(zip_rows: list[dict]) -> list[dict]:
    now = _now()
    rows = []
    row_id = 1
    for zip_row in zip_rows:
        zip_code = zip_row["zip"]
        seed = sum(ord(ch) for ch in zip_code)
        values = {
            "median_income": 52000 + (seed % 34) * 1000,
            "food_insecurity_index": 20 + (seed % 31),
            "primary_care_access": 55 + (seed % 36),
            "transportation_barrier": 10 + (seed % 41),
            "rehospitalization_nonmetro": 5 + (seed % 26),
        }
        for measure_id, category_id, description, name, unit_name, _ in SDOH_MEASURES:
            unit_id = 1 if unit_name == "USD" else 2
            rows.append({
                "social_determinant_id": row_id,
                "sdoh_datasetmetadata_id": 1,
                "category_id": category_id,
                "unit_of_measure_id": unit_id,
                "name": name,
                "description": description,
                "value": str(values[name]),
                "value_as_number": float(values[name]),
                "location_type": "zip",
                "location_value": zip_code,
                "location_JSON": f'{{"zip":"{zip_code}","countyFips":"{zip_row["countyFips"]}"}}',
                "harmonization_key": f"{zip_code}:{name}",
                "SourceTable": "SyntheticSdohSeed",
                "SourceModifiedOn": now,
            })
            row_id += 1
    return rows


def write_spark_tables(spark, rows_by_table: dict[str, list[dict]]) -> None:
    for table_name, rows in rows_by_table.items():
        df = spark.createDataFrame(rows)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def main() -> None:
    zip_rows = build_zip_to_fips_rows()
    dims = build_sdoh_dimension_rows()
    social_rows = build_social_determinant_rows(zip_rows)
    rows_by_table = {"zip_to_fips_mapping": zip_rows, **dims, "social_determinant": social_rows}

    try:
        spark  # type: ignore[name-defined]
    except NameError as exc:
        raise SystemExit("Run this script in a Fabric Spark notebook attached to healthcare1_msft_gold_cma.") from exc

    print(SDOH_SOURCE_BANNER)
    write_spark_tables(spark, rows_by_table)  # type: ignore[name-defined]
    print("Seeded CMA SDOH tables:")
    for table_name, rows in rows_by_table.items():
        print(f"  {table_name}: {len(rows)}")


if __name__ == "__main__":
    main()
