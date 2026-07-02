from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


PHASE2_DIR = Path(__file__).resolve().parents[1]


def load_seed_module():
    module_name = "seed_cma_sdoh_under_test"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, PHASE2_DIR / "seed_cma_sdoh.py")
    if spec is None or spec.loader is None:
        raise AssertionError("could not load seed_cma_sdoh.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class CmaSdohSeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.seed = load_seed_module()
        self.addCleanup(sys.modules.pop, "seed_cma_sdoh_under_test", None)

    def test_zip_to_fips_rows_are_deduplicated_sorted_and_default_unknown_zip_to_fulton(self) -> None:
        with patch.object(self.seed, "_now", return_value="2026-07-01T12:00:00"):
            rows = self.seed.build_zip_to_fips_rows(["30309", "99999", "30309", "30080"])

        self.assertEqual(["30080", "30309", "99999"], [row["zip"] for row in rows])
        self.assertEqual(
            {
                "id": "GA-30080",
                "zip": "30080",
                "stateAbbreviation": "GA",
                "countyFips": "13067",
                "countyName": "Cobb County",
                "SourceTable": "SyntheticSdohSeed",
                "SourceModifiedOn": "2026-07-01T12:00:00",
            },
            rows[0],
        )
        self.assertEqual("13121", rows[2]["countyFips"])
        self.assertEqual("Fulton County", rows[2]["countyName"])

    def test_dimension_rows_have_stable_keys_and_metadata_for_cma_model_tables(self) -> None:
        with patch.object(self.seed, "_now", return_value="2026-07-01T12:00:00"):
            dims = self.seed.build_sdoh_dimension_rows()

        self.assertEqual(
            {"sdoh_category", "sdoh_unitofmeasure", "sdoh_datasetmetadata", "sdoh_fips"},
            set(dims),
        )
        self.assertEqual(
            [category_id for category_id, _, _ in self.seed.SDOH_CATEGORIES],
            [row["category_id"] for row in dims["sdoh_category"]],
        )
        self.assertEqual(
            [(1, "USD"), (2, "Index")],
            [(row["unit_of_measure_id"], row["abbreviation"]) for row in dims["sdoh_unitofmeasure"]],
        )
        self.assertEqual("Synthetic Atlanta SDOH Seed", dims["sdoh_datasetmetadata"][0]["dataset_name"])
        self.assertEqual("2026-07-01", dims["sdoh_datasetmetadata"][0]["published_date"])
        self.assertEqual(
            sorted({fips for fips, _ in self.seed.ATLANTA_ZIP_FIPS.values()}),
            [row["fips_code"] for row in dims["sdoh_fips"]],
        )

    def test_social_determinant_rows_emit_one_deterministic_measure_per_zip_and_measure(self) -> None:
        zip_rows = [
            {
                "zip": "30309",
                "countyFips": "13121",
            }
        ]

        with patch.object(self.seed, "_now", return_value="2026-07-01T12:00:00"):
            rows = self.seed.build_social_determinant_rows(zip_rows)

        self.assertEqual(len(self.seed.SDOH_MEASURES), len(rows))
        self.assertEqual(list(range(1, len(rows) + 1)), [row["social_determinant_id"] for row in rows])
        rows_by_name = {row["name"]: row for row in rows}
        self.assertEqual({measure[3] for measure in self.seed.SDOH_MEASURES}, set(rows_by_name))

        seed_value = sum(ord(ch) for ch in "30309")
        self.assertEqual(float(52000 + (seed_value % 34) * 1000), rows_by_name["median_income"]["value_as_number"])
        self.assertEqual("USD", next(measure[4] for measure in self.seed.SDOH_MEASURES if measure[3] == "median_income"))
        self.assertEqual(1, rows_by_name["median_income"]["unit_of_measure_id"])
        self.assertEqual(2, rows_by_name["food_insecurity_index"]["unit_of_measure_id"])
        self.assertEqual("30309:primary_care_access", rows_by_name["primary_care_access"]["harmonization_key"])
        self.assertEqual('{"zip":"30309","countyFips":"13121"}', rows_by_name["transportation_barrier"]["location_JSON"])
        self.assertEqual("SyntheticSdohSeed", rows_by_name["rehospitalization_nonmetro"]["SourceTable"])


if __name__ == "__main__":
    unittest.main()
