from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from typing import Any


SYNTHEA_DIR = Path(__file__).resolve().parents[1]


def load_synthea_generator() -> Any:
    module_name = "generate_cached_bundles_inpatient_under_test"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(
        module_name, SYNTHEA_DIR / "generate_cached_bundles.py"
    )
    if spec is None or spec.loader is None:
        raise AssertionError("could not load generate_cached_bundles.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class SyntheticInpatientEncounterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.generator = load_synthea_generator()

    @classmethod
    def tearDownClass(cls) -> None:
        sys.modules.pop("generate_cached_bundles_inpatient_under_test", None)

    @staticmethod
    def resources_by_type(bundle: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        resources: dict[str, list[dict[str, Any]]] = {}
        for entry in bundle["entry"]:
            resource = entry["resource"]
            resources.setdefault(resource["resourceType"], []).append(resource)
        return resources

    def generate_bundle(self, index: int) -> dict[str, Any]:
        _filename, bundle = self.generator.generate_patient(index)
        return bundle
    def test_small_index_range_contains_inpatient_and_ambulatory_encounters(self) -> None:
        encounters = []
        for index in range(4):
            resources = self.resources_by_type(self.generate_bundle(index))
            encounters.extend(resources["Encounter"])

        class_pairs = {(encounter["class"]["code"], encounter["class"]["display"]) for encounter in encounters}
        self.assertIn(("IMP", "inpatient encounter"), class_pairs)
        self.assertIn(("AMB", "ambulatory"), class_pairs)

    def test_inpatient_encounter_has_dates_and_retains_clinical_references(self) -> None:
        for index in range(4):
            bundle = self.generate_bundle(index)
            resources = self.resources_by_type(bundle)
            encounter = resources["Encounter"][0]
            patient = resources["Patient"][0]
            coverage = resources["Coverage"][0]

            if encounter["class"]["code"] != "IMP":
                continue

            self.assertEqual("inpatient encounter", encounter["class"]["display"])
            self.assertTrue(
                encounter.get("period", {}).get("start")
                or encounter.get("period", {}).get("end")
                or encounter.get("hospitalization")
            )
            patient_reference = f"urn:uuid:{patient['id']}"
            self.assertEqual(patient_reference, encounter["subject"]["reference"])
            hospital = encounter["serviceProvider"]["reference"].removeprefix("Organization/")
            self.assertIn(hospital, self.generator.ATLANTA_HOSPITALS)
            self.assertIn(hospital, encounter["location"][0]["location"]["reference"])

            self.assertEqual(patient_reference, coverage["beneficiary"]["reference"])
            self.assertTrue(coverage["payor"])
            self.assertTrue(coverage["payor"][0]["reference"])

    def test_each_encounter_hospital_is_an_atlanta_hospital(self) -> None:
        for index in range(4):
            resources = self.resources_by_type(self.generate_bundle(index))
            encounter = resources["Encounter"][0]
            hospital = encounter["serviceProvider"]["reference"].removeprefix("Organization/")
            self.assertIn(hospital, self.generator.ATLANTA_HOSPITALS)


if __name__ == "__main__":
    unittest.main()
