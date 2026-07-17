from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


SYNTHEA_DIR = Path(__file__).resolve().parents[1]
RACE_URL = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
ETHNICITY_URL = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"
CDC_RACE_ETHNICITY_SYSTEM = "urn:oid:2.16.840.1.113883.6.238"


def load_generate_cached_bundles():
    module_name = "generate_cached_bundles_under_test"
    spec = importlib.util.spec_from_file_location(
        module_name,
        SYNTHEA_DIR / "generate_cached_bundles.py",
    )
    if spec is None or spec.loader is None:
        raise AssertionError("could not load generate_cached_bundles.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def nested_extension_by_url(extension: dict) -> dict[str, dict]:
    return {child.get("url"): child for child in extension.get("extension", [])}


class PatientDemographicExtensionTests(unittest.TestCase):
    def setUp(self) -> None:
        sys.modules.pop("generate_cached_bundles_under_test", None)
        self.addCleanup(sys.modules.pop, "generate_cached_bundles_under_test", None)
        self.generator = load_generate_cached_bundles()

    def assert_demographic_extension(
        self,
        extension: dict,
        url: str,
        expected_code: str,
        expected_display: str,
    ) -> None:
        self.assertEqual(extension["url"], url)
        self.assertNotIn("valueCoding", extension)
        self.assertNotIn("valueString", extension)
        children = nested_extension_by_url(extension)
        self.assertEqual(
            set(children),
            {"ombCategory", "text"},
        )
        self.assertEqual(
            children["ombCategory"].get("valueCoding"),
            {
                "system": CDC_RACE_ETHNICITY_SYSTEM,
                "code": expected_code,
                "display": expected_display,
            },
        )
        self.assertEqual(children["text"].get("valueString"), expected_display)

    def test_patient_demographic_extensions_use_us_core_nested_shape_for_each_profile(self) -> None:
        for idx, profile in enumerate(self.generator.RACE_ETHNICITY_PROFILES):
            with self.subTest(profile=profile):
                extensions = self.generator.patient_demographic_extensions(idx)

                self.assertEqual(len(extensions), 2)
                extensions_by_url = {extension["url"]: extension for extension in extensions}
                self.assertEqual(set(extensions_by_url), {RACE_URL, ETHNICITY_URL})
                self.assert_demographic_extension(
                    extensions_by_url[RACE_URL],
                    RACE_URL,
                    profile["race_code"],
                    profile["race_display"],
                )
                self.assert_demographic_extension(
                    extensions_by_url[ETHNICITY_URL],
                    ETHNICITY_URL,
                    profile["ethnicity_code"],
                    profile["ethnicity_display"],
                )

    def test_generated_patient_resource_carries_race_and_ethnicity_extensions(self) -> None:
        named_uuids = iter([
            "patient-uuid",
            "encounter-uuid",
            "condition-uuid",
            "coverage-uuid",
            "goal-uuid",
            "care-plan-uuid",
            "oxygen-observation-uuid",
            "heart-rate-observation-uuid",
            "blood-pressure-observation-uuid",
            "medication-request-uuid",
            "procedure-uuid",
        ])
        with patch.object(self.generator.random, "choice", lambda values: values[0]), \
             patch.object(self.generator.random, "randint", lambda start, _end: start), \
             patch.object(self.generator.uuid, "uuid4", side_effect=lambda: next(named_uuids, "extra-encounter-uuid")):
            _filename, bundle = self.generator.generate_patient(4)

        patient = bundle["entry"][0]["resource"]
        self.assertEqual(patient["resourceType"], "Patient")
        extensions_by_url = {extension["url"]: extension for extension in patient["extension"]}
        self.assertEqual(set(extensions_by_url), {RACE_URL, ETHNICITY_URL})
        self.assert_demographic_extension(
            extensions_by_url[RACE_URL],
            RACE_URL,
            "2106-3",
            "White",
        )
        self.assert_demographic_extension(
            extensions_by_url[ETHNICITY_URL],
            ETHNICITY_URL,
            "2135-2",
            "Hispanic or Latino",
        )


if __name__ == "__main__":
    unittest.main()
