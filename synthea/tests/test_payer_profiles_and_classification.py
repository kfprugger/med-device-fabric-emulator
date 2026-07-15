from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path
from typing import Any


REPO_DIR = Path(__file__).resolve().parents[2]
SYNTHEA_DIR = REPO_DIR / "synthea"
PHASE5_DIR = REPO_DIR / "phase-5"


def load_synthea_generator():
    module_name = "generate_cached_bundles_payer_profiles_under_test"
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


def load_payer_classifier():
    """Load the Spark-independent phase-5 payer classifier contract."""
    module_name = "payer_classification_under_test"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(
        module_name, PHASE5_DIR / "payer_classification.py"
    )
    if spec is None or spec.loader is None:
        raise AssertionError(
            "expected phase-5/payer_classification.py exposing "
            "classify_payer_category(coverage)"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    classifier = getattr(module, "classify_payer_category", None)
    if not callable(classifier):
        raise AssertionError(
            "payer_classification.py must expose "
            "classify_payer_category(coverage)"
        )
    return classifier


class SyntheticPayerProfileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.generator = load_synthea_generator()
        self.addCleanup(
            sys.modules.pop,
            "generate_cached_bundles_payer_profiles_under_test",
            None,
        )

    def test_profile_catalog_contains_all_supported_payer_categories(self) -> None:
        profiles = self.generator.PAYER_PROFILES

        self.assertEqual(
            {"Medicare", "Medicaid", "Commercial", "Uninsured"},
            {profile["category"] for profile in profiles},
        )
        self.assertEqual(
            {"payer-medicare", "payer-medicaid", "payer-commercial", "payer-uninsured"},
            {profile["id"] for profile in profiles},
        )

    def test_each_profile_generates_coverage_with_matching_payor_and_type_signals(self) -> None:
        for profile in self.generator.PAYER_PROFILES:
            with self.subTest(category=profile["category"]):
                coverage = self.generator.coverage_resource(
                    "coverage-test", "patient-test", profile
                )

                self.assertEqual("Coverage", coverage["resourceType"])
                self.assertEqual(profile["name"], coverage["type"]["text"])
                self.assertEqual(profile["name"], coverage["payor"][0]["display"])
                self.assertEqual(
                    profile["type_code"], coverage["type"]["coding"][0]["code"]
                )
                self.assertEqual(
                    profile["type_display"], coverage["type"]["coding"][0]["display"]
                )


class CoveragePayerClassificationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.classify = staticmethod(load_payer_classifier())

    def test_representative_coverage_signals_map_to_normalized_categories(self) -> None:
        cases: list[tuple[str, dict[str, Any], str]] = [
            (
                "type text",
                {"type": {"text": "Medicare Advantage"}},
                "Medicare",
            ),
            (
                "coding display",
                {"type": {"coding": [{"display": "State Medicaid program"}]}},
                "Medicaid",
            ),
            (
                "coding code",
                {"type": {"coding": [{"code": "commercial"}]}},
                "Commercial",
            ),
            (
                "payor display",
                {"payor": [{"display": "Acme Commercial Health Plan"}]},
                "Commercial",
            ),
            (
                "uninsured self pay",
                {"type": {"text": "Self-pay"}},
                "Uninsured",
            ),
        ]

        for name, coverage, expected in cases:
            with self.subTest(signal=name):
                self.assertEqual(expected, self.classify(coverage))

    def test_unknown_or_missing_coverage_signals_fall_back_to_unknown(self) -> None:
        cases = [
            ("empty coverage", {}),
            ("missing type and payor", {"status": "active"}),
            ("unknown type text", {"type": {"text": "Veterans benefit"}}),
            (
                "unknown coding",
                {"type": {"coding": [{"code": "ZZZ", "display": "Other"}]}},
            ),
            ("empty payor", {"payor": [{}]}),
        ]

        for name, coverage in cases:
            with self.subTest(signal=name):
                self.assertEqual("Unknown", self.classify(coverage))


if __name__ == "__main__":
    unittest.main()
