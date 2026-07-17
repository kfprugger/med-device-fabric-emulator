from __future__ import annotations

import builtins
import importlib.util
import io
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


FHIR_LOADER_DIR = Path(__file__).resolve().parents[1]


def load_fhir_module():
    module_name = "load_fhir_under_test"
    sys.modules.pop(module_name, None)

    requests_module = types.ModuleType("requests")
    azure_module = types.ModuleType("azure")
    azure_identity_module = types.ModuleType("azure.identity")
    azure_storage_module = types.ModuleType("azure.storage")
    azure_storage_blob_module = types.ModuleType("azure.storage.blob")

    class FakeCredential:
        def __init__(self, *args, **kwargs):
            pass

    class FakeBlobServiceClient:
        def __init__(self, *args, **kwargs):
            pass

    azure_identity_module.ManagedIdentityCredential = FakeCredential
    azure_identity_module.DefaultAzureCredential = FakeCredential
    azure_storage_blob_module.BlobServiceClient = FakeBlobServiceClient

    fake_modules = {
        "requests": requests_module,
        "azure": azure_module,
        "azure.identity": azure_identity_module,
        "azure.storage": azure_storage_module,
        "azure.storage.blob": azure_storage_blob_module,
    }

    original_open = builtins.open

    def fake_open(path, *args, **kwargs):
        if path == "/app/device_registry.json":
            return io.StringIO(json.dumps({"devices": []}))
        if path == "/app/atlanta_providers.json":
            return io.StringIO(json.dumps([]))
        return original_open(path, *args, **kwargs)

    spec = importlib.util.spec_from_file_location(module_name, FHIR_LOADER_DIR / "load_fhir.py")
    if spec is None or spec.loader is None:
        raise AssertionError("could not load load_fhir.py")

    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, fake_modules), patch.object(builtins, "open", fake_open):
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
    return module


class FhirReferenceTransformTests(unittest.TestCase):
    def setUp(self) -> None:
        self.loader = load_fhir_module()
        self.addCleanup(sys.modules.pop, "load_fhir_under_test", None)

    def test_full_url_map_uses_resource_type_and_id_for_transaction_urns(self) -> None:
        bundle = {
            "entry": [
                {
                    "fullUrl": "urn:uuid:patient-123",
                    "resource": {"resourceType": "Patient", "id": "patient-123"},
                },
                {
                    "fullUrl": "urn:uuid:goal-abc",
                    "resource": {"resourceType": "Goal", "id": "goal-abc"},
                },
                {
                    "fullUrl": "urn:uuid:missing-type",
                    "resource": {"id": "missing-type"},
                },
            ]
        }

        self.assertEqual(
            {
                "urn:uuid:patient-123": "Patient/patient-123",
                "urn:uuid:goal-abc": "Goal/goal-abc",
            },
            self.loader.build_full_url_reference_map(bundle),
        )

    def test_reference_transform_converts_nested_transaction_urns_to_typed_fhir_refs(self) -> None:
        resource = {
            "resourceType": "CarePlan",
            "subject": {"reference": "urn:uuid:patient-123"},
            "addresses": [{"reference": "urn:uuid:condition-456"}],
            "goal": [{"reference": "urn:uuid:goal-789"}],
            "contained": [
                {
                    "resourceType": "Observation",
                    "subject": {"reference": "urn:uuid:patient-123"},
                }
            ],
        }
        full_url_ref_map = {
            "urn:uuid:patient-123": "Patient/patient-123",
            "urn:uuid:condition-456": "Condition/condition-456",
            "urn:uuid:goal-789": "Goal/goal-789",
        }

        transformed = self.loader.transform_references_in_resource(resource, full_url_ref_map)

        self.assertEqual("Patient/patient-123", transformed["subject"]["reference"])
        self.assertEqual("Condition/condition-456", transformed["addresses"][0]["reference"])
        self.assertEqual("Goal/goal-789", transformed["goal"][0]["reference"])
        self.assertEqual("Patient/patient-123", transformed["contained"][0]["subject"]["reference"])

    def test_unknown_transaction_urn_is_preserved_instead_of_degraded_to_bare_uuid(self) -> None:
        resource = {"subject": {"reference": "urn:uuid:not-in-this-bundle"}}

        transformed = self.loader.transform_references_in_resource(resource, {})

        self.assertEqual("urn:uuid:not-in-this-bundle", transformed["subject"]["reference"])

    def test_conditional_reference_map_keeps_fhir_resource_type_on_resolved_identifiers(self) -> None:
        bundle = {
            "entry": [
                {
                    "fullUrl": "urn:uuid:practitioner-1",
                    "resource": {
                        "resourceType": "Practitioner",
                        "id": "practitioner-1",
                        "identifier": [
                            {"system": "http://hl7.org/fhir/sid/us-npi", "value": "9999812345"}
                        ],
                    },
                }
            ]
        }

        self.assertEqual(
            {
                "Practitioner?identifier=http://hl7.org/fhir/sid/us-npi|9999812345": "Practitioner/practitioner-1"
            },
            self.loader.build_conditional_reference_map(bundle),
        )

    def test_device_assignments_cover_all_devices_when_patients_are_reused(self) -> None:
        devices = [{"id": f"device-{index}"} for index in range(5)]
        patients = [{"id": "patient-a"}, {"id": "patient-b"}]

        assignments = self.loader.build_device_patient_assignments(devices, patients, 5)

        self.assertEqual([device["id"] for device, _patient in assignments], [f"device-{index}" for index in range(5)])
        self.assertEqual([patient["id"] for _device, patient in assignments], ["patient-a", "patient-b", "patient-a", "patient-b", "patient-a"])


if __name__ == "__main__":
    unittest.main()
