from __future__ import annotations

import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


DICOM_LOADER_DIR = Path(__file__).resolve().parents[1]


class FakeDataset:
    def __init__(self, modality: str = "CT") -> None:
        self.Modality = modality
        self.PatientID = "original-patient"
        self.PatientName = "Original^Patient"
        self.PatientBirthDate = "19000101"
        self.PatientSex = "O"
        self.StudyInstanceUID = "original-study"
        self.SeriesInstanceUID = "original-series"
        self.SOPInstanceUID = "original-sop"
        self.BodyPartExamined = "ORIGINAL"
        self.StudyDate = "19990101"
        self.saved_as: str | None = None

    def save_as(self, output_path: str) -> None:
        self.saved_as = output_path


def load_module_from_path(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"could not load module spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class DicomRetaggerReportingTests(unittest.TestCase):
    def import_retagger_with_fakes(self, datasets: dict[str, FakeDataset], uids: list[str]):
        pydicom_module = types.ModuleType("pydicom")
        pydicom_module.dcmread = lambda path: datasets[path]
        uid_module = types.ModuleType("pydicom.uid")
        uid_iter = iter(uids)
        uid_module.generate_uid = lambda: next(uid_iter)

        module_name = "dicom_retagger_under_test"
        with patch.dict(
            sys.modules,
            {"pydicom": pydicom_module, "pydicom.uid": uid_module},
        ):
            sys.modules.pop(module_name, None)
            module = load_module_from_path(module_name, DICOM_LOADER_DIR / "dicom_retagger.py")
        self.addCleanup(sys.modules.pop, module_name, None)
        return module

    def patient_info(self) -> dict[str, str]:
        return {
            "idOrig": "patient-123",
            "family": "Garcia",
            "given": "Ana",
            "birthDate": "1978-02-03",
            "gender": "female",
        }

    def test_retag_dicom_file_returns_metadata_from_retagged_dataset(self) -> None:
        dataset = FakeDataset(modality="MR")
        retagger = self.import_retagger_with_fakes(
            {"/input/original.dcm": dataset},
            ["9.9.9.9.1"],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = str(Path(tmpdir) / "instance_0000.dcm")
            study_uid, series_uid, metadata = retagger.retag_dicom_file(
                dcm_path="/input/original.dcm",
                patient_info=self.patient_info(),
                device_id="MASIMO-RADIUS7-0033",
                hospital_name="Emory University Hospital",
                body_part_examined="CHEST",
                output_path=output_path,
                study_uid="1.2.840.study",
                series_uid="1.2.840.series",
            )

        self.assertEqual(study_uid, "1.2.840.study")
        self.assertEqual(series_uid, "1.2.840.series")
        self.assertEqual(dataset.saved_as, output_path)
        self.assertEqual(dataset.PatientID, "patient-123")
        self.assertEqual(str(dataset.PatientName), "Garcia^Ana")
        self.assertEqual(dataset.PatientBirthDate, "19780203")
        self.assertEqual(dataset.PatientSex, "F")
        self.assertEqual(dataset.AccessionNumber, "ACC-MASIMO-RADIUS7-0033")
        self.assertEqual(dataset.InstitutionName, "Emory University Hospital")
        self.assertEqual(dataset.BodyPartExamined, "CHEST")
        self.assertEqual(dataset.StudyInstanceUID, "1.2.840.study")
        self.assertEqual(dataset.SeriesInstanceUID, "1.2.840.series")
        self.assertEqual(dataset.SOPInstanceUID, "9.9.9.9.1")
        self.assertRegex(dataset.StudyDate, r"^\d{8}$")
        self.assertEqual(
            metadata,
            {
                "studyInstanceUid": "1.2.840.study",
                "seriesInstanceUid": "1.2.840.series",
                "sopInstanceUid": "9.9.9.9.1",
                "modality": "MR",
                "bodyPartExamined": "CHEST",
                "studyDate": dataset.StudyDate,
                "patientId": "patient-123",
                "patientName": "Garcia^Ana",
                "patientBirthDate": "19780203",
                "patientSex": "F",
            },
        )

    def test_retag_series_uses_one_study_and_series_with_ordered_instance_uids(self) -> None:
        first = FakeDataset(modality="CT")
        second = FakeDataset(modality="CT")
        retagger = self.import_retagger_with_fakes(
            {"/input/first.dcm": first, "/input/second.dcm": second},
            ["2.25.study", "2.25.series", "2.25.sop.1", "2.25.sop.2"],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            study_uid, series_uid, output_files, metadata = retagger.retag_series(
                dcm_files=["/input/first.dcm", "/input/second.dcm"],
                patient_info=self.patient_info(),
                device_id="MASIMO-RADIUS7-0033",
                hospital_name="Emory University Hospital",
                body_part_examined="CHEST",
                output_dir=tmpdir,
            )

        self.assertEqual(study_uid, "2.25.study")
        self.assertEqual(series_uid, "2.25.series")
        self.assertEqual(
            [Path(path).name for path in output_files],
            ["instance_0000.dcm", "instance_0001.dcm"],
        )
        self.assertEqual(first.StudyInstanceUID, "2.25.study")
        self.assertEqual(second.StudyInstanceUID, "2.25.study")
        self.assertEqual(first.SeriesInstanceUID, "2.25.series")
        self.assertEqual(second.SeriesInstanceUID, "2.25.series")
        self.assertEqual(first.SOPInstanceUID, "2.25.sop.1")
        self.assertEqual(second.SOPInstanceUID, "2.25.sop.2")
        self.assertNotIn("sopInstanceUid", metadata)
        self.assertEqual(metadata["studyInstanceUid"], "2.25.study")
        self.assertEqual(metadata["seriesInstanceUid"], "2.25.series")
        self.assertEqual(metadata["sopInstanceUids"], ["2.25.sop.1", "2.25.sop.2"])
        self.assertEqual(metadata["patientId"], "patient-123")
        self.assertEqual(metadata["bodyPartExamined"], "CHEST")


class ImagingManifestRecordTests(unittest.TestCase):
    def import_load_dicom_with_fakes(self):
        azure_module = types.ModuleType("azure")
        azure_identity_module = types.ModuleType("azure.identity")
        azure_identity_module.ManagedIdentityCredential = object
        azure_identity_module.DefaultAzureCredential = object
        azure_storage_module = types.ModuleType("azure.storage")
        azure_storage_blob_module = types.ModuleType("azure.storage.blob")
        azure_storage_blob_module.BlobServiceClient = object
        httpx_module = types.ModuleType("httpx")
        tcia_client_module = types.ModuleType("tcia_client")
        tcia_client_module.TCIAClient = object
        dicom_retagger_module = types.ModuleType("dicom_retagger")
        dicom_retagger_module.retag_series = lambda *args, **kwargs: None

        module_name = "load_dicom_under_test"
        fakes = {
            "azure": azure_module,
            "azure.identity": azure_identity_module,
            "azure.storage": azure_storage_module,
            "azure.storage.blob": azure_storage_blob_module,
            "httpx": httpx_module,
            "tcia_client": tcia_client_module,
            "dicom_retagger": dicom_retagger_module,
        }
        with patch.dict(sys.modules, fakes):
            sys.modules.pop(module_name, None)
            module = load_module_from_path(module_name, DICOM_LOADER_DIR / "load_dicom.py")
        self.addCleanup(sys.modules.pop, module_name, None)
        return module

    def test_manifest_record_normalizes_study_uid_and_preserves_dicom_provenance(self) -> None:
        load_dicom = self.import_load_dicom_with_fakes()

        record = load_dicom.build_imaging_manifest_record(
            patient_id="runtime-patient",
            device_id="MASIMO-RADIUS7-0033",
            patient_info={"idOrig": "fhir-patient"},
            study_uid="urn:oid:1.2.840.study",
            series_uid="1.2.840.series",
            planned_modality="CT",
            body_site={"code": "39607008", "display": "Chest", "dicom_body_part": "CHEST"},
            instance_count=2,
            blob_base_path="runtime-patient/1.2.840.study/1.2.840.series",
            source_collection="RSNA Pneumonia",
            source_tcia_study_uid="source-study",
            source_tcia_series_uid="source-series",
            dicom_metadata={
                "patientId": "dicom-patient",
                "modality": "CR",
                "bodyPartExamined": "CHEST",
                "studyDate": "20260701",
                "sopInstanceUids": ["sop-1", "sop-2"],
            },
            condition_codings=[{"code": "233604007", "display": "Pneumonia"}],
            fhir_imaging_study_id="imaging-study-123",
        )

        self.assertEqual(
            record,
            {
                "patientId": "runtime-patient",
                "patientFhirId": "fhir-patient",
                "dicomPatientId": "dicom-patient",
                "deviceId": "MASIMO-RADIUS7-0033",
                "studyInstanceUid": "1.2.840.study",
                "studyInstanceUidUrn": "urn:oid:1.2.840.study",
                "seriesInstanceUid": "1.2.840.series",
                "sopInstanceUids": ["sop-1", "sop-2"],
                "plannedModality": "CT",
                "actualModality": "CR",
                "bodyPartExamined": "CHEST",
                "bodySiteCode": "39607008",
                "bodySiteDisplay": "Chest",
                "studyDate": "20260701",
                "instanceCount": 2,
                "blobBasePath": "runtime-patient/1.2.840.study/1.2.840.series",
                "sourceCollection": "RSNA Pneumonia",
                "sourceTciaStudyInstanceUid": "source-study",
                "sourceTciaSeriesInstanceUid": "source-series",
                "conditionCode": "233604007",
                "conditionDisplay": "Pneumonia",
                "fhirImagingStudyId": "imaging-study-123",
            },
        )

    def test_manifest_record_falls_back_to_planned_values_when_dicom_metadata_is_absent(self) -> None:
        load_dicom = self.import_load_dicom_with_fakes()

        record = load_dicom.build_imaging_manifest_record(
            patient_id="runtime-patient",
            device_id="MASIMO-RADIUS7-0033",
            patient_info={"idOrig": "fhir-patient"},
            study_uid="1.2.840.study",
            series_uid="1.2.840.series",
            planned_modality="MR",
            body_site={"code": "69536005", "display": "Head structure", "dicom_body_part": "HEAD"},
            instance_count=1,
            blob_base_path="runtime-patient/1.2.840.study/1.2.840.series",
            source_collection="LIDC-IDRI",
            source_tcia_study_uid="source-study",
            source_tcia_series_uid="source-series",
            dicom_metadata={},
            condition_codings=[],
            fhir_imaging_study_id=None,
        )

        self.assertEqual(record["studyInstanceUid"], "1.2.840.study")
        self.assertEqual(record["studyInstanceUidUrn"], "urn:oid:1.2.840.study")
        self.assertEqual(record["dicomPatientId"], "fhir-patient")
        self.assertEqual(record["sopInstanceUids"], [])
        self.assertEqual(record["actualModality"], "MR")
        self.assertEqual(record["bodyPartExamined"], "HEAD")
        self.assertIsNone(record["conditionCode"])
        self.assertIsNone(record["conditionDisplay"])
        self.assertIsNone(record["fhirImagingStudyId"])


if __name__ == "__main__":
    unittest.main()
