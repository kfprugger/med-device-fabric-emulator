"""DICOM re-tagging logic — overwrites patient demographics and UIDs on downloaded TCIA files."""
from __future__ import annotations

import os
import logging
import pydicom
from pydicom.uid import generate_uid
from datetime import datetime

logger = logging.getLogger(__name__)

# FHIR gender → DICOM PatientSex mapping
GENDER_MAP = {"male": "M", "female": "F", "other": "O", "unknown": "O"}


def retag_dicom_file(
    dcm_path: str,
    patient_info: dict,
    device_id: str,
    hospital_name: str,
    body_part_examined: str,
    output_path: str,
    study_uid: str | None = None,
    series_uid: str | None = None,
) -> tuple[str, str, dict]:
    """
    Re-tag a single DICOM file with Synthea patient identifiers.

    Args:
        dcm_path: Path to original TCIA DICOM file.
        patient_info: Dict with keys: idOrig, family, given, birthDate, gender.
        device_id: e.g. "MASIMO-RADIUS7-0033" (for accession number).
        hospital_name: e.g. "Emory University Hospital".
        output_path: Where to save the re-tagged file.
        study_uid: Shared across all files in same study (generate once per patient).
        series_uid: Shared across all files in same series (generate once per series).

        (study_uid, series_uid, metadata) — the UIDs used and selected DICOM tags after re-tagging.
    """
    ds = pydicom.dcmread(dcm_path)

    # Patient demographics
    ds.PatientID = patient_info["idOrig"]
    ds.PatientName = f"{patient_info['family']}^{patient_info['given']}"
    birth = patient_info.get("birthDate", "")
    ds.PatientBirthDate = birth.replace("-", "") if birth else ""
    ds.PatientSex = GENDER_MAP.get(patient_info.get("gender", "").lower(), "O")

    # Study/Series/Instance UIDs — always generate new (never reuse TCIA originals)
    ds.StudyInstanceUID = study_uid or generate_uid()
    ds.SeriesInstanceUID = series_uid or generate_uid()
    ds.SOPInstanceUID = generate_uid()  # unique per instance

    # Institutional context
    ds.AccessionNumber = f"ACC-{device_id}"
    ds.InstitutionName = hospital_name
    ds.StudyDate = datetime.now().strftime("%Y%m%d")

    # Body part context aligns DICOM metadata with ImagingStudy.bodySite.
    ds.BodyPartExamined = body_part_examined

    # DO NOT modify: Modality, SOPClassUID, TransferSyntaxUID, PixelData,
    # Rows, Columns, BitsAllocated, WindowCenter, WindowWidth, etc.

    metadata = {
        "studyInstanceUid": str(ds.StudyInstanceUID),
        "seriesInstanceUid": str(ds.SeriesInstanceUID),
        "sopInstanceUid": str(ds.SOPInstanceUID),
        "modality": str(getattr(ds, "Modality", "") or ""),
        "bodyPartExamined": str(getattr(ds, "BodyPartExamined", "") or ""),
        "studyDate": str(getattr(ds, "StudyDate", "") or ""),
        "patientId": str(getattr(ds, "PatientID", "") or ""),
        "patientName": str(getattr(ds, "PatientName", "") or ""),
        "patientBirthDate": str(getattr(ds, "PatientBirthDate", "") or ""),
        "patientSex": str(getattr(ds, "PatientSex", "") or ""),
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    ds.save_as(output_path)

    return ds.StudyInstanceUID, ds.SeriesInstanceUID, metadata


def retag_series(
    dcm_files: list[str],
    patient_info: dict,
    device_id: str,
    hospital_name: str,
    body_part_examined: str,
    output_dir: str,
) -> tuple[str, str, list[str], dict]:
    """
    Re-tag all DICOM files in a series for one patient.

        (study_uid, series_uid, list of output file paths, metadata)
    """
    study_uid = generate_uid()
    series_uid = generate_uid()
    output_files = []
    sop_instance_uids = []
    series_metadata = {}
    for i, src in enumerate(dcm_files):
        dst = os.path.join(output_dir, f"instance_{i:04d}.dcm")
        _, _, instance_metadata = retag_dicom_file(
            dcm_path=src,
            patient_info=patient_info,
            device_id=device_id,
            hospital_name=hospital_name,
            body_part_examined=body_part_examined,
            output_path=dst,
            study_uid=study_uid,
            series_uid=series_uid,
        )
        if not series_metadata:
            series_metadata = {k: v for k, v in instance_metadata.items() if k != "sopInstanceUid"}
        sop_instance_uids.append(instance_metadata["sopInstanceUid"])
        output_files.append(dst)

    series_metadata["sopInstanceUids"] = sop_instance_uids
    logger.info("Re-tagged %d instances for patient %s (study %s)",
                len(output_files), patient_info["idOrig"], study_uid)
    return study_uid, series_uid, output_files, series_metadata
