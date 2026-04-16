"""
DICOM Loader — downloads TCIA studies, re-tags with Synthea patients, uploads to ADLS Gen2.

Re-tagged .dcm files land in blob storage ready for import into Microsoft Fabric.

Environment variables:
    FHIR_SERVICE_URL   — Azure FHIR service endpoint
    STORAGE_ACCOUNT    — ADLS Gen2 storage account name
    DICOM_CONTAINER    — Blob container for DICOM output (default: dicom-output)
    AZURE_CLIENT_ID    — User-Assigned Managed Identity client ID
    TCIA_COLLECTION    — TCIA collection name (default: LIDC-IDRI)
    STUDY_COUNT        — Max studies to download (default: 100)
"""
from __future__ import annotations

import os
import sys
import json
import math
import hashlib
import shutil
import logging
import tempfile
from datetime import datetime, timezone

import httpx
from azure.identity import ManagedIdentityCredential, DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from tcia_client import TCIAClient
from dicom_retagger import retag_series

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ── Azure Auth ───────────────────────────────────────────────────────────

class AzureClient:
    """Handles credential + blob/FHIR access."""

    def __init__(self, fhir_url: str, storage_account: str, dicom_container: str,
                 client_id: str | None = None):
        self.fhir_url = fhir_url.rstrip("/")
        self.storage_account = storage_account
        self.dicom_container = dicom_container
        if client_id:
            self.credential = ManagedIdentityCredential(client_id=client_id)
        else:
            self.credential = DefaultAzureCredential()

        # Blob client
        account_url = f"https://{storage_account}.blob.core.windows.net"
        self.blob_service = BlobServiceClient(account_url, credential=self.credential)
        self.container_client = self.blob_service.get_container_client(dicom_container)

    @property
    def fhir_token(self) -> str:
        scope = f"{self.fhir_url}/.default"
        return self.credential.get_token(scope).token

    def upload_blob(self, blob_path: str, file_path: str) -> str:
        """Upload a file to blob storage. Returns the blob URL."""
        blob_client = self.container_client.get_blob_client(blob_path)
        with open(file_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)
        return blob_client.url


# ── FHIR Queries ─────────────────────────────────────────────────────────

def get_device_associations_from_blob(storage_account: str, credential, container: str = "synthea-output") -> list[dict]:
    """
    Read device-association mapping from blob storage (written by FHIR loader).
    Returns list of dicts with patientId, deviceId, patientName.
    """
    try:
        account_url = f"https://{storage_account}.blob.core.windows.net"
        blob_service = BlobServiceClient(account_url, credential=credential)
        blob_client = blob_service.get_blob_client(container, "device-associations.json")
        data = blob_client.download_blob().readall()
        associations = json.loads(data)
        logger.info("Loaded %d device associations from blob (device-associations.json)", len(associations))
        return associations
    except Exception as e:
        logger.info("No device-associations.json in blob storage: %s", e)
        return []


def get_device_associated_patients(fhir_url: str, token: str) -> list[dict]:
    """
    Query FHIR for device-associated patients via Basic resources (fallback).
    Returns list of dicts with patientId and deviceId.
    """
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/fhir+json"}
    patients = []
    url = f"{fhir_url}/Basic?code=http://terminology.hl7.org/CodeSystem/basic-resource-type|device-assoc&_count=200"

    while url:
        resp = httpx.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        bundle = resp.json()

        for entry in bundle.get("entry", []):
            resource = entry.get("resource", {})
            subject_ref = resource.get("subject", {}).get("reference", "")
            if not subject_ref.startswith("Patient/"):
                continue
            patient_id = subject_ref.split("/")[1]

            device_id = None
            for ext in resource.get("extension", []):
                if ext.get("url", "").endswith("associated-device"):
                    dev_ref = ext.get("valueReference", {}).get("reference", "")
                    if "/" in dev_ref:
                        device_id = dev_ref.split("/")[1]

            if patient_id and device_id:
                patients.append({"patientId": patient_id, "deviceId": device_id})

        url = None
        for link in bundle.get("link", []):
            if link.get("relation") == "next":
                url = link.get("url")
                break

    logger.info("Found %d device-associated patients", len(patients))
    return patients


def get_patient_info(fhir_url: str, token: str, patient_id: str) -> dict:
    """Fetch patient demographics from FHIR."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/fhir+json"}
    resp = httpx.get(f"{fhir_url}/Patient/{patient_id}", headers=headers, timeout=30)
    resp.raise_for_status()
    pt = resp.json()

    names = pt.get("name", [{}])
    name = names[0] if names else {}
    given_names = name.get("given", [])

    return {
        "idOrig": pt.get("id", patient_id),
        "family": name.get("family", "Unknown"),
        "given": given_names[0] if given_names else "Unknown",
        "birthDate": pt.get("birthDate", ""),
        "gender": pt.get("gender", "unknown"),
    }


def get_patient_conditions(fhir_url: str, token: str, patient_id: str) -> list[dict]:
    """Get SNOMED condition codings for a patient (code + display)."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/fhir+json"}
    resp = httpx.get(
        f"{fhir_url}/Condition",
        params={"subject": f"Patient/{patient_id}", "_count": "100"},
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    bundle = resp.json()

    codings: list[dict] = []
    for entry in bundle.get("entry", []):
        condition = entry.get("resource", {})
        for coding in condition.get("code", {}).get("coding", []):
            if "snomed" in coding.get("system", "").lower():
                codings.append({
                    "code": coding.get("code", ""),
                    "display": coding.get("display", ""),
                })
    return codings


def determine_collection(condition_codings: list[dict], modality_map: dict) -> tuple[str, str]:
    """Match patient conditions to a TCIA collection. Returns (collection, modality)."""
    snomed_codes = {c.get("code", "") for c in condition_codings}
    for mapping in modality_map["mappings"]:
        if mapping["snomed"] in snomed_codes:
            return mapping["collection"], mapping["modality"]
    return modality_map["default"]["collection"], modality_map["default"]["modality"]


def infer_body_site(condition_codings: list[dict]) -> dict:
    """
    Infer ImagingStudy bodySite from diagnoses.
    Returns {code, display, text, dicom_body_part}.
    """
    display_text = " ".join((c.get("display") or "") for c in condition_codings).lower()

    # Respiratory and cardiopulmonary diagnoses map to chest imaging.
    if any(k in display_text for k in ["copd", "asthma", "pneumonia", "respiratory", "covid", "lung", "heart", "coronary", "myocardial", "hypertension"]):
        return {
            "code": "39607008",
            "display": "Chest",
            "text": "Chest",
            "dicom_body_part": "CHEST",
        }

    # Sleep/neurologic diagnoses map to head imaging.
    if any(k in display_text for k in ["sleep", "insomnia", "migraine", "headache", "stroke", "neuro", "seizure", "head"]):
        return {
            "code": "69536005",
            "display": "Head structure",
            "text": "Head",
            "dicom_body_part": "HEAD",
        }

    if any(k in display_text for k in ["diabetes", "obesity", "abdominal", "gastro", "liver", "kidney", "renal", "pancrea"]):
        return {
            "code": "113345001",
            "display": "Abdominal structure",
            "text": "Abdomen",
            "dicom_body_part": "ABDOMEN",
        }

    # Fallback to chest for broad clinical usefulness.
    return {
        "code": "39607008",
        "display": "Chest",
        "text": "Chest",
        "dicom_body_part": "CHEST",
    }


# ── FHIR ImagingStudy Creation ───────────────────────────────────────────

def create_imaging_study(
    fhir_url: str,
    token: str,
    patient_id: str,
    study_uid: str,
    series_uid: str,
    modality: str,
    body_site: dict,
    instance_count: int,
    blob_base_path: str,
) -> str:
    """Create a FHIR ImagingStudy resource referencing the blob storage path."""
    resource = {
        "resourceType": "ImagingStudy",
        "status": "available",
        "subject": {"reference": f"Patient/{patient_id}"},
        "started": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "numberOfSeries": 1,
        "numberOfInstances": instance_count,
        "description": f"TCIA re-tagged study — {instance_count} instances stored in ADLS Gen2",
        "bodySite": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": body_site["code"],
                    "display": body_site["display"],
                }
            ],
            "text": body_site["text"],
        },
        "series": [
            {
                "uid": series_uid,
                "modality": {
                    "system": "http://dicom.nema.org/resources/ontology/DCM",
                    "code": modality,
                },
                "numberOfInstances": instance_count,
            }
        ],
        "identifier": [
            {
                "system": "urn:dicom:uid",
                "value": f"urn:oid:{study_uid}",
            }
        ],
        "note": [
            {
                "text": f"DICOM files stored at: {blob_base_path}"
            }
        ],
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/fhir+json",
        "Accept": "application/fhir+json",
    }

    resp = httpx.post(f"{fhir_url}/ImagingStudy", json=resource, headers=headers, timeout=30)
    resp.raise_for_status()
    result = resp.json()
    return result.get("id", "unknown")


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    fhir_url = os.environ.get("FHIR_SERVICE_URL", "")
    storage_account = os.environ.get("STORAGE_ACCOUNT", "")
    dicom_container = os.environ.get("DICOM_CONTAINER", "dicom-output")
    client_id = os.environ.get("AZURE_CLIENT_ID")
    collection = os.environ.get("TCIA_COLLECTION", "LIDC-IDRI")
    study_count = int(os.environ.get("STUDY_COUNT", "100"))

    if not fhir_url or not storage_account:
        logger.error("FHIR_SERVICE_URL and STORAGE_ACCOUNT must be set")
        sys.exit(1)

    logger.info("DICOM Loader starting")
    logger.info("  FHIR URL:        %s", fhir_url)
    logger.info("  Storage Account: %s", storage_account)
    logger.info("  DICOM Container: %s", dicom_container)
    logger.info("  Collection:      %s", collection)
    logger.info("  Max studies:     %d", study_count)

    # Load condition → modality mapping
    map_path = os.path.join(os.path.dirname(__file__), "condition_modality_map.json")
    with open(map_path) as f:
        modality_map = json.load(f)

    # Load atlanta_providers for hospital names
    providers_path = os.path.join(os.path.dirname(__file__), "..", "fhir-loader", "atlanta_providers.json")
    if os.path.exists(providers_path):
        with open(providers_path) as f:
            providers = json.load(f)
        hospital_names = [p["name"] for p in providers.get("organizations", [])]
    else:
        hospital_names = ["Emory University Hospital"]
    logger.info("  Hospitals: %d available", len(hospital_names))

    # Authenticate
    azure = AzureClient(fhir_url, storage_account, dicom_container, client_id)
    logger.info("Azure authentication configured")

    # Ensure container exists
    try:
        azure.container_client.get_container_properties()
    except Exception:
        logger.info("Creating blob container '%s'...", dicom_container)
        azure.blob_service.create_container(dicom_container)

    # Step 1: Get device-associated patients (blob first, FHIR search fallback)
    # Blob is strongly consistent — no search indexing delay
    associations = get_device_associations_from_blob(storage_account, azure.credential)
    
    if not associations:
        # Fallback: query FHIR directly (may hit search indexing delays)
        logger.info("Falling back to FHIR search for device associations...")
        fhir_token = azure.fhir_token
        for attempt in range(30):  # Retry up to 5 minutes
            associations = get_device_associated_patients(fhir_url, fhir_token)
            if associations:
                break
            if attempt < 29:
                logger.info("Waiting for FHIR search index to be consistent... (%ds)", (attempt + 1) * 10)
                import time
                time.sleep(10)
                fhir_token = azure.fhir_token

    if not associations:
        logger.error("No device-associated patients found. Run the FHIR loader first.")
        sys.exit(1)

    associations = associations[:study_count]
    logger.info("Processing %d patients", len(associations))

    # Step 2: Get available TCIA studies
    tcia = TCIAClient()
    studies_by_collection: dict[str, list[dict]] = {}

    def get_studies_for_collection(coll_name: str) -> list[dict]:
        if coll_name not in studies_by_collection:
            logger.info("Fetching TCIA studies for collection '%s'...", coll_name)
            studies_by_collection[coll_name] = tcia.get_studies(coll_name)
            logger.info("  %d studies available in %s", len(studies_by_collection[coll_name]), coll_name)
        return studies_by_collection[coll_name]

    default_studies = get_studies_for_collection(collection)
    if not default_studies:
        logger.error("No studies found in TCIA collection '%s'", collection)
        sys.exit(1)

    # Ensure at least 10%% of assigned studies are chest X-rays.
    chest_target_count = max(1, math.ceil(len(associations) * 0.10))
    sorted_assoc = sorted(
        associations,
        key=lambda a: hashlib.sha256(f"{a.get('patientId','')}|{a.get('deviceId','')}".encode("utf-8")).hexdigest(),
    )
    forced_chest_patient_ids = {a["patientId"] for a in sorted_assoc[:chest_target_count]}
    logger.info("Forcing chest X-ray assignment for %d/%d patients (>=10%%)", chest_target_count, len(associations))

    fhir_token = azure.fhir_token

    # Step 3: Process each patient
    stats = {"uploaded": 0, "failed": 0, "imaging_studies_created": 0, "blobs_written": 0}
    tmp_base = tempfile.mkdtemp(prefix="dicom_loader_")
    fhir_token = azure.fhir_token

    try:
        for idx, assoc in enumerate(associations):
            patient_id = assoc["patientId"]
            device_id = assoc["deviceId"]
            hospital = hospital_names[idx % len(hospital_names)]

            logger.info("[%d/%d] Patient %s (device %s)",
                        idx + 1, len(associations), patient_id, device_id)

            try:
                # Refresh token periodically
                if idx % 20 == 0 and idx > 0:
                    fhir_token = azure.fhir_token

                # Get patient info and conditions
                patient_info = get_patient_info(fhir_url, fhir_token, patient_id)
                condition_codings = get_patient_conditions(fhir_url, fhir_token, patient_id)
                target_collection, modality = determine_collection(condition_codings, modality_map)
                body_site = infer_body_site(condition_codings)

                if patient_id in forced_chest_patient_ids:
                    target_collection = "RSNA Pneumonia"
                    modality = "CR"
                    body_site = {
                        "code": "39607008",
                        "display": "Chest",
                        "text": "Chest",
                        "dicom_body_part": "CHEST",
                    }

                # Pick a TCIA study (cycle through available studies)
                candidate_studies = get_studies_for_collection(target_collection)
                if not candidate_studies:
                    logger.warning("  No studies found in %s; falling back to %s", target_collection, collection)
                    candidate_studies = default_studies
                tcia_study = candidate_studies[idx % len(candidate_studies)]
                study_uid_tcia = tcia_study["StudyInstanceUID"]

                # Get series for the study (pick first series)
                series_list = tcia.get_series_for_study(study_uid_tcia)
                if not series_list:
                    logger.warning("  No series found for TCIA study %s — skipping", study_uid_tcia)
                    stats["failed"] += 1
                    continue

                series_uid_tcia = series_list[0]["SeriesInstanceUID"]

                # Download DICOM files
                download_dir = os.path.join(tmp_base, f"download_{idx}")
                logger.info("  Downloading series %s...", series_uid_tcia)
                dcm_files = tcia.download_series(series_uid_tcia, download_dir)
                logger.info("  Downloaded %d DICOM files", len(dcm_files))

                if not dcm_files:
                    logger.warning("  No DICOM files in series — skipping")
                    stats["failed"] += 1
                    continue

                # Re-tag with Synthea patient identifiers
                retag_dir = os.path.join(tmp_base, f"retag_{idx}")
                study_uid, series_uid, retagged_files = retag_series(
                    dcm_files=dcm_files,
                    patient_info=patient_info,
                    device_id=device_id,
                    hospital_name=hospital,
                    body_part_examined=body_site["dicom_body_part"],
                    output_dir=retag_dir,
                )

                # Upload re-tagged .dcm files to ADLS Gen2
                # Path: {patientId}/{studyUID}/{seriesUID}/{instance}.dcm
                blob_base = f"{patient_id}/{study_uid}/{series_uid}"
                logger.info("  Uploading %d .dcm files to blob: %s/...", len(retagged_files), blob_base)
                for dcm_file in retagged_files:
                    fname = os.path.basename(dcm_file)
                    blob_path = f"{blob_base}/{fname}"
                    azure.upload_blob(blob_path, dcm_file)
                    stats["blobs_written"] += 1

                stats["uploaded"] += 1
                logger.info("  Uploaded study %s (%d files)", study_uid, len(retagged_files))

                # Create FHIR ImagingStudy resource
                fhir_token = azure.fhir_token
                img_study_id = create_imaging_study(
                    fhir_url, fhir_token, patient_id,
                    study_uid, series_uid, modality,
                    body_site,
                    len(retagged_files), blob_base,
                )
                stats["imaging_studies_created"] += 1
                logger.info("  Created ImagingStudy/%s", img_study_id)

                # Clean up temp files for this patient
                shutil.rmtree(download_dir, ignore_errors=True)
                shutil.rmtree(retag_dir, ignore_errors=True)

            except Exception:
                logger.exception("  Error processing patient %s", patient_id)
                stats["failed"] += 1

    finally:
        shutil.rmtree(tmp_base, ignore_errors=True)

    # Summary
    logger.info("=" * 60)
    logger.info("DICOM LOADER COMPLETE")
    logger.info("  Studies uploaded:          %d", stats["uploaded"])
    logger.info("  DICOM blobs written:       %d", stats["blobs_written"])
    logger.info("  ImagingStudy resources:    %d", stats["imaging_studies_created"])
    logger.info("  Failed:                    %d", stats["failed"])
    logger.info("  Total patients processed:  %d", len(associations))
    logger.info("  Blob path: %s/%s/{patientId}/{studyUID}/...", storage_account, dicom_container)
    logger.info("=" * 60)

    if stats["failed"] > 0 and stats["uploaded"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
