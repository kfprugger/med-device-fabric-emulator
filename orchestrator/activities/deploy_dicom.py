"""Phase 1: Load TCIA DICOM studies into ADLS and FHIR ImagingStudy resources.

Ports DICOM logic from phase-1/deploy-fhir.ps1 -RunDicom:
- Build DICOM loader image
- Deploy and run dicom-loader-job.bicep

The active imaging path uses ADLS Gen2 + Fabric HDS shortcuts; it does not deploy
Azure Health Data Services DICOM service.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from shared.azure_client import AzureClient

logger = logging.getLogger(__name__)


def run(config: dict[str, Any], resources: dict[str, Any]) -> dict[str, Any]:
    """Execute Phase 1: DICOM loading.

    Args:
        config: DeploymentConfig as dict.
        resources: Accumulated resources from prior phases.

    Returns:
        DICOM loader details.
    """
    start = time.time()
    client = AzureClient()

    rg_name = config["resource_group_name"]
    tags = config.get("tags", {})
    acr_name = resources.get("acr_name", "")
    fhir_service_url = resources.get("fhir_service_url", "")
    dicom_storage_account = resources.get("fhir_storage_account") or resources.get("storage_account_name", "")
    aci_identity_id = resources.get("aci_identity_id", "")
    aci_identity_client_id = resources.get("aci_identity_client_id", "")
    dicom_image = f"{acr_name}.azurecr.io/dicom-loader:v1" if acr_name else ""

    if not all([acr_name, fhir_service_url, dicom_storage_account, aci_identity_id, aci_identity_client_id]):
        raise RuntimeError(
            "DICOM loader requires ACR, FHIR service URL, ADLS storage account, "
            "and the shared ACI managed identity from fhir-infra."
        )

    # 1. Build DICOM loader image
    if acr_name:
        dicom_loader_context = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "dicom-loader",
        )
        try:
            client.build_container_image(
                resource_group=rg_name,
                acr_name=acr_name,
                image_name="dicom-loader",
                image_tag="v1",
                docker_context_path=dicom_loader_context,
            )
        except Exception as e:
            logger.warning("DICOM Loader image build: %s", e)

    # 2. Deploy and run DICOM loader job
    logger.info("Running DICOM loader (TCIA download + re-tag + ADLS upload)…")
    client.deploy_bicep(
        resource_group=rg_name,
        deployment_name="dicom-loader-job",
        template_file="dicom-loader-job.bicep",
        parameters={
            "acrName": acr_name,
            "imageName": dicom_image,
            "storageAccountName": dicom_storage_account,
            "fhirServiceUrl": fhir_service_url,
            "aciIdentityId": aci_identity_id,
            "aciIdentityClientId": aci_identity_client_id,
        },
        tags=tags,
    )

    loader_result = client.wait_for_aci_job(
        resource_group=rg_name,
        container_group_name="dicom-loader-job",
        timeout_minutes=45,
    )

    logger.info(
        "DICOM Loader completed: %s (exit=%d, %.0fs)",
        loader_result["state"],
        loader_result["exit_code"],
        loader_result["duration_seconds"],
    )

    if loader_result["state"] != "Succeeded" and loader_result["exit_code"] != 0:
        raise RuntimeError(
            f"DICOM Loader failed: {loader_result['state']}, "
            f"exit_code={loader_result['exit_code']}"
        )

    duration = time.time() - start

    return {
        "phase": "Phase 1: DICOM Loader",
        "duration_seconds": duration,
        "resources": {
            "dicom_storage_account": dicom_storage_account,
            "dicom_loader_state": loader_result["state"],
        },
    }
