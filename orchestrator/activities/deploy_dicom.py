"""Phase 2b: Deploy DICOM infrastructure and load imaging studies.

Ports DICOM logic from deploy-fhir.ps1 -RunDicom:
- Deploy dicom-infra.bicep
- Build DICOM loader image
- Deploy and run dicom-loader-job.bicep
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from shared.azure_client import AzureClient

logger = logging.getLogger(__name__)


def run(config: dict[str, Any], resources: dict[str, Any]) -> dict[str, Any]:
    """Execute Phase 2b: DICOM Infrastructure & Loading.

    Args:
        config: DeploymentConfig as dict.
        resources: Accumulated resources from prior phases.

    Returns:
        DICOM service details.
    """
    start = time.time()
    client = AzureClient()

    rg_name = config["resource_group_name"]
    location = config["location"]
    tags = config.get("tags", {})
    acr_name = resources.get("acr_name", "")

    # 1. Deploy DICOM infrastructure
    logger.info("Deploying DICOM infrastructure…")
    dicom_outputs = client.deploy_bicep(
        resource_group=rg_name,
        deployment_name="dicom-infra",
        template_file="dicom-infra.bicep",
        parameters={},
        tags=tags,
    )

    dicom_service_url = dicom_outputs.get("dicomServiceUrl", "")
    dicom_storage_account = dicom_outputs.get("storageAccountName", "")

    # 2. Build DICOM loader image
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

    # 3. Deploy and run DICOM loader job
    logger.info("Running DICOM loader (TCIA download + re-tag + upload)…")
    client.deploy_bicep(
        resource_group=rg_name,
        deployment_name="dicom-loader-job",
        template_file="dicom-loader-job.bicep",
        parameters={
            "acrName": acr_name,
            "storageAccountName": dicom_storage_account,
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
        "phase": "Phase 2b: DICOM Infrastructure & Loading",
        "duration_seconds": duration,
        "resources": {
            "dicom_service_url": dicom_service_url,
            "dicom_storage_account": dicom_storage_account,
            "dicom_loader_state": loader_result["state"],
        },
    }
