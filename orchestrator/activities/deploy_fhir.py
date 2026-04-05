"""Phase 2: Deploy FHIR Service + Synthea patient generation + FHIR data load.

Ports logic from deploy-fhir.ps1:
- Deploy fhir-infra.bicep (FHIR workspace, FHIR service, storage, MI)
- Build and run Synthea container (synthetic patient generation)
- Build and run FHIR Loader container (upload to FHIR Service)
- Create device associations
"""

from __future__ import annotations

import logging
import time
from typing import Any

from shared.azure_client import AzureClient

logger = logging.getLogger(__name__)


def run(config: dict[str, Any], resources: dict[str, Any]) -> dict[str, Any]:
    """Execute Phase 2: FHIR Service & Data Loading.

    Args:
        config: DeploymentConfig as dict.
        resources: Accumulated resources from prior phases.

    Returns:
        FHIR service details and data loading results.
    """
    start = time.time()
    client = AzureClient()

    rg_name = config["resource_group_name"]
    location = config["location"]
    tags = config.get("tags", {})
    patient_count = config.get("patient_count", 100)
    acr_name = resources.get("acr_name", "")
    admin_group = config.get("admin_security_group", "")

    admin_group_id = ""
    if admin_group:
        try:
            admin_group_id = client.resolve_security_group_id(admin_group)
        except Exception:
            pass

    # 1. Deploy FHIR infrastructure
    logger.info("Deploying FHIR infrastructure…")
    fhir_outputs = client.deploy_bicep(
        resource_group=rg_name,
        deployment_name="fhir-infra",
        template_file="fhir-infra.bicep",
        parameters={
            "adminGroupObjectId": admin_group_id,
        },
        tags=tags,
    )

    fhir_service_url = fhir_outputs.get("fhirServiceUrl", "")
    fhir_storage_account = fhir_outputs.get("storageAccountName", "")
    fhir_mi_id = fhir_outputs.get("managedIdentityId", "")

    logger.info("FHIR Service URL: %s", fhir_service_url)

    # 2. Build Synthea container image
    if acr_name:
        import os

        synthea_context = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "synthea",
        )
        try:
            client.build_container_image(
                resource_group=rg_name,
                acr_name=acr_name,
                image_name="synthea-generator",
                image_tag="v1",
                docker_context_path=synthea_context,
            )
        except Exception as e:
            logger.warning("Synthea image build: %s", e)

    # 3. Deploy and run Synthea job
    logger.info("Running Synthea patient generator (%d patients)…", patient_count)
    synthea_outputs = client.deploy_bicep(
        resource_group=rg_name,
        deployment_name="synthea-job",
        template_file="synthea-job.bicep",
        parameters={
            "acrName": acr_name,
            "storageAccountName": fhir_storage_account,
            "patientCount": patient_count,
        },
        tags=tags,
    )

    # Wait for Synthea container to complete
    synthea_result = client.wait_for_aci_job(
        resource_group=rg_name,
        container_group_name="synthea-generator-job",
        timeout_minutes=45,
    )
    logger.info(
        "Synthea completed: %s (exit=%d, %.0fs)",
        synthea_result["state"],
        synthea_result["exit_code"],
        synthea_result["duration_seconds"],
    )

    if synthea_result["state"] != "Succeeded" and synthea_result["exit_code"] != 0:
        raise RuntimeError(
            f"Synthea failed: {synthea_result['state']}, "
            f"exit_code={synthea_result['exit_code']}"
        )

    # 4. Build FHIR Loader image
    if acr_name:
        fhir_loader_context = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "fhir-loader",
        )
        try:
            client.build_container_image(
                resource_group=rg_name,
                acr_name=acr_name,
                image_name="fhir-loader",
                image_tag="v1",
                docker_context_path=fhir_loader_context,
            )
        except Exception as e:
            logger.warning("FHIR Loader image build: %s", e)

    # 5. Deploy and run FHIR Loader job
    logger.info("Running FHIR Loader…")
    client.deploy_bicep(
        resource_group=rg_name,
        deployment_name="fhir-loader-job",
        template_file="fhir-loader-job.bicep",
        parameters={
            "acrName": acr_name,
            "storageAccountName": fhir_storage_account,
            "fhirServiceUrl": fhir_service_url,
        },
        tags=tags,
    )

    loader_result = client.wait_for_aci_job(
        resource_group=rg_name,
        container_group_name="fhir-loader-job",
        timeout_minutes=60,
    )
    logger.info(
        "FHIR Loader completed: %s (exit=%d, %.0fs)",
        loader_result["state"],
        loader_result["exit_code"],
        loader_result["duration_seconds"],
    )

    if loader_result["state"] != "Succeeded" and loader_result["exit_code"] != 0:
        raise RuntimeError(
            f"FHIR Loader failed: {loader_result['state']}, "
            f"exit_code={loader_result['exit_code']}"
        )

    duration = time.time() - start

    return {
        "phase": "Phase 2: FHIR Service & Data Loading",
        "duration_seconds": duration,
        "resources": {
            "fhir_service_url": fhir_service_url,
            "fhir_storage_account": fhir_storage_account,
            "fhir_managed_identity_id": fhir_mi_id,
            "synthea_state": synthea_result["state"],
            "synthea_duration": synthea_result["duration_seconds"],
            "loader_state": loader_result["state"],
            "loader_duration": loader_result["duration_seconds"],
        },
    }
