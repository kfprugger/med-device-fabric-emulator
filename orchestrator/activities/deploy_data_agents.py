"""Phase 5: Deploy Data Agents (Patient 360 + Clinical Triage).

Ports logic from phase-2/deploy-data-agents.ps1:
- Discover KQL Database and Silver Lakehouse
- Build datasource configurations with table/schema elements
- Create Patient 360 Data Agent
- Create Clinical Triage Data Agent
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from shared.fabric_client import FabricClient

logger = logging.getLogger(__name__)

# Silver Lakehouse tables (from deploy-data-agents.ps1)
SILVER_TABLES = [
    "Patient", "Condition", "Device", "Location", "Encounter",
    "Basic", "Observation", "MedicationRequest", "Procedure",
    "Immunization", "ImagingStudy",
]

# KQL native tables (not functions or external tables)
KQL_TABLES = ["TelemetryRaw", "AlertHistory"]


def _build_lakehouse_elements() -> list[dict[str, Any]]:
    """Build the lakehouse datasource elements structure."""
    return [
        {
            "display_name": "dbo",
            "type": "lakehouse_tables.schema",
            "is_selected": True,
            "children": [
                {
                    "display_name": table,
                    "type": "lakehouse_tables.table",
                    "is_selected": True,
                }
                for table in SILVER_TABLES
            ],
        }
    ]


def _build_kql_elements() -> list[dict[str, Any]]:
    """Build the KQL datasource elements structure."""
    return [
        {
            "id": str(uuid.uuid4()),
            "display_name": table,
            "type": "kusto.table",
            "is_selected": True,
        }
        for table in KQL_TABLES
    ]


def run(config: dict[str, Any], resources: dict[str, Any]) -> dict[str, Any]:
    """Execute Phase 5: Data Agents.

    Args:
        config: DeploymentConfig as dict.
        resources: Accumulated resources from prior phases.

    Returns:
        Data Agent IDs and names.
    """
    start = time.time()
    fabric = FabricClient(config.get("fabric_api_base", "https://api.fabric.microsoft.com/v1"))
    workspace_id = resources["fabric_workspace_id"]
    kql_db_id = resources.get("kql_db_id", "")
    kql_db_name = resources.get("kql_db_name", "MasimoKQLDB")

    # Discover Silver Lakehouse
    silver_lh_id = resources.get("silver_lakehouse_id", "")
    silver_lh_name = resources.get("silver_lakehouse_name", "")
    if not silver_lh_id:
        silver_lh = fabric.find_lakehouse(workspace_id, "silver")
        if silver_lh:
            silver_lh_id = silver_lh["id"]
            silver_lh_name = silver_lh["displayName"]
        else:
            raise RuntimeError("Silver Lakehouse not found for Data Agent configuration.")

    # Build datasources
    kql_datasource = {
        "type": "KQLDatabase",
        "displayName": kql_db_name,
        "databaseId": kql_db_id,
        "elements": _build_kql_elements(),
        "instructions": (
            "Query TelemetryRaw for real-time Masimo vital signs "
            "(SpO2, pulse_rate, perfusion_index, signal_quality). "
            "Query AlertHistory for clinical alerts with timestamps in EST."
        ),
    }

    lakehouse_datasource = {
        "type": "Lakehouse",
        "displayName": silver_lh_name,
        "lakehouseId": silver_lh_id,
        "elements": _build_lakehouse_elements(),
        "instructions": (
            "Query FHIR clinical tables for patient demographics, "
            "conditions, encounters, observations, medications, "
            "procedures, immunizations, and imaging studies."
        ),
    }

    agent_results = {}

    # Patient 360 Agent
    logger.info("Creating Patient 360 Data Agent…")
    try:
        p360 = fabric.create_data_agent(
            workspace_id=workspace_id,
            display_name="Patient 360",
            description=(
                "Unified patient view combining real-time Masimo vital signs "
                "with FHIR clinical data (conditions, medications, encounters)."
            ),
            instructions=(
                "You are a clinical assistant providing a 360-degree view of patients. "
                "Combine real-time telemetry from KQL with historical FHIR data from the Lakehouse. "
                "Always display timestamps in Eastern Standard Time (EST). "
                "When showing vital signs, include SpO2, pulse_rate, and perfusion_index."
            ),
            datasources=[kql_datasource, lakehouse_datasource],
        )
        agent_results["patient360_id"] = p360["id"] if p360 else ""
        logger.info("Patient 360 agent created: %s", agent_results["patient360_id"])
    except Exception as e:
        logger.error("Failed to create Patient 360 agent: %s", e)
        agent_results["patient360_error"] = str(e)

    # Clinical Triage Agent
    logger.info("Creating Clinical Triage Data Agent…")
    try:
        triage = fabric.create_data_agent(
            workspace_id=workspace_id,
            display_name="Clinical Triage",
            description=(
                "Alert-based patient risk stratification using real-time "
                "telemetry alerts and clinical history."
            ),
            instructions=(
                "You are a clinical triage assistant that prioritizes patients "
                "based on alert severity and clinical context. "
                "Cross-reference AlertHistory with patient conditions and medications. "
                "Always display timestamps in Eastern Standard Time (EST)."
            ),
            datasources=[kql_datasource, lakehouse_datasource],
        )
        agent_results["triage_id"] = triage["id"] if triage else ""
        logger.info("Clinical Triage agent created: %s", agent_results["triage_id"])
    except Exception as e:
        logger.error("Failed to create Clinical Triage agent: %s", e)
        agent_results["triage_error"] = str(e)

    duration = time.time() - start

    return {
        "phase": "Phase 5: Data Agents",
        "duration_seconds": duration,
        "resources": agent_results,
    }
