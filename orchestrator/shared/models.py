"""Pydantic models for deployment configuration and state."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class PhaseStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    WAITING_FOR_INPUT = "waiting_for_input"
    CANCELLED = "cancelled"


class DeploymentConfig(BaseModel):
    """All parameters needed to run a full deployment."""

    # Azure
    resource_group_name: str = "rg-medtech-rti-fhir"
    location: str = "eastus"
    admin_security_group: str = "sg-azure-admins"
    tags: dict[str, str] = Field(default_factory=dict)

    # FHIR / Synthea
    patient_count: int = 100

    # Fabric
    fabric_workspace_name: str
    fabric_api_base: str = "https://api.fabric.microsoft.com/v1"

    # Phase control
    skip_base_infra: bool = False
    skip_fhir: bool = False
    skip_dicom: bool = False
    skip_fabric: bool = False
    phase2_only: bool = False
    phase3_only: bool = False
    rebuild_containers: bool = False

    # Phase 3 / 4
    dicom_toolkit_path: str = ""
    alert_email: str = ""
    alert_tier_threshold: str = "URGENT"
    alert_cooldown_minutes: int = 15

    # Optional overrides (auto-discovered if blank)
    silver_lakehouse_id: str = ""
    silver_lakehouse_name: str = ""
    kusto_uri: str = ""
    event_hub_namespace: str = ""
    event_hub_name: str = "telemetry-stream"
    fhir_service_url: str = ""


class StepResult(BaseModel):
    """Result of a single deployment step."""

    name: str
    success: bool
    duration_seconds: float = 0.0
    detail: str = ""
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class PhaseResult(BaseModel):
    """Result of a deployment phase (group of steps)."""

    phase: str
    status: PhaseStatus = PhaseStatus.PENDING
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    resources: dict[str, str] = Field(default_factory=dict)
    steps: list[StepResult] = Field(default_factory=list)
    duration_seconds: float = 0.0
    error: str | None = None


class DeploymentState(BaseModel):
    """Full deployment state — stored in SQLite DB (JSON fallback in state-tracking/)."""

    instance_id: str = ""
    config: DeploymentConfig | None = None
    status: PhaseStatus = PhaseStatus.PENDING
    phases: list[PhaseResult] = Field(default_factory=list)
    started_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    completed_at: str | None = None

    # Accumulated resource IDs across phases
    resources: dict[str, Any] = Field(default_factory=dict)
