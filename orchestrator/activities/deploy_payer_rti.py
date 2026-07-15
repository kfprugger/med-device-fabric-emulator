"""Activity: deploy Phase 7 payer RTI and operations resources."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from activities.invoke_powershell import (
    _ps_hashtable_literal,
    _ps_single_quoted,
    _run_powershell,
)

SCRIPT = Path(__file__).resolve().parents[2] / "phase-7" / "deploy-payer-rti.ps1"


def run(config: dict[str, Any], resources: dict[str, Any]) -> dict[str, Any]:
    """Invoke the existing Phase 7 script with the orchestrator config."""
    start = time.time()
    params = [
        f"-FabricWorkspaceName {_ps_single_quoted(config['fabric_workspace_name'])}",
        f"-ResourceGroupName {_ps_single_quoted(config.get('resource_group_name', 'rg-medtech-rti-fhir'))}",
        f"-Location {_ps_single_quoted(config.get('location', 'eastus'))}",
        f"-ExpectedTenantId {_ps_single_quoted(config.get('expected_tenant_id', '8d038e6a-9b7d-4cb8-bbcf-e84dff156478'))}",
        f"-ExpectedSubscriptionId {_ps_single_quoted(config.get('expected_subscription_id', '9bbee190-dc61-4c58-ab47-1275cb04018f'))}",
        f"-EventHubNamespace {_ps_single_quoted(config.get('event_hub_namespace') or resources.get('event_hub_namespace', ''))}",
        f"-FabricApiBase {_ps_single_quoted(config.get('fabric_api_base', 'https://api.fabric.microsoft.com/v1'))}",
        f"-PayerOpsEmail {_ps_single_quoted(config.get('payer_ops_email', ''))}",
        f"-ClaimEventRatePerMinute {int(config.get('claim_event_rate_per_minute', 60))}",
        f"-Tags {_ps_hashtable_literal(config.get('tags', {}))}",
    ]
    for key, switch in (
        ("skip_payer_rti", "SkipPayerRti"),
        ("skip_payer_activator", "SkipPayerActivator"),
        ("skip_ops_agent", "SkipOpsAgent"),
        ("skip_graph_agent", "SkipGraphAgent"),
    ):
        if config.get(key):
            params.append(f"-{switch}")

    command = f"& {_ps_single_quoted(str(SCRIPT))} {' '.join(params)}"
    exit_code = _run_powershell(
        ["pwsh", "-NoProfile", "-NonInteractive", "-Command", command]
    )
    if exit_code != 0:
        raise RuntimeError(f"deploy-payer-rti.ps1 exited with code {exit_code}")

    return {
        "phase": "Phase 7: Payer RTI & Ops",
        "duration_seconds": time.time() - start,
        "exit_code": exit_code,
        "resources": {"payer_rti": "deployed"},
    }
