"""Pure helpers for teardown resource scanning."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def live_fabric_workspaces_for_teardown(
    live_workspaces: Iterable[Mapping[str, Any]],
    previously_deployed_workspace_names: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Return Fabric workspaces that are safe to show as teardown candidates.

    Deployment history is intentionally not merged into this list. History can
    seed orphaned identity lookups and annotate live workspaces, but teardown
    candidates must correspond to workspaces returned by the current live
    Fabric `/workspaces` scan.
    """
    del previously_deployed_workspace_names
    return [dict(workspace) for workspace in live_workspaces]
