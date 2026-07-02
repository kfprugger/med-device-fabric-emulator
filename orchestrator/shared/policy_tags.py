"""Deployment tag policy helpers."""

from __future__ import annotations

from collections.abc import Mapping

REQUIRED_POLICY_TAG_KEY = "SecurityControl"
REQUIRED_POLICY_TAG_VALUE = "Ignore"


def normalize_policy_tags(tags: Mapping[str, str] | None) -> dict[str, str]:
    """Return deployment tags with the required Azure Policy bypass tag.

    Caller-provided tags are preserved except SecurityControl, which is forced
    to the value required by the deployment Azure Policy exemption. The
    localhost orchestrator must not launch a run that can recreate the Event Hub
    namespace without this bypass tag.
    """
    normalized = dict(tags or {})
    normalized[REQUIRED_POLICY_TAG_KEY] = REQUIRED_POLICY_TAG_VALUE
    return normalized
