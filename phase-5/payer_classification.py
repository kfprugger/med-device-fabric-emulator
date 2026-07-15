"""Pure payer classification helpers shared by tests and data-generation tooling."""
from collections.abc import Mapping
from typing import Any


_CATEGORIES = {
    "medicare": "Medicare",
    "medicaid": "Medicaid",
    "commercial": "Commercial",
    "private": "Commercial",
    "uninsured": "Uninsured",
    "self-pay": "Uninsured",
    "self pay": "Uninsured",
}


def classify_payer_category(coverage: Mapping[str, Any]) -> str:
    """Normalize FHIR Coverage text/coding/payor signals into a payer category."""
    type_obj = coverage.get("type") or {}
    coding = type_obj.get("coding") or []
    signals = []
    if isinstance(type_obj, Mapping):
        signals.extend(str(type_obj.get(key) or "") for key in ("text",))
    for item in coding:
        if isinstance(item, Mapping):
            signals.extend(str(item.get(key) or "") for key in ("code", "display"))
    for payor in coverage.get("payor") or []:
        if isinstance(payor, Mapping):
            signals.append(str(payor.get("display") or ""))
    haystack = " ".join(signals).lower()
    if "medicare" in haystack or " mcr " in f" {haystack} ":
        return "Medicare"
    if "medicaid" in haystack or " mcd " in f" {haystack} ":
        return "Medicaid"
    if any(value in haystack for value in ("self-pay", "self pay", "uninsured", "no insurance")):
        return "Uninsured"
    if any(value in haystack for value in ("commercial", "private", "employer", "blue cross", "bcbs", "aetna", "cigna", "united", "humana", "anthem", "kaiser")):
        return "Commercial"
    return "Unknown"
