import re as _re
import sys

# Define the exact functions from orchestrator/local_server.py to test the parsing logic
def _normalize_url(raw: str) -> str:
    """Trim common trailing punctuation from captured URLs."""
    return raw.strip().rstrip(",.;)\"]'")

def _extract_deployment_links(message: str) -> dict[str, str]:
    """Extract well-known deployment URLs from log lines."""
    links: dict[str, str] = {}

    report_match = _re.search(r"Report URL:\s*(https?://\S+)", message, flags=_re.IGNORECASE)
    if report_match:
        links["imagingReport"] = _normalize_url(report_match.group(1))

    settings_match = _re.search(r"Settings:\s*(https?://\S+)", message, flags=_re.IGNORECASE)
    if settings_match:
        links["imagingReportSettings"] = _normalize_url(settings_match.group(1))

    viewer_match = _re.search(r"OHIF Viewer(?: \(from Azure\))?\s*:\s*(https?://\S+)", message, flags=_re.IGNORECASE)
    if viewer_match:
        links["ohifViewer"] = _normalize_url(viewer_match.group(1))

    if "azurestaticapps.net" in message.lower() and "ohifViewer" not in links:
        swa_match = _re.search(r"(https?://[^\s]*azurestaticapps\.net\S*)", message, flags=_re.IGNORECASE)
        if swa_match:
            links["ohifViewer"] = _normalize_url(swa_match.group(1))

    return links

def main():
    print("Testing orchestrator regex parsing logic on the new PowerShell output...")
    
    mock_log = """
  --- Step 10b: Deploy Population Health & Quality Dashboard report ---
  Report definition found at: phase-5\\cms-quality-report
  ✓ Population Health & Quality Dashboard artifacts staged for deployment
    (10 pages: Quality Overview, Measure Deep-Dive, Claims Analytics,
     Medication Adherence, Care Gap Closure, Payer Performance,
     Star Rating Simulator, Risk Adjustment & RAF,
     Readmission Risk, Cost & Utilization)

  ╔═══════════════════════════════════════════════════════╗
  ║  ⚠️  ACTION REQUIRED: AUTHORIZE DATA CONNECTION         ║
  ╚═══════════════════════════════════════════════════════╝
  To view the Population Health & Quality Dashboard, you
  MUST authorize the semantic model connection in the portal.
  Click 'Edit credentials' -> OAuth2 to bind your token.

  Settings: https://app.fabric.microsoft.com/groups/8732edc9-7e43-4412-a311-4c30981c775f/settings/datasets/429bede8-09bc-4806-b5db-40a1a79341d2
"""

    links = _extract_deployment_links(mock_log)
    print("\nParsed Links:")
    print(links)
    
    expected_url = "https://app.fabric.microsoft.com/groups/8732edc9-7e43-4412-a311-4c30981c775f/settings/datasets/429bede8-09bc-4806-b5db-40a1a79341d2"
    parsed_url = links.get("imagingReportSettings")
    
    if parsed_url == expected_url:
        print("\n✓ SUCCESS: The regex successfully extracted the correct settings URL from the logs!")
        sys.exit(0)
    else:
        print(f"\n✗ FAILURE: Expected URL: {expected_url}")
        print(f"Captured URL: {parsed_url}")
        sys.exit(1)

if __name__ == "__main__":
    main()
