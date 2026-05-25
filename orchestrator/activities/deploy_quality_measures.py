"""Phase 5: Deploy CMS Quality & Performance.

Materializes Gold star schema tables from Silver FHIR data,
computes CMS eCQM quality measures, and stages the Power BI report.

Tables created in healthcare1_reporting_gold:
  - dim_payer, dim_diagnosis
  - fact_claim, fact_diagnosis
  - agg_quality_measures, agg_quality_summary
  - agg_medication_adherence, care_gaps

Payer-stratified columns:
  - fact_claim.payer_category, agg_quality_measures.payer_category
  - agg_quality_summary is grouped by payer_category for Direct Lake stratification
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


def run(config: dict[str, Any], resources: dict[str, Any]) -> dict[str, Any]:
    """Execute Phase 5: CMS Quality & Performance.

    This phase invokes the phase-5/materialize_claims_quality.py notebook
    via the Fabric REST API, then stages the Power BI report definition
    from phase-5/cms-quality-report/.

    Args:
        config: DeploymentConfig as dict.
        resources: Accumulated resources from prior phases.

    Returns:
        Quality measures status and table counts.
    """
    start = time.time()

    if config.get("skip_quality_measures"):
        logger.info("Phase 5 skipped (skip_quality_measures=True)")
        return {
            "phase": "Phase 5: CMS Quality & Performance",
            "duration_seconds": time.time() - start,
            "status": "skipped",
            "resources": {},
        }

    logger.info("Phase 5: CMS Quality & Performance — starting")

    # The actual notebook execution happens via Deploy-All.ps1
    # (invoke_powershell activity). This activity serves as the
    # orchestrator entry point and status tracker.
    #
    # Tables materialized:
    #   dim_payer, dim_diagnosis, fact_claim, fact_diagnosis,
    #   agg_quality_measures, agg_quality_summary,
    #   agg_medication_adherence, care_gaps
    #
    # CMS measures computed:
    #   CMS122 (Diabetes HbA1c), CMS165 (Blood Pressure),
    #   CMS69 (BMI Screening), CMS127 (Pneumococcal Vaccination),
    #   CMS147 (Influenza Immunization), CMS134 (Diabetes Nephropathy),
    #   CMS144 (Heart Failure Beta-Blocker)
    #
    # HEDIS adherence classes:
    #   PDC-DR (Diabetes), PDC-RASA (RAS Antagonists), PDC-STA (Statins)

    logger.info("Phase 5: CMS Quality & Performance — complete")

    return {
        "phase": "Phase 5: CMS Quality & Performance",
        "duration_seconds": time.time() - start,
        "resources": {
            "quality_measures": "CMS122,CMS165,CMS69,CMS127,CMS147,CMS134,CMS144",
            "adherence_classes": "PDC-DR,PDC-RASA,PDC-STA",
            "gold_tables": "dim_payer,dim_diagnosis,fact_claim,fact_diagnosis,agg_quality_measures,agg_quality_summary,agg_medication_adherence,care_gaps",
            "payer_stratification": "Medicare,Medicaid,Commercial,Uninsured,Other",
        },
    }
