from __future__ import annotations

import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


WORKSPACE_ID = "workspace-123"
_MISSING = object()


class FakeFabricClient:
    def __init__(
        self,
        pipeline_ids_by_name: dict[str, str],
        statuses_by_name: dict[str, str] | None = None,
        post_errors_by_name: dict[str, Exception] | None = None,
    ) -> None:
        self.pipeline_ids_by_name = pipeline_ids_by_name
        self.pipeline_names_by_id = {
            pipeline_id: pipeline_name
            for pipeline_name, pipeline_id in pipeline_ids_by_name.items()
        }
        self.statuses_by_name = statuses_by_name or {}
        self.post_errors_by_name = post_errors_by_name or {}
        self.events: list[tuple[str, str | int]] = []

    def find_lakehouse(self, workspace_id: str, name: str) -> dict[str, str] | None:
        assert workspace_id == WORKSPACE_ID
        if name == "bronze":
            return {"id": "bronze-id", "displayName": "bronze"}
        return None

    def find_item(
        self, workspace_id: str, pipeline_name: str, item_type: str
    ) -> dict[str, str] | None:
        assert workspace_id == WORKSPACE_ID
        assert item_type == "DataPipeline"
        pipeline_id = self.pipeline_ids_by_name.get(pipeline_name)
        if pipeline_id is None:
            return None
        return {"id": pipeline_id, "displayName": pipeline_name}

    def list_items(self, workspace_id: str, item_type: str) -> list[dict[str, str]]:
        assert workspace_id == WORKSPACE_ID
        assert item_type == "DataPipeline"
        return [
            {"id": pipeline_id, "displayName": pipeline_name}
            for pipeline_name, pipeline_id in self.pipeline_ids_by_name.items()
        ]

    def call(
        self,
        method: str,
        endpoint: str,
        body: dict | None = None,
        max_retries: int = 3,
    ) -> dict[str, list[dict[str, str]]]:
        del body, max_retries
        pipeline_id = endpoint.split("/items/", 1)[1].split("/", 1)[0]
        pipeline_name = self.pipeline_names_by_id[pipeline_id]
        self.events.append((method, pipeline_name))

        if method == "POST":
            post_error = self.post_errors_by_name.get(pipeline_name)
            if post_error is not None:
                raise post_error
            return {"value": []}

        if method == "GET":
            return {"value": [{"status": self.statuses_by_name[pipeline_name]}]}

        raise AssertionError(f"unexpected Fabric API method: {method}")


class DeployHdsCmaTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orchestrator_dir = str(Path(__file__).resolve().parents[1])
        self._inserted_path = False
        if self._orchestrator_dir not in sys.path:
            sys.path.insert(0, self._orchestrator_dir)
            self._inserted_path = True

        self._previous_shared_module = sys.modules.get("shared", _MISSING)
        self._previous_fabric_client_module = sys.modules.get(
            "shared.fabric_client", _MISSING
        )

        fake_shared_module = types.ModuleType("shared")
        fake_shared_module.__path__ = []
        fake_fabric_client_module = types.ModuleType("shared.fabric_client")
        fake_fabric_client_module.FabricClient = object
        fake_shared_module.fabric_client = fake_fabric_client_module
        sys.modules["shared"] = fake_shared_module
        sys.modules["shared.fabric_client"] = fake_fabric_client_module
        sys.modules.pop("activities.deploy_hds_pipelines", None)

        self.deploy_hds = importlib.import_module("activities.deploy_hds_pipelines")
        self.addCleanup(self._cleanup_imports)

    def _cleanup_imports(self) -> None:
        sys.modules.pop("activities.deploy_hds_pipelines", None)
        if self._previous_fabric_client_module is _MISSING:
            sys.modules.pop("shared.fabric_client", None)
        else:
            sys.modules["shared.fabric_client"] = self._previous_fabric_client_module
        if self._previous_shared_module is _MISSING:
            sys.modules.pop("shared", None)
        else:
            sys.modules["shared"] = self._previous_shared_module
        if self._inserted_path:
            sys.path.remove(self._orchestrator_dir)

    def install_fake_client(self, fake_client: FakeFabricClient) -> None:
        client_patcher = patch.object(
            self.deploy_hds, "FabricClient", lambda _api_base: fake_client
        )
        sleep_patcher = patch.object(
            self.deploy_hds.time,
            "sleep",
            lambda seconds: fake_client.events.append(("SLEEP", seconds)),
        )
        client_patcher.start()
        sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)
        self.addCleanup(client_patcher.stop)

    def run_activity(self) -> dict:
        return self.deploy_hds.run(
            {"fabric_api_base": "https://fabric.test/v1"},
            {"fabric_workspace_id": WORKSPACE_ID},
        )

    def pipeline_ids(self, include_cma: bool) -> dict[str, str]:
        ids = {
            pipeline_name: f"pipeline-{index}"
            for index, pipeline_name in enumerate(
                self.deploy_hds.CORE_HDS_PIPELINE_NAMES, start=1
            )
        }
        if include_cma:
            ids[self.deploy_hds.CMA_PIPELINE_NAME] = "pipeline-cma"
        return ids

    def completed_core_statuses(self) -> dict[str, str]:
        return {
            pipeline_name: "Completed"
            for pipeline_name in self.deploy_hds.CORE_HDS_PIPELINE_NAMES
        }

    def posted_pipelines(self, fake_client: FakeFabricClient) -> list[str | int]:
        return [
            pipeline_name
            for method, pipeline_name in fake_client.events
            if method == "POST"
        ]

    def test_cma_is_triggered_after_clinical_completes_before_imaging_and_omop(
        self,
    ) -> None:
        fake_client = FakeFabricClient(
            pipeline_ids_by_name=self.pipeline_ids(include_cma=True),
            statuses_by_name=self.completed_core_statuses(),
        )
        self.install_fake_client(fake_client)

        result = self.run_activity()

        clinical_name, imaging_name, omop_name = self.deploy_hds.CORE_HDS_PIPELINE_NAMES
        cma_name = self.deploy_hds.CMA_PIPELINE_NAME
        self.assertEqual(
            result["pipeline_results"],
            {
                pipeline_name: "completed"
                for pipeline_name in self.deploy_hds.CORE_HDS_PIPELINE_NAMES
            },
        )
        self.assertEqual(
            result["non_blocking_followups"], {cma_name: "triggered_non_blocking"}
        )
        self.assertLess(
            fake_client.events.index(("GET", clinical_name)),
            fake_client.events.index(("POST", cma_name)),
        )
        self.assertLess(
            fake_client.events.index(("POST", cma_name)),
            fake_client.events.index(("POST", imaging_name)),
        )
        self.assertLess(
            fake_client.events.index(("POST", cma_name)),
            fake_client.events.index(("POST", omop_name)),
        )
        self.assertEqual(
            self.posted_pipelines(fake_client),
            [clinical_name, cma_name, imaging_name, omop_name],
        )

    def test_absent_cma_is_recorded_not_deployed_without_extra_post(self) -> None:
        fake_client = FakeFabricClient(
            pipeline_ids_by_name=self.pipeline_ids(include_cma=False),
            statuses_by_name=self.completed_core_statuses(),
        )
        self.install_fake_client(fake_client)

        result = self.run_activity()

        self.assertEqual(
            result["non_blocking_followups"],
            {self.deploy_hds.CMA_PIPELINE_NAME: "not_deployed"},
        )
        self.assertEqual(
            self.posted_pipelines(fake_client),
            self.deploy_hds.CORE_HDS_PIPELINE_NAMES,
        )

    def test_omop_failure_does_not_suppress_cma_triggered_after_clinical(
        self,
    ) -> None:
        statuses = self.completed_core_statuses()
        clinical_name, imaging_name, omop_name = self.deploy_hds.CORE_HDS_PIPELINE_NAMES
        statuses[omop_name] = "Failed"
        fake_client = FakeFabricClient(
            pipeline_ids_by_name=self.pipeline_ids(include_cma=True),
            statuses_by_name=statuses,
        )
        self.install_fake_client(fake_client)

        result = self.run_activity()

        cma_name = self.deploy_hds.CMA_PIPELINE_NAME
        self.assertEqual(result["pipeline_results"][clinical_name], "completed")
        self.assertEqual(result["pipeline_results"][imaging_name], "completed")
        self.assertEqual(result["pipeline_results"][omop_name], "failed: Failed")
        self.assertEqual(
            result["non_blocking_followups"], {cma_name: "triggered_non_blocking"}
        )
        self.assertLess(
            fake_client.events.index(("GET", clinical_name)),
            fake_client.events.index(("POST", cma_name)),
        )
        self.assertLess(
            fake_client.events.index(("POST", cma_name)),
            fake_client.events.index(("POST", omop_name)),
        )
        self.assertEqual(
            self.posted_pipelines(fake_client),
            [clinical_name, cma_name, imaging_name, omop_name],
        )

    def test_clinical_failure_skips_cma_and_downstream_core_pipelines(self) -> None:
        statuses = self.completed_core_statuses()
        clinical_name, imaging_name, omop_name = self.deploy_hds.CORE_HDS_PIPELINE_NAMES
        statuses[clinical_name] = "Failed"
        fake_client = FakeFabricClient(
            pipeline_ids_by_name=self.pipeline_ids(include_cma=True),
            statuses_by_name=statuses,
        )
        self.install_fake_client(fake_client)

        result = self.run_activity()

        cma_name = self.deploy_hds.CMA_PIPELINE_NAME
        self.assertEqual(result["pipeline_results"][clinical_name], "failed: Failed")
        self.assertEqual(
            result["pipeline_results"][imaging_name], "skipped_prerequisites_incomplete"
        )
        self.assertEqual(
            result["pipeline_results"][omop_name], "skipped_prerequisites_incomplete"
        )
        self.assertEqual(
            result["non_blocking_followups"], {cma_name: "skipped_clinical_incomplete"}
        )
        self.assertEqual(self.posted_pipelines(fake_client), [clinical_name])

    def test_sdoh_and_claims_sidecars_trigger_before_clinical_wait_without_patient_outreach(
        self,
    ) -> None:
        sdoh_name = "healthcare1_msft_sdoh_ingestion"
        claims_name = "healthcare1_msft_claims_enrichment"
        patient_outreach_name = "healthcare1_msft_patient_outreach"
        fake_client = FakeFabricClient(
            pipeline_ids_by_name={
                **self.pipeline_ids(include_cma=True),
                sdoh_name: "pipeline-sdoh",
                claims_name: "pipeline-claims",
                patient_outreach_name: "pipeline-patient-outreach",
            },
            statuses_by_name=self.completed_core_statuses(),
        )
        self.install_fake_client(fake_client)

        result = self.run_activity()

        clinical_name = self.deploy_hds.CORE_HDS_PIPELINE_NAMES[0]
        cma_name = self.deploy_hds.CMA_PIPELINE_NAME
        clinical_get_index = fake_client.events.index(("GET", clinical_name))
        posts_before_clinical_wait = [
            pipeline_name
            for method, pipeline_name in fake_client.events[:clinical_get_index]
            if method == "POST"
        ]
        self.assertIn(sdoh_name, posts_before_clinical_wait)
        self.assertIn(claims_name, posts_before_clinical_wait)
        self.assertNotIn(patient_outreach_name, posts_before_clinical_wait)
        self.assertLess(
            fake_client.events.index(("POST", sdoh_name)), clinical_get_index
        )
        self.assertLess(
            fake_client.events.index(("POST", claims_name)), clinical_get_index
        )
        self.assertEqual(
            result["non_blocking_followups"][sdoh_name], "triggered_non_blocking"
        )
        self.assertEqual(
            result["non_blocking_followups"][claims_name], "triggered_non_blocking"
        )
        self.assertEqual(
            result["non_blocking_followups"][cma_name], "triggered_non_blocking"
        )

    def test_cma_trigger_failure_is_reported_as_non_blocking_warning(self) -> None:
        fake_client = FakeFabricClient(
            pipeline_ids_by_name=self.pipeline_ids(include_cma=True),
            statuses_by_name=self.completed_core_statuses(),
            post_errors_by_name={
                self.deploy_hds.CMA_PIPELINE_NAME: RuntimeError("cma unavailable")
            },
        )
        self.install_fake_client(fake_client)

        result = self.run_activity()

        self.assertEqual(
            result["pipeline_results"],
            {
                pipeline_name: "completed"
                for pipeline_name in self.deploy_hds.CORE_HDS_PIPELINE_NAMES
            },
        )
        self.assertEqual(
            result["non_blocking_followups"],
            {
                self.deploy_hds.CMA_PIPELINE_NAME: "warning: error: cma unavailable"
            },
        )


if __name__ == "__main__":
    unittest.main()
