from __future__ import annotations

import base64
import json

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
        statuses_by_name: dict[str, str | list[str]] | None = None,
        post_errors_by_name: dict[str, Exception] | None = None,
        item_ids_by_type_and_name: dict[tuple[str, str], str] | None = None,
        definitions_by_item_id: dict[str, dict] | None = None,
    ) -> None:
        self.pipeline_ids_by_name = pipeline_ids_by_name
        self.pipeline_names_by_id = {
            pipeline_id: pipeline_name
            for pipeline_name, pipeline_id in pipeline_ids_by_name.items()
        }
        self.statuses_by_name = {
            name: list(status) if isinstance(status, list) else [status]
            for name, status in (statuses_by_name or {}).items()
        }
        self.post_errors_by_name = post_errors_by_name or {}
        self.item_ids_by_type_and_name = {
            ("DataPipeline", pipeline_name): pipeline_id
            for pipeline_name, pipeline_id in pipeline_ids_by_name.items()
        }
        self.item_ids_by_type_and_name.update(item_ids_by_type_and_name or {})
        self.item_names_by_id = {
            item_id: display_name
            for (
                _item_type,
                display_name,
            ), item_id in self.item_ids_by_type_and_name.items()
        }
        self.definitions_by_item_id = definitions_by_item_id or {}
        self.update_definition_calls: list[dict] = []
        self.get_definition_calls: list[dict] = []
        self.create_item_calls: list[dict] = []
        self.events: list[tuple[str, str | int]] = []

    def find_lakehouse(self, workspace_id: str, name: str) -> dict[str, str] | None:
        assert workspace_id == WORKSPACE_ID
        if name == "bronze":
            return {"id": "bronze-id", "displayName": "bronze"}
        if name == "healthcare1_msft_gold_cma":
            return {"id": "gold-cma-id", "displayName": "healthcare1_msft_gold_cma"}
        return None

    def find_item(
        self, workspace_id: str, display_name: str, item_type: str
    ) -> dict[str, str] | None:
        assert workspace_id == WORKSPACE_ID
        item_id = self.item_ids_by_type_and_name.get((item_type, display_name))
        if item_id is None:
            return None
        return {"id": item_id, "displayName": display_name, "type": item_type}

    def list_items(self, workspace_id: str, item_type: str) -> list[dict[str, str]]:
        assert workspace_id == WORKSPACE_ID
        return [
            {"id": item_id, "displayName": display_name, "type": item_type}
            for (
                stored_type,
                display_name,
            ), item_id in self.item_ids_by_type_and_name.items()
            if stored_type == item_type
        ]

    def call(
        self,
        method: str,
        endpoint: str,
        body: dict | None = None,
        max_retries: int = 3,
    ) -> dict[str, list[dict[str, str]]]:
        del body, max_retries
        # Gold CMA lakehouse SQL-endpoint lookup used by the semantic-model repoint.
        if "/lakehouses/gold-cma-id" in endpoint and method == "GET":
            self.events.append(("GET", "gold-cma-sql-endpoint"))
            return {
                "properties": {
                    "sqlEndpointProperties": {
                        "connectionString": "live-gold-cma.datawarehouse.fabric.microsoft.com"
                    }
                }
            }
        pipeline_id = endpoint.split("/items/", 1)[1].split("/", 1)[0]
        pipeline_name = self.pipeline_names_by_id[pipeline_id]
        self.events.append((method, pipeline_name))

        if method == "POST":
            post_error = self.post_errors_by_name.get(pipeline_name)
            if post_error is not None:
                raise post_error
            return {"value": []}

        if method == "GET":
            statuses = self.statuses_by_name.get(pipeline_name, ["Failed"])
            status = statuses.pop(0) if len(statuses) > 1 else statuses[0]
            return {"value": [{"status": status}]}

        raise AssertionError(f"unexpected Fabric API method: {method}")

    def update_item_definition(
        self, workspace_id: str, item_id: str, definition: dict
    ) -> dict:
        assert workspace_id == WORKSPACE_ID
        display_name = self.item_names_by_id[item_id]
        self.events.append(("UPDATE_DEFINITION", display_name))
        self.update_definition_calls.append(
            {"workspace_id": workspace_id, "item_id": item_id, "definition": definition}
        )
        return {}

    def get_item_definition(self, workspace_id: str, item_id: str) -> dict:
        assert workspace_id == WORKSPACE_ID
        display_name = self.item_names_by_id[item_id]
        self.events.append(("GET_DEFINITION", display_name))
        self.get_definition_calls.append(
            {"workspace_id": workspace_id, "item_id": item_id}
        )
        return self.definitions_by_item_id[item_id]

    def create_item(
        self, workspace_id: str, display_name: str, item_type: str, definition: dict
    ) -> dict[str, str]:
        assert workspace_id == WORKSPACE_ID
        item_id = f"created-{display_name}"
        self.item_ids_by_type_and_name[(item_type, display_name)] = item_id
        self.item_names_by_id[item_id] = display_name
        self.events.append(("CREATE_ITEM", display_name))
        self.create_item_calls.append(
            {
                "workspace_id": workspace_id,
                "display_name": display_name,
                "item_type": item_type,
                "definition": definition,
            }
        )
        return {"id": item_id}


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

    def run_activity(self, workspace_name: str | None = None) -> dict:
        config = {"fabric_api_base": "https://fabric.test/v1"}
        if workspace_name is not None:
            config["fabric_workspace_name"] = workspace_name
        return self.deploy_hds.run(config, {"fabric_workspace_id": WORKSPACE_ID})

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

    def completed_statuses(self, include_cma: bool) -> dict[str, str]:
        statuses = self.completed_core_statuses()
        if include_cma:
            statuses[self.deploy_hds.CMA_PIPELINE_NAME] = "Completed"
        return statuses

    def cma_item_ids(self, include_report: bool = True) -> dict[tuple[str, str], str]:
        item_ids = {
            (
                "SemanticModel",
                self.deploy_hds.CMA_SEMANTIC_MODEL_NAME,
            ): "semantic-model-id"
        }
        if include_report:
            item_ids[("Report", self.deploy_hds.CMA_REPORT_NAMES[0])] = "report-id"
        return item_ids

    def cma_report_definition(self, connection_string: str) -> dict:
        pbir = {
            "version": "4.0",
            "datasetReference": {
                "byConnection": {"connectionString": connection_string}
            },
        }
        return {
            "format": "PBIR-Legacy",
            "parts": [
                {
                    "path": "definition.pbir",
                    "payload": base64.b64encode(json.dumps(pbir).encode("utf-8")).decode(
                        "ascii"
                    ),
                    "payloadType": "InlineBase64",
                }
            ],
        }

    def decoded_definition_part(self, definition: dict, path: str) -> bytes:
        matches = [part for part in definition["parts"] if part["path"] == path]
        self.assertEqual(len(matches), 1)
        return base64.b64decode(matches[0]["payload"])

    def update_definition_for(self, fake_client: FakeFabricClient, item_id: str) -> dict:
        matches = [
            call
            for call in fake_client.update_definition_calls
            if call["item_id"] == item_id
        ]
        self.assertEqual(len(matches), 1)
        return matches[0]["definition"]["definition"]

    def event_indexes(
        self, fake_client: FakeFabricClient, event: tuple[str, str | int]
    ) -> list[int]:
        return [
            index for index, recorded in enumerate(fake_client.events) if recorded == event
        ]

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
            statuses_by_name=self.completed_statuses(include_cma=True),
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


    def test_cma_finalization_waits_after_core_then_overwrites_model_and_rebinds_report(
        self,
    ) -> None:
        cma_name = self.deploy_hds.CMA_PIPELINE_NAME
        semantic_model_name = self.deploy_hds.CMA_SEMANTIC_MODEL_NAME
        report_name = self.deploy_hds.CMA_REPORT_NAMES[0]
        workspace_name = "Healthcare One Workspace"
        statuses = self.completed_core_statuses()
        statuses[cma_name] = ["Running", "Completed"]
        fake_client = FakeFabricClient(
            pipeline_ids_by_name=self.pipeline_ids(include_cma=True),
            statuses_by_name=statuses,
            item_ids_by_type_and_name=self.cma_item_ids(),
            definitions_by_item_id={
                "report-id": self.cma_report_definition(
                    "Data Source=old;initial catalog=old;semanticmodelid=old"
                )
            },
        )
        self.install_fake_client(fake_client)

        result = self.run_activity(workspace_name=workspace_name)

        self.assertEqual(
            result["cma_finalization"],
            {
                "pipeline_completion": "completed",
                "semantic_model_datasource": "repointed:live-gold-cma.datawarehouse.fabric.microsoft.com",
                "semantic_model": "overwritten",
                f"report:{report_name}": "rebound",
            },
        )
        cma_get_indexes = self.event_indexes(fake_client, ("GET", cma_name))
        self.assertEqual(len(cma_get_indexes), 2)
        omop_name = self.deploy_hds.CORE_HDS_PIPELINE_NAMES[2]
        self.assertLess(
            fake_client.events.index(("GET", omop_name)), cma_get_indexes[0]
        )
        self.assertLess(
            cma_get_indexes[-1],
            fake_client.events.index(("UPDATE_DEFINITION", semantic_model_name)),
        )
        self.assertLess(
            fake_client.events.index(("UPDATE_DEFINITION", semantic_model_name)),
            fake_client.events.index(("UPDATE_DEFINITION", report_name)),
        )

        semantic_definition = self.update_definition_for(
            fake_client, "semantic-model-id"
        )
        self.assertEqual(semantic_definition["format"], "TMDL")
        expected_model = (
            self.deploy_hds.CMA_ARTIFACT_DIR
            / f"{semantic_model_name}.SemanticModel"
            / "definition"
            / "model.tmdl"
        ).read_bytes()
        self.assertEqual(
            self.decoded_definition_part(semantic_definition, "definition/model.tmdl"),
            expected_model,
        )

        # The Sql.Database source in every table part must be repointed to the live
        # Gold CMA SQL endpoint (the fix that keeps CMA report visuals from going blank).
        person_tmdl = self.decoded_definition_part(
            semantic_definition, "definition/tables/person.tmdl"
        ).decode("utf-8")
        self.assertIn(
            'Sql.Database("live-gold-cma.datawarehouse.fabric.microsoft.com", '
            '"healthcare1_msft_gold_cma")',
            person_tmdl,
        )
        self.assertNotIn("nkhahdl5to4ezo6p5bg76flepa", person_tmdl)

        report_definition = self.update_definition_for(fake_client, "report-id")
        patched_pbir = json.loads(
            self.decoded_definition_part(report_definition, "definition.pbir").decode(
                "utf-8"
            )
        )
        expected_connection_string = (
            "Data Source=powerbi://api.powerbi.com/v1.0/myorg/"
            f"{workspace_name};initial catalog={semantic_model_name};"
            "integrated security=ClaimsToken;semanticmodelid=semantic-model-id"
        )
        self.assertEqual(
            patched_pbir["datasetReference"]["byConnection"]["connectionString"],
            expected_connection_string,
        )
        self.assertEqual(
            fake_client.get_definition_calls,
            [{"workspace_id": WORKSPACE_ID, "item_id": "report-id"}],
        )
        self.assertEqual(fake_client.create_item_calls, [])

    def test_absent_cma_is_recorded_not_deployed_without_extra_post(self) -> None:
        fake_client = FakeFabricClient(
            pipeline_ids_by_name=self.pipeline_ids(include_cma=False),
            statuses_by_name=self.completed_core_statuses(),
            item_ids_by_type_and_name=self.cma_item_ids(),
            definitions_by_item_id={
                "report-id": self.cma_report_definition(
                    "Data Source=old;initial catalog=old;semanticmodelid=old"
                )
            },
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
        self.assertEqual(result["cma_finalization"], {})
        self.assertEqual(fake_client.update_definition_calls, [])
        self.assertEqual(fake_client.get_definition_calls, [])
        self.assertEqual(fake_client.create_item_calls, [])

    def test_omop_failure_does_not_suppress_cma_triggered_after_clinical(
        self,
    ) -> None:
        statuses = self.completed_statuses(include_cma=True)
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
            statuses_by_name=self.completed_statuses(include_cma=True),
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
            statuses_by_name=self.completed_statuses(include_cma=True),
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
