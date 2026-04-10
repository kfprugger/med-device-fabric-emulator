/**
 * Mock deployment simulator — runs entirely in the browser.
 * Simulates the 9-phase deployment with realistic logs and timing
 * so the UI can be tested without the Durable Functions backend.
 */

import type { DeploymentConfig, DeploymentStatus, PhaseInfo } from "./api";

/** Log entry for a deployment phase */
export interface PhaseLog {
  timestamp: string;
  level: "info" | "warn" | "error" | "success";
  message: string;
}

interface MockPhase {
  phase: string;
  durationMs: number;
  logs: Array<{ delayPct: number; level: PhaseLog["level"]; message: string }>;
  isManualGate?: boolean;
}

const MOCK_PHASES: MockPhase[] = [
  // ── Phase 1 ──
  {
    phase: "Phase 1: Fabric Workspace",
    durationMs: 3000,
    logs: [
      { delayPct: 0, level: "info", message: "Searching for workspace 'med-device-rti-hds'…" },
      { delayPct: 30, level: "success", message: "Workspace found: d2398e14-7f1f-481f-82d9-3c73a0ffb364" },
      { delayPct: 50, level: "info", message: "Provisioning workspace managed identity…" },
      { delayPct: 90, level: "success", message: "Workspace identity provisioned" },
    ],
  },
  {
    phase: "Phase 1: Base Azure Infrastructure",
    durationMs: 8000,
    logs: [
      { delayPct: 0, level: "info", message: "Resolving admin security group 'sg-azure-admins'…" },
      { delayPct: 5, level: "success", message: "Admin group resolved → a1b2c3d4-5678-90ab-cdef" },
      { delayPct: 10, level: "info", message: "Ensuring resource group 'rg-medtech-rti-fhir' in eastus…" },
      { delayPct: 15, level: "success", message: "Resource group ready" },
      { delayPct: 20, level: "info", message: "Deploying infra.bicep (Event Hub, ACR, Storage, Key Vault)…" },
      { delayPct: 40, level: "info", message: "Bicep deployment 'infra' in progress…" },
      { delayPct: 55, level: "success", message: "infra.bicep deployed → ACR: masimoxyz, EH: masimoxyz-eh-ns" },
      { delayPct: 60, level: "info", message: "Building emulator image masimo-emulator:v1 in ACR…" },
      { delayPct: 75, level: "success", message: "Image built → masimoxyz.azurecr.io/masimo-emulator:v1" },
      { delayPct: 80, level: "info", message: "Deploying emulator.bicep (Masimo ACI container)…" },
      { delayPct: 95, level: "success", message: "Emulator ACI deployed with system-assigned identity" },
    ],
  },
  {
    phase: "Phase 1: FHIR Service + Synthea + Loader",
    durationMs: 25000,
    logs: [
      { delayPct: 0, level: "info", message: "Deploying fhir-infra.bicep (FHIR workspace + service)…" },
      { delayPct: 10, level: "info", message: "Bicep deployment 'fhir-infra' in progress…" },
      { delayPct: 20, level: "success", message: "FHIR Service URL: https://fhir-xyz.fhir.azurehealthcareapis.com" },
      { delayPct: 25, level: "info", message: "Building synthea-generator:v1 in ACR…" },
      { delayPct: 30, level: "success", message: "Synthea image built" },
      { delayPct: 32, level: "info", message: "Running Synthea (100 patients)…" },
      { delayPct: 35, level: "info", message: "[synthea] Generating 100 patients in Atlanta, GA…" },
      { delayPct: 40, level: "info", message: "[synthea] Generated 25/100 patients…" },
      { delayPct: 50, level: "info", message: "[synthea] Generated 50/100 patients…" },
      { delayPct: 55, level: "info", message: "[synthea] Generated 75/100 patients…" },
      { delayPct: 60, level: "success", message: "[synthea] Generated 100/100 patients, uploading to blob…" },
      { delayPct: 65, level: "success", message: "Synthea completed (exit=0)" },
      { delayPct: 68, level: "info", message: "Building fhir-loader:v1 in ACR…" },
      { delayPct: 72, level: "info", message: "Running FHIR Loader (100 patient bundles)…" },
      { delayPct: 75, level: "info", message: "[fhir-loader] Processing bundle 1/100…" },
      { delayPct: 80, level: "info", message: "[fhir-loader] Processing bundle 25/100…" },
      { delayPct: 85, level: "info", message: "[fhir-loader] Processing bundle 50/100…" },
      { delayPct: 90, level: "info", message: "[fhir-loader] Processing bundle 75/100…" },
      { delayPct: 95, level: "success", message: "[fhir-loader] 100/100 bundles uploaded. 847 resources created." },
      { delayPct: 98, level: "success", message: "FHIR Loader completed (exit=0)" },
    ],
  },
  {
    phase: "Phase 1: DICOM Service + Loader",
    durationMs: 15000,
    logs: [
      { delayPct: 0, level: "info", message: "Deploying dicom-infra.bicep…" },
      { delayPct: 15, level: "success", message: "DICOM Service deployed" },
      { delayPct: 20, level: "info", message: "Building dicom-loader:v1 in ACR…" },
      { delayPct: 30, level: "info", message: "Running DICOM Loader (TCIA download + re-tag)…" },
      { delayPct: 40, level: "info", message: "[dicom-loader] Downloading TCGA-LUAD series from TCIA…" },
      { delayPct: 55, level: "info", message: "[dicom-loader] Downloaded 12 studies, re-tagging patient IDs…" },
      { delayPct: 70, level: "info", message: "[dicom-loader] Uploading to ADLS Gen2 dicom-output/…" },
      { delayPct: 85, level: "info", message: "[dicom-loader] 12/12 studies uploaded (2.3 GB)" },
      { delayPct: 95, level: "success", message: "DICOM Loader completed (exit=0)" },
    ],
  },
  {
    phase: "Phase 1: Fabric RTI",
    durationMs: 10000,
    logs: [
      { delayPct: 0, level: "info", message: "Creating Eventhouse 'MasimoEventhouse'…" },
      { delayPct: 15, level: "success", message: "Eventhouse created: eh-abc123" },
      { delayPct: 20, level: "info", message: "Creating KQL Database 'MasimoKQLDB'…" },
      { delayPct: 30, level: "success", message: "KQL Database created: db-def456" },
      { delayPct: 35, level: "info", message: "Running 01-alert-history-table.kql (3 commands)…" },
      { delayPct: 45, level: "success", message: "✓ .create-merge table TelemetryRaw" },
      { delayPct: 50, level: "success", message: "✓ .create-merge table AlertHistory" },
      { delayPct: 55, level: "success", message: "✓ .alter table TelemetryRaw policy update" },
      { delayPct: 60, level: "info", message: "Running 02-telemetry-functions.kql (5 commands)…" },
      { delayPct: 70, level: "success", message: "KQL 02: 5/5 succeeded" },
      { delayPct: 75, level: "info", message: "Running 03-clinical-alert-functions.kql (4 commands)…" },
      { delayPct: 85, level: "success", message: "KQL 03: 4/4 succeeded" },
      { delayPct: 90, level: "info", message: "Creating Eventstream for telemetry ingestion…" },
      { delayPct: 98, level: "success", message: "Fabric RTI Phase 1 complete" },
    ],
  },
  {
    phase: "Phase 1: HDS Detection",
    durationMs: 3000,
    isManualGate: false,
    logs: [
      { delayPct: 0, level: "info", message: "Checking for existing Healthcare Data Solutions deployment…" },
      { delayPct: 20, level: "info", message: "Searching workspace items for type 'HealthcareDataSolution'…" },
      { delayPct: 50, level: "success", message: "HDS solution detected: healthcare1_msft (deployed 2026-04-03T18:42:00Z)" },
      { delayPct: 65, level: "info", message: "Verifying scipy 1.11.4 in healthcare1_environment…" },
      { delayPct: 80, level: "success", message: "scipy 1.11.4 confirmed in environment" },
      { delayPct: 95, level: "success", message: "HDS pre-deployed — skipping manual gate, continuing automatically" },
    ],
  },
  // ── Phase 2 ──
  {
    phase: "Phase 2: Fabric RTI",
    durationMs: 8000,
    logs: [
      { delayPct: 0, level: "info", message: "Discovering Silver Lakehouse…" },
      { delayPct: 10, level: "success", message: "Silver Lakehouse: healthcare1_msft_silver (lh-789xyz)" },
      { delayPct: 15, level: "info", message: "Creating Bronze LH shortcut → FHIR export ADLS Gen2 storage…" },
      { delayPct: 25, level: "success", message: "Bronze shortcut FHIR-HDS created" },
      { delayPct: 30, level: "info", message: "Running 04-hds-enrichment-example.kql…" },
      { delayPct: 45, level: "success", message: "KQL 04: 3/3 succeeded" },
      { delayPct: 55, level: "info", message: "Running 05-dashboard-queries.kql…" },
      { delayPct: 70, level: "success", message: "KQL 05: 7/7 succeeded" },
      { delayPct: 80, level: "info", message: "Running 06-agent-wrapper-functions.kql…" },
      { delayPct: 92, level: "success", message: "KQL 06: 5/5 succeeded" },
      { delayPct: 98, level: "success", message: "Fabric RTI Phase 2 complete" },
    ],
  },
  {
    phase: "Phase 2: DICOM Shortcut + HDS Pipelines",
    durationMs: 5000,
    logs: [
      { delayPct: 0, level: "info", message: "Creating DICOM shortcut → ADLS Gen2 dicom-output/…" },
      { delayPct: 15, level: "success", message: "DICOM shortcut DICOM-HDS created" },
      { delayPct: 30, level: "info", message: "Triggering pipeline: healthcare1_msft_imaging_with_clinical_foundation_ingestion…" },
      { delayPct: 50, level: "success", message: "Pipeline triggered: imaging_with_clinical" },
      { delayPct: 60, level: "info", message: "Triggering pipeline: healthcare1_msft_omop_ingestion…" },
      { delayPct: 80, level: "success", message: "Pipeline triggered: omop_ingestion" },
      { delayPct: 95, level: "success", message: "All HDS pipelines triggered" },
    ],
  },
  {
    phase: "Phase 2: Data Agents",
    durationMs: 6000,
    logs: [
      { delayPct: 0, level: "info", message: "Building datasource config (2 KQL tables, 11 Lakehouse tables)…" },
      { delayPct: 15, level: "info", message: "Creating Patient 360 Data Agent…" },
      { delayPct: 35, level: "success", message: "Patient 360 agent created: agent-p360-abc" },
      { delayPct: 50, level: "info", message: "Creating Clinical Triage Data Agent…" },
      { delayPct: 75, level: "success", message: "Clinical Triage agent created: agent-triage-def" },
      { delayPct: 95, level: "success", message: "Data Agents deployed (2/2)" },
    ],
  },
  // ── Phase 3 ──
  {
    phase: "Phase 3: Imaging & Reporting",
    durationMs: 8000,
    logs: [
      { delayPct: 0, level: "info", message: "Deploying FabricDicomCohortingToolkit…" },
      { delayPct: 10, level: "info", message: "Creating Cohorting Data Agent…" },
      { delayPct: 30, level: "success", message: "Cohorting Agent created: agent-cohort-jkl" },
      { delayPct: 40, level: "info", message: "Deploying DICOM Viewer (OHIF) to Azure Static Web Apps…" },
      { delayPct: 60, level: "success", message: "OHIF Viewer: https://green-bush-0e9d6d01e.6.azurestaticapps.net" },
      { delayPct: 70, level: "info", message: "Deploying DICOMweb Proxy to Container Apps…" },
      { delayPct: 85, level: "success", message: "DICOMweb Proxy: https://hds-dicom-proxy.eastus.azurecontainerapps.io" },
      { delayPct: 90, level: "info", message: "Publishing Power BI Imaging Report (Direct Lake over Gold OMOP)…" },
      { delayPct: 98, level: "success", message: "Phase 3: Imaging Toolkit complete" },
    ],
  },
  // ── Phase 4 ──
  {
    phase: "Phase 4: Ontology",
    durationMs: 7000,
    logs: [
      { delayPct: 0, level: "info", message: "Verifying clinical pipeline completion…" },
      { delayPct: 10, level: "success", message: "Silver tables populated (11/11 tables with data)" },
      { delayPct: 15, level: "info", message: "Creating DeviceAssociation managed table via Spark SQL…" },
      { delayPct: 30, level: "success", message: "DeviceAssociation table created with 100 rows" },
      { delayPct: 35, level: "info", message: "Creating ClinicalDeviceOntology (9 entity types)…" },
      { delayPct: 50, level: "info", message: "Configuring data bindings to Silver Lakehouse SQL endpoint…" },
      { delayPct: 60, level: "info", message: "Configuring relationships (Patient ↔ Device, Patient ↔ Condition…)" },
      { delayPct: 70, level: "info", message: "Polling ontology provisioning status…" },
      { delayPct: 80, level: "info", message: "Provisioning: InProgress…" },
      { delayPct: 90, level: "success", message: "Ontology provisioned: ont-ghi789" },
      { delayPct: 93, level: "info", message: "Binding ontology to Patient 360 + Clinical Triage agents…" },
      { delayPct: 98, level: "success", message: "ClinicalDeviceOntology deployed + agents updated" },
    ],
  },
  {
    phase: "Phase 4: Data Activator",
    durationMs: 5000,
    logs: [
      { delayPct: 0, level: "info", message: "Creating ClinicalAlertActivator Reflex item…" },
      { delayPct: 20, level: "success", message: "Reflex created: reflex-mno456" },
      { delayPct: 30, level: "info", message: "Configuring trigger: SpO2 < 90% sustained ≥ 2 min…" },
      { delayPct: 45, level: "info", message: "Configuring trigger: Pulse Rate > 120 bpm sustained ≥ 3 min…" },
      { delayPct: 60, level: "info", message: "Configuring email action → joey@example.com…" },
      { delayPct: 75, level: "success", message: "Email rule created (tier ≥ URGENT, cooldown 15 min)" },
      { delayPct: 85, level: "info", message: "Activating Reflex triggers…" },
      { delayPct: 95, level: "success", message: "ClinicalAlertActivator active — monitoring AlertHistory" },
    ],
  },
];

/** In-memory store for mock deployments */
const mockInstances = new Map<
  string,
  {
    config: DeploymentConfig;
    phases: Array<PhaseInfo & { logs: PhaseLog[] }>;
    currentPhaseIndex: number;
    status: string;
    startedAt: string;
    resources: Record<string, string>;
    waitingForHds: boolean;
    timers: ReturnType<typeof setTimeout>[];
  }
>();

function generateId(): string {
  return "mock-" + Math.random().toString(36).slice(2, 10) + "-" + Date.now().toString(36);
}

function now(): string {
  return new Date().toISOString();
}

/**
 * Start a mock deployment. Returns an instanceId immediately,
 * then simulates phases in the background with timers.
 */
export function startMockDeployment(config: DeploymentConfig): string {
  const instanceId = generateId();

  // Build phase list respecting skip flags
  const activePhases = MOCK_PHASES.filter((p) => {
    if (config.skip_base_infra && p.phase.includes("Phase 1:")) return false;
    if (config.skip_fhir && p.phase.includes("Phase 2:")) return false;
    if (config.skip_dicom && p.phase.includes("Phase 2b")) return false;
    if (config.skip_fabric && p.phase.includes("Phase 3")) return false;
    return true;
  });

  const instance = {
    config,
    phases: activePhases.map((p) => ({
      phase: p.phase,
      status: "pending" as string,
      duration: undefined as number | undefined,
      logs: [] as PhaseLog[],
    })),
    currentPhaseIndex: -1,
    status: "Running",
    startedAt: now(),
    resources: {} as Record<string, string>,
    waitingForHds: false,
    timers: [] as ReturnType<typeof setTimeout>[],
  };

  mockInstances.set(instanceId, instance);

  // Kick off first phase
  advancePhase(instanceId);

  return instanceId;
}

function advancePhase(instanceId: string): void {
  const inst = mockInstances.get(instanceId);
  if (!inst) return;

  inst.currentPhaseIndex++;

  if (inst.currentPhaseIndex >= inst.phases.length) {
    // All done
    inst.status = "Completed";
    return;
  }

  const phaseIndex = inst.currentPhaseIndex;
  const phase = inst.phases[phaseIndex];
  const mockDef = MOCK_PHASES.find((m) => m.phase === phase.phase);
  if (!mockDef) return;

  // Manual gate (HDS)
  if (mockDef.isManualGate) {
    phase.status = "waiting_for_input";
    inst.waitingForHds = true;
    // Add the gate logs immediately
    for (const logDef of mockDef.logs) {
      phase.logs.push({
        timestamp: now(),
        level: logDef.level,
        message: logDef.message,
      });
    }
    return;
  }

  // Start the phase
  phase.status = "running";
  const startTime = Date.now();

  // Schedule each log entry
  for (const logDef of mockDef.logs) {
    const delay = (logDef.delayPct / 100) * mockDef.durationMs;
    const timer = setTimeout(() => {
      phase.logs.push({
        timestamp: now(),
        level: logDef.level,
        message: logDef.message,
      });
    }, delay);
    inst.timers.push(timer);
  }

  // Schedule phase completion
  const completeTimer = setTimeout(() => {
    phase.status = "succeeded";
    phase.duration = (Date.now() - startTime) / 1000;

    // Add mock resources
    addMockResources(inst, phase.phase);

    // Advance to next
    advancePhase(instanceId);
  }, mockDef.durationMs);
  inst.timers.push(completeTimer);
}

function addMockResources(inst: NonNullable<ReturnType<typeof mockInstances.get>>, phase: string): void {
  switch (true) {
    case phase.includes("Step 1:"):
      inst.resources.fabric_workspace_id = "d2398e14-7f1f-481f-82d9-3c73a0ffb364";
      inst.resources.fabric_workspace_name = inst.config.fabric_workspace_name;
      break;
    case phase.includes("Step 1b"):
      inst.resources.resource_group_name = inst.config.resource_group_name;
      inst.resources.acr_name = "masimoxyz";
      inst.resources.event_hub_namespace = "masimoxyz-eh-ns";
      inst.resources.storage_account_name = "stfhirxyz";
      break;
    case phase.includes("Step 2:"):
      inst.resources.fhir_service_url = "https://fhir-xyz.fhir.azurehealthcareapis.com";
      break;
    case phase.includes("Step 2b"):
      inst.resources.dicom_service_url = "https://dicom-xyz.dicom.azurehealthcareapis.com";
      break;
    case phase.includes("Step 3"):
      inst.resources.eventhouse_id = "eh-abc123";
      inst.resources.kql_db_id = "db-def456";
      break;
    case phase.includes("Step 5:"):
      inst.resources.silver_lakehouse_id = "lh-789xyz";
      break;
    case phase.includes("Step 6"):
      inst.resources.patient360_id = "agent-p360-abc";
      inst.resources.triage_id = "agent-triage-def";
      break;
    case phase.includes("Step 7"):
      inst.resources.cohorting_agent_id = "agent-cohort-jkl";
      inst.resources.ohif_viewer_url = "https://green-bush-0e9d6d01e.6.azurestaticapps.net";
      inst.resources.dicomweb_proxy_url = "https://hds-dicom-proxy.eastus.azurecontainerapps.io";
      break;
    case phase.includes("Step 8"):
      inst.resources.ontology_id = "ont-ghi789";
      break;
    case phase.includes("Step 9"):
      inst.resources.reflex_id = "reflex-mno456";
      break;
  }
}

/**
 * Resume after HDS manual gate.
 */
export function resumeMockHds(instanceId: string): void {
  const inst = mockInstances.get(instanceId);
  if (!inst || !inst.waitingForHds) return;

  inst.waitingForHds = false;
  const phase = inst.phases[inst.currentPhaseIndex];
  phase.status = "succeeded";
  phase.duration = 0;
  phase.logs.push({
    timestamp: now(),
    level: "success",
    message: "HDS deployment acknowledged. Resuming pipeline…",
  });

  advancePhase(instanceId);
}

/**
 * Cancel a mock deployment.
 */
export function cancelMockDeployment(instanceId: string): void {
  const inst = mockInstances.get(instanceId);
  if (!inst) return;

  // Clear all pending timers
  for (const t of inst.timers) clearTimeout(t);
  inst.timers = [];
  inst.status = "Terminated";

  // Mark current running phase as cancelled
  const current = inst.phases[inst.currentPhaseIndex];
  if (current && current.status === "running") {
    current.status = "cancelled";
    current.logs.push({
      timestamp: now(),
      level: "error",
      message: "Phase cancelled by user",
    });
  }
}

/**
 * Get the status of a mock deployment (matches DeploymentStatus shape).
 */
export function getMockStatus(instanceId: string): DeploymentStatus | null {
  const inst = mockInstances.get(instanceId);
  if (!inst) return null;

  const completedPhases = inst.phases.filter(
    (p) => p.status === "succeeded" || p.status === "skipped"
  );
  const currentPhase = inst.phases[inst.currentPhaseIndex];

  return {
    instanceId,
    runtimeStatus: inst.status,
    output:
      inst.status === "Completed" || inst.status === "Terminated"
        ? {
            status: inst.status === "Completed" ? "succeeded" : "cancelled",
            phases: inst.phases,
            resources: inst.resources,
          }
        : null,
    customStatus: {
      currentPhase: currentPhase?.phase ?? "",
      status: inst.waitingForHds ? "waiting_for_input" : currentPhase?.status ?? "pending",
      detail: inst.waitingForHds
        ? "Deploy Healthcare Data Solutions (HDS) manually in the Fabric portal, install scipy in the environment, then click 'Continue'."
        : "",
      completedPhases: completedPhases.length,
      totalPhases: inst.phases.length,
      resources: inst.resources,
    },
    createdTime: inst.startedAt,
    lastUpdatedTime: now(),
  };
}

/**
 * Get logs for a specific phase of a mock deployment.
 */
export function getMockPhaseLogs(
  instanceId: string,
  phaseName: string
): PhaseLog[] {
  const inst = mockInstances.get(instanceId);
  if (!inst) return [];
  const phase = inst.phases.find((p) => p.phase === phaseName);
  return phase?.logs ?? [];
}

/**
 * Get all phases with their logs.
 */
export function getMockPhases(
  instanceId: string
): Array<PhaseInfo & { logs: PhaseLog[] }> {
  const inst = mockInstances.get(instanceId);
  if (!inst) return [];
  return inst.phases;
}

/**
 * Check if an instanceId is a mock deployment.
 */
export function isMockInstance(instanceId: string): boolean {
  return instanceId.startsWith("mock-");
}

/**
 * List all mock deployment summaries for the History tab.
 */
export function listMockDeployments(): Array<{
  instanceId: string;
  name: string;
  runtimeStatus: string;
  createdTime: string | null;
  lastUpdatedTime: string | null;
  customStatus: Record<string, unknown> | null;
}> {
  const results: Array<{
    instanceId: string;
    name: string;
    runtimeStatus: string;
    createdTime: string | null;
    lastUpdatedTime: string | null;
    customStatus: Record<string, unknown> | null;
  }> = [];

  for (const [id, inst] of mockInstances) {
    results.push({
      instanceId: id,
      name: "deploy_all_orchestrator",
      runtimeStatus: inst.status,
      createdTime: inst.startedAt,
      lastUpdatedTime: now(),
      customStatus: {
        workspaceName: inst.config.fabric_workspace_name,
        completedPhases: inst.phases.filter((p) => p.status === "succeeded").length,
        totalPhases: inst.phases.length,
      },
    });
  }

  return results;
}

// ═══════════════════════════════════════════════════════════════════════
// MOCK RESOURCE SCANNER — Simulates Azure/Fabric resource enumeration
// for the Teardown tab
// ═══════════════════════════════════════════════════════════════════════

export interface MockSubscription {
  id: string;
  name: string;
}

export interface TeardownCandidate {
  type: "fabric" | "azure" | "spn";
  name: string;
  id: string;
  status: "full" | "partial" | "orphaned";
  detail: string;
  resourceCount?: number;
  expectedCount?: number;
  matchedArtifacts?: string[];
  subscription?: string;
  qualified?: boolean;
  previouslyDeployed?: boolean;
}

const MOCK_SUBSCRIPTIONS: MockSubscription[] = [
  { id: "5772d06a-5513-4cc5-ac08-a3805440c60e", name: "Azure-brakekat" },
  { id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890", name: "Dev/Test" },
  { id: "f9e8d7c6-b5a4-3210-fedc-ba0987654321", name: "Production" },
];

export interface MockCapacity {
  name: string;
  id: string;
  state: string;
  sku: string;
  resourceGroup: string;
  location: string;
  subscription: string;
}

const MOCK_CAPACITIES: MockCapacity[] = [
  {
    name: "fabrjbwu3",
    id: "/subscriptions/5772d06a/resourceGroups/rg-fabricskus/providers/Microsoft.Fabric/capacities/fabrjbwu3",
    state: "Active",
    sku: "F64",
    resourceGroup: "rg-fabricskus",
    location: "eastus",
    subscription: "5772d06a-5513-4cc5-ac08-a3805440c60e",
  },
  {
    name: "fabr-dev-f2",
    id: "/subscriptions/a1b2c3d4/resourceGroups/rg-fabric-dev/providers/Microsoft.Fabric/capacities/fabr-dev-f2",
    state: "Paused",
    sku: "F2",
    resourceGroup: "rg-fabric-dev",
    location: "eastus",
    subscription: "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  },
];

/**
 * Get mock Fabric capacities.
 */
export function getMockCapacities(): MockCapacity[] {
  return MOCK_CAPACITIES;
}

const MOCK_TEARDOWN_CANDIDATES: TeardownCandidate[] = [
  // Real workspace — matches actual deployed state
  {
    type: "fabric",
    name: "med-device-rti-hds-0404-1",
    id: "bdea7f44-adf9-4e4f-a4af-b13f48565b31",
    status: "full",
    detail: "Full deployment detected — 33 Fabric items present",
    resourceCount: 33,
    expectedCount: 33,
    matchedArtifacts: [
      "Eventhouse: MasimoEventhouse",
      "KQLDatabase: MasimoKQLDB",
      "Eventstream: TelemetryIngestion",
      "KQLDashboard: ClinicalMonitor",
      "KQLDashboard: ClinicalAlertsMap",
      "Lakehouse: healthcare1_msft_bronze",
      "Lakehouse: healthcare1_msft_silver",
      "Lakehouse: healthcare1_msft_gold",
      "Lakehouse: healthcare1_msft_reporting",
      "DataPipeline: (×4)",
      "Notebook: (×13)",
      "Environment: healthcare1_environment",
      "Healthcaredatasolution: healthcare1_msft",
      "SemanticModel: (×1)",
      "SQLEndpoint: (×4)",
    ],
  },
  // Partial deployment — old workspace, Phase 1 only
  {
    type: "fabric",
    name: "med-device-rti-hds-0403-7",
    id: "d2398e14-7f1f-481f-82d9-3c73a0ffb364",
    status: "partial",
    detail: "Partial deployment — Phase 1 artifacts only (8/33 items)",
    resourceCount: 8,
    expectedCount: 33,
    matchedArtifacts: [
      "Eventhouse: MasimoEventhouse",
      "KQLDatabase: MasimoKQLDB",
      "Eventstream: TelemetryIngestion",
      "KQLDashboard: ClinicalMonitor",
    ],
  },
  // Full Azure RG — matches actual rg-med-device-rti-0404-1
  {
    type: "azure",
    name: "rg-med-device-rti-0404-1",
    id: "/subscriptions/5772d06a/resourceGroups/rg-med-device-rti-0404-1",
    status: "full",
    detail: "Full Azure deployment — 12/12 resources",
    resourceCount: 12,
    expectedCount: 12,
    subscription: "5772d06a-5513-4cc5-ac08-a3805440c60e",
    matchedArtifacts: [
      "EventHub Namespace: masimobqnpr3sx5n3g2-eh-ns",
      "Container Registry: masimobqnpr3sx5n3g2acr",
      "Storage Account: stfhirbqnpr3sx5n3g2",
      "Key Vault: masimobqnpr3sx5n3g2-kv",
      "Managed Identity: id-aci-fhir-jobs",
      "Container Instance: masimo-emulator-grp",
      "Container Instance: synthea-generator-job",
      "Container Instance: fhir-loader-job",
      "Container Instance: dicom-loader-job",
      "Healthcare Workspace: hdwsbqnpr3sx5n3g2",
      "FHIR Service: fhirbqnpr3sx5n3g2",
      "EventGrid SystemTopic: stfhirbqnpr3sx5n3g2-*",
    ],
  },
  // Older Azure RG — rg-medtech-rti-fhir
  {
    type: "azure",
    name: "rg-medtech-rti-fhir",
    id: "/subscriptions/5772d06a/resourceGroups/rg-medtech-rti-fhir",
    status: "partial",
    detail: "Older Azure deployment — 10/12 resources (no DICOM loader)",
    resourceCount: 10,
    expectedCount: 12,
    subscription: "5772d06a-5513-4cc5-ac08-a3805440c60e",
    matchedArtifacts: [
      "EventHub Namespace: masimoth5y4kfhjahpm-eh-ns",
      "Container Registry: masimoth5y4kfhjahpmacr",
      "Storage Account: stfhirth5y4kfhjahpm",
      "Key Vault: masimoth5y4kfhjahpm-kv",
      "Managed Identity: id-aci-fhir-jobs",
      "Container Instance: masimo-emulator-grp",
      "Container Instance: synthea-generator-job",
      "Healthcare Workspace: hdwsth5y4kfhjahpm",
      "FHIR Service: fhirth5y4kfhjahpm",
      "EventGrid SystemTopic: stfhirth5y4kfhjahpm-*",
    ],
  },
  // SPN matching workspace identity
  {
    type: "spn",
    name: "med-device-rti-hds-0404-1",
    id: "sp-bdea7f44",
    status: "orphaned",
    detail: "Workspace identity SPN — matches workspace 'med-device-rti-hds-0404-1'",
    matchedArtifacts: [
      "App Registration: med-device-rti-hds-0404-1 (created 2026-04-04)",
    ],
  },
];

/**
 * Get mock Azure subscriptions.
 */
export function getMockSubscriptions(): MockSubscription[] {
  return MOCK_SUBSCRIPTIONS;
}

/**
 * Scan for teardown candidates (mock).
 */
export function scanForTeardownCandidates(
  subscriptionId?: string
): TeardownCandidate[] {
  if (subscriptionId) {
    return MOCK_TEARDOWN_CANDIDATES.filter(
      (c) => c.type !== "azure" || c.subscription === subscriptionId
    );
  }
  return MOCK_TEARDOWN_CANDIDATES;
}

// ═══════════════════════════════════════════════════════════════════════
// MOCK TEARDOWN SIMULATOR
// ═══════════════════════════════════════════════════════════════════════

export interface TeardownStep {
  name: string;
  status: "pending" | "running" | "deleted" | "failed" | "skipped";
  logs: PhaseLog[];
  duration?: number;
}

export interface TeardownInstance {
  instanceId: string;
  candidateName: string;
  candidateType: TeardownCandidate["type"];
  steps: TeardownStep[];
  status: "running" | "completed" | "failed";
  startedAt: string;
}

const teardownInstances = new Map<string, TeardownInstance>();

const FABRIC_TEARDOWN_STEPS = [
  { name: "Data Agents", delay: 2000, logs: ["Deleting Patient 360 agent…", "Deleting Clinical Triage agent…", "Deleting Cohorting Agent…", "✓ 3 agents deleted"] },
  { name: "Ontology", delay: 1500, logs: ["Deleting ClinicalDeviceOntology…", "✓ Ontology deleted"] },
  { name: "Reflex", delay: 1000, logs: ["Deleting ClinicalAlertActivator…", "✓ Reflex deleted"] },
  { name: "Data Pipelines", delay: 2000, logs: ["Deleting 4 HDS pipelines…", "✓ Pipelines deleted"] },
  { name: "Eventstream", delay: 1000, logs: ["Deleting TelemetryIngestion eventstream…", "✓ Eventstream deleted"] },
  { name: "KQL Dashboards", delay: 1500, logs: ["Deleting ClinicalMonitor dashboard…", "Deleting ClinicalAlertsMap dashboard…", "✓ 2 dashboards deleted"] },
  { name: "KQL Database", delay: 1000, logs: ["Deleting MasimoKQLDB…", "✓ KQL Database deleted"] },
  { name: "Eventhouse", delay: 1500, logs: ["Deleting MasimoEventhouse…", "✓ Eventhouse deleted"] },
  { name: "Lakehouses", delay: 2500, logs: ["Deleting healthcare1_msft_bronze…", "Deleting healthcare1_msft_silver…", "Deleting healthcare1_msft_gold…", "Deleting healthcare1_msft_reporting…", "Deleting 4 SQL endpoints…", "✓ 4 lakehouses + endpoints deleted"] },
  { name: "Notebooks", delay: 2000, logs: ["Deleting 13 notebooks…", "✓ Notebooks deleted"] },
  { name: "HDS & Environment", delay: 2000, logs: ["Deleting healthcare1_msft HDS solution…", "Deleting healthcare1_environment…", "Deleting SemanticModel…", "✓ HDS + environment deleted"] },
  { name: "Workspace Identity", delay: 1000, logs: ["Deprovisioning workspace identity…", "✓ Identity deprovisioned"] },
  { name: "Workspace", delay: 1500, logs: ["Deleting workspace med-device-rti-hds-0404-1…", "✓ Workspace deleted"] },
];

const AZURE_TEARDOWN_STEPS = [
  { name: "Container Instances", delay: 2000, logs: ["Deleting masimo-emulator ACI…", "Deleting synthea-generator-job…", "Deleting fhir-loader-job…", "Deleting dicom-loader-job…", "✓ 4 container instances deleted"] },
  { name: "FHIR Service", delay: 3000, logs: ["Deleting FHIR service fhir-xyz…", "Deleting FHIR workspace hdws-xyz…", "✓ FHIR resources deleted"] },
  { name: "DICOM Service", delay: 2000, logs: ["Deleting DICOM service…", "✓ DICOM resources deleted"] },
  { name: "Container Registry", delay: 1500, logs: ["Deleting ACR masimoxyzacr…", "✓ ACR deleted"] },
  { name: "Storage Accounts", delay: 2000, logs: ["Deleting stfhirxyz (hot)…", "Deleting stfhircoolxyz (cool)…", "✓ 2 storage accounts deleted"] },
  { name: "Key Vault", delay: 1000, logs: ["Deleting kv-masimoxyz…", "✓ Key Vault deleted"] },
  { name: "Event Hub", delay: 1500, logs: ["Deleting namespace masimoxyz-eh-ns…", "✓ Event Hub namespace deleted"] },
  { name: "Managed Identity", delay: 1000, logs: ["Deleting id-aci-fhir-jobs…", "✓ Managed Identity deleted"] },
  { name: "Resource Group", delay: 6000, logs: [
    "Initiating resource group deletion: rg-med-device-rti…",
    "Polling RG deletion status… (Deleting)",
    "Polling RG deletion status… (Deleting) [10s]",
    "Polling RG deletion status… (Deleting) [20s]",
    "Polling RG deletion status… (Deleting) [30s]",
    "Polling RG deletion status… (Deleting) [40s]",
    "Verifying: GET /subscriptions/.../resourceGroups/rg-med-device-rti → 404 Not Found",
    "✓ Resource group deleted and verified"
  ] },
];

const SPN_TEARDOWN_STEPS = [
  { name: "App Registration", delay: 2000, logs: ["Finding app registration…", "Deleting app registration…", "✓ SPN deleted"] },
];

/**
 * Start a mock teardown for a candidate.
 */
export function startMockTeardown(candidate: TeardownCandidate): string {
  const instanceId = `teardown-${Math.random().toString(36).slice(2, 8)}-${Date.now().toString(36)}`;

  const stepDefs =
    candidate.type === "fabric" ? FABRIC_TEARDOWN_STEPS
    : candidate.type === "azure" ? AZURE_TEARDOWN_STEPS
    : SPN_TEARDOWN_STEPS;

  const instance: TeardownInstance = {
    instanceId,
    candidateName: candidate.name,
    candidateType: candidate.type,
    steps: stepDefs.map((s) => ({
      name: s.name,
      status: "pending",
      logs: [],
    })),
    status: "running",
    startedAt: now(),
  };

  teardownInstances.set(instanceId, instance);
  runTeardownSteps(instanceId, stepDefs);

  return instanceId;
}

function runTeardownSteps(
  instanceId: string,
  stepDefs: Array<{ name: string; delay: number; logs: string[] }>
): void {
  let cumulativeDelay = 0;

  stepDefs.forEach((def, idx) => {
    // Start step
    const startDelay = cumulativeDelay;
    setTimeout(() => {
      const inst = teardownInstances.get(instanceId);
      if (!inst || inst.status !== "running") return;
      inst.steps[idx].status = "running";
      inst.steps[idx].logs.push({
        timestamp: now(),
        level: "info",
        message: `Deleting: ${def.name}…`,
      });
    }, startDelay);

    // Stream logs
    def.logs.forEach((log, logIdx) => {
      const logDelay = startDelay + ((logIdx + 1) / (def.logs.length + 1)) * def.delay;
      setTimeout(() => {
        const inst = teardownInstances.get(instanceId);
        if (!inst || inst.status !== "running") return;
        inst.steps[idx].logs.push({
          timestamp: now(),
          level: log.startsWith("✓") ? "success" : "info",
          message: log,
        });
      }, logDelay);
    });

    // Complete step
    cumulativeDelay += def.delay;
    setTimeout(() => {
      const inst = teardownInstances.get(instanceId);
      if (!inst || inst.status !== "running") return;
      inst.steps[idx].status = "deleted";
      inst.steps[idx].duration = def.delay / 1000;

      // Check if all done
      if (inst.steps.every((s) => s.status === "deleted" || s.status === "skipped")) {
        inst.status = "completed";
      }
    }, cumulativeDelay);
  });
}

/**
 * Get a teardown instance.
 */
export function getMockTeardownInstance(instanceId: string): TeardownInstance | null {
  return teardownInstances.get(instanceId) ?? null;
}

/**
 * List all teardown instances.
 */
export function listMockTeardowns(): TeardownInstance[] {
  return Array.from(teardownInstances.values());
}

/**
 * Check if an instanceId is a mock teardown.
 */
export function isMockTeardown(instanceId: string): boolean {
  return instanceId.startsWith("teardown-");
}
