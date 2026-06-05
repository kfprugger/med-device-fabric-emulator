/**
 * API client for the Durable Functions backend.
 */

const API_BASE = "/api";

interface ApiRequestOptions extends RequestInit {
  timeoutMs?: number;
  retry?: number;
  retryDelayMs?: number;
}

function isAbortError(error: unknown) {
  return error instanceof DOMException && error.name === "AbortError";
}

async function sleep(ms: number) {
  await new Promise((resolve) => window.setTimeout(resolve, ms));
}

async function parseResponseBody(resp: Response): Promise<unknown> {
  const text = await resp.text();
  if (!text) return null;
  const contentType = resp.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  }
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function errorMessageFromBody(body: unknown, fallback: string) {
  if (!body) return fallback;
  if (typeof body === "string") return body.slice(0, 500) || fallback;
  if (typeof body === "object") {
    const record = body as Record<string, unknown>;
    const issues = Array.isArray(record.issues) ? record.issues.map(String) : [];
    const detail = record.detail;
    const detailMessages = Array.isArray(detail)
      ? detail
          .map((entry) => {
            const value = entry as { loc?: unknown[]; msg?: string };
            const path = Array.isArray(value?.loc) ? value.loc.slice(1).join(".") : "";
            return path ? `${path}: ${value?.msg ?? "Invalid value"}` : value?.msg ?? "Invalid request";
          })
          .filter(Boolean)
      : typeof detail === "string"
        ? [detail]
        : [];
    const message = record.error || record.message || record.title;
    return [typeof message === "string" ? message : fallback, ...issues, ...detailMessages]
      .filter(Boolean)
      .join(" ");
  }
  return fallback;
}

export async function requestJson<T>(
  input: string,
  { timeoutMs = 15000, retry = 0, retryDelayMs = 500, signal, ...init }: ApiRequestOptions = {}
): Promise<T> {
  let attempt = 0;
  let lastError: unknown;

  while (attempt <= retry) {
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
    const onAbort = () => controller.abort();
    if (signal) {
      if (signal.aborted) controller.abort();
      else signal.addEventListener("abort", onAbort, { once: true });
    }

    try {
      const resp = await fetch(input, { ...init, signal: controller.signal });
      const body = await parseResponseBody(resp);
      if (!resp.ok) {
        throw new Error(errorMessageFromBody(body, `Request failed (${resp.status})`));
      }
      return body as T;
    } catch (error) {
      lastError = error;
      const shouldRetry = attempt < retry && !isAbortError(error) && (!signal || !signal.aborted);
      if (!shouldRetry) {
        if (isAbortError(error)) {
          throw new Error(`Request timed out after ${Math.round(timeoutMs / 1000)}s`);
        }
        throw error;
      }
      await sleep(retryDelayMs * Math.pow(2, attempt));
      attempt += 1;
    } finally {
      window.clearTimeout(timeoutId);
      if (signal) signal.removeEventListener("abort", onAbort);
    }
  }

  throw lastError instanceof Error ? lastError : new Error("Request failed");
}

export async function requestVoid(input: string, options: ApiRequestOptions = {}): Promise<void> {
  await requestJson<unknown>(input, options);
}

export interface DeploymentConfig {
  resource_group_name: string;
  location: string;
  admin_security_group: string;
  fabric_workspace_name: string;
  patient_count: number;
  tags: Record<string, string>;
  skip_base_infra: boolean;
  skip_fhir: boolean;
  skip_dicom: boolean;
  skip_fabric: boolean;
  alert_email: string;
  capacity_subscription_id: string;
  capacity_resource_group: string;
  capacity_name: string;
  pause_capacity_after_deploy: boolean;
  reuse_patients: boolean;
  use_cached_synthea: boolean;
  skip_synthea: boolean;
  skip_device_assoc: boolean;
  skip_fhir_export: boolean;
  skip_rti_phase2: boolean;
  skip_hds_pipelines: boolean;
  skip_data_agents: boolean;
  skip_imaging: boolean;
  skip_ontology: boolean;
  skip_activator: boolean;
  skip_quality_measures: boolean;
  source_resource_group?: string;
}

export interface DeploymentStatus {
  instanceId: string;
  runtimeStatus: string;
  output: {
    status: string;
    phases: PhaseInfo[];
    resources: Record<string, string>;
  } | null;
  customStatus: {
    currentPhase: string;
    status: string;
    detail: string;
    completedPhases: number;
    totalPhases: number;
    resources: Record<string, string>;
    workspaceName?: string;
    resourceGroupName?: string;
    runType?: string;
    logs?: Array<{ timestamp: string; level: string; message: string; phase?: number }>;
    durationSeconds?: number;
  } | null;
  createdTime: string | null;
  lastUpdatedTime: string | null;
}

export interface PhaseInfo {
  phase: string;
  status: string;
  duration?: number;
  warnings?: string[];
  milestone?: number;
}

export interface DeploymentSummary {
  instanceId: string;
  name: string;
  runtimeStatus: string;
  createdTime: string | null;
  lastUpdatedTime: string | null;
  customStatus: Record<string, unknown> | null;
}

export interface AuthMechanismContext {
  installed: boolean;
  loggedIn: boolean;
  user: string;
  subscriptionName: string;
  subscriptionId: string;
  tenantId: string;
  error: string;
}

export interface AuthContext {
  ready: boolean;
  cli: AuthMechanismContext;
  pwsh: AuthMechanismContext;
  aligned: {
    subscription: boolean;
    tenant: boolean;
  };
  issues: string[];
}

export interface Subscription {
  id: string;
  name: string;
}

export async function startDeployment(
  config: DeploymentConfig
): Promise<{ instanceId: string; statusUrl: string }> {
  return requestJson(`${API_BASE}/deploy/start`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
    timeoutMs: 30000,
  });
}

export async function getAuthContext(force = false): Promise<AuthContext> {
  return requestJson(`${API_BASE}/auth/context${force ? "?force=1" : ""}`, { timeoutMs: 8000, retry: 1 });
}

export async function getDeploymentStatus(
  instanceId: string,
  signal?: AbortSignal
): Promise<DeploymentStatus> {
  return requestJson(`${API_BASE}/deploy/${encodeURIComponent(instanceId)}/status`, {
    timeoutMs: 12000,
    retry: 1,
    signal,
  });
}

export async function resumeAfterHds(instanceId: string): Promise<void> {
  return requestVoid(`${API_BASE}/deploy/${encodeURIComponent(instanceId)}/resume-hds`, {
    method: "POST",
    timeoutMs: 15000,
  });
}

export async function cancelDeployment(instanceId: string): Promise<void> {
  return requestVoid(`${API_BASE}/deploy/${encodeURIComponent(instanceId)}/cancel`, {
    method: "POST",
    timeoutMs: 15000,
  });
}

export async function startTeardown(config: {
  fabric_workspace_name: string;
  resource_group_name: string;
  delete_workspace: boolean;
  delete_azure_rg: boolean;
}): Promise<{ instanceId: string }> {
  return requestJson(`${API_BASE}/teardown/start`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
    timeoutMs: 30000,
  });
}

export async function listDeployments(signal?: AbortSignal): Promise<DeploymentSummary[]> {
  return requestJson(`${API_BASE}/deployments`, { timeoutMs: 12000, retry: 1, signal });
}

export async function deleteDeployment(instanceId: string): Promise<void> {
  return requestVoid(`${API_BASE}/deploy/${encodeURIComponent(instanceId)}`, {
    method: "DELETE",
    timeoutMs: 15000,
  });
}

export interface DeployedResource {
  name: string;
  type: string;
  fullType?: string;
  location?: string;
  id: string;
}

export interface DeployedResourcesResult {
  azure: DeployedResource[];
  fabric: DeployedResource[];
  workspace: { name: string; id: string; url: string } | null;
}

export async function getDeployedResources(
  instanceId: string,
  signal?: AbortSignal
): Promise<DeployedResourcesResult> {
  return requestJson(`${API_BASE}/deploy/${encodeURIComponent(instanceId)}/deployed-resources`, {
    timeoutMs: 20000,
    retry: 1,
    signal,
  });
}

export interface AfterActionReportResult {
  adminGroup: string;
  keyVaultName: string;
  azurePortalUrl: string;
  fabricWorkspaceUrl: string;
  resources: Array<{
    name: string;
    category: "Azure" | "Fabric";
    type: string;
    identity: string;
    credentialLocation: string;
    credentialDetails: string;
    accessControlDetails: string;
  }>;
}

export async function getAfterActionReport(
  instanceId: string,
  signal?: AbortSignal
): Promise<AfterActionReportResult> {
  if (instanceId.startsWith("mock-")) {
    return {
      adminGroup: "sg-msft-hds-dicom-project",
      keyVaultName: "masimo-kv-mock",
      azurePortalUrl: "https://portal.azure.com",
      fabricWorkspaceUrl: "https://app.powerbi.com",
      resources: [
        {
          name: "masimo-kv-mock",
          category: "Azure",
          type: "Vaults",
          identity: "System-Assigned Managed Identity",
          credentialLocation: "Azure Key Vault",
          credentialDetails: "SpnClientId, SpnClientSecret, SpnTenantId, EventHubConnStr (Secure Secrets)",
          accessControlDetails: "Securely stores connection strings and SPN secrets. Fully governed by RBAC roles assigned to Admin Security Group 'sg-msft-hds-dicom-project'."
        },
        {
          name: "masimoxyz-eh-ns",
          category: "Azure",
          type: "Namespaces",
          identity: "System-Assigned Managed Identity / SAS Rule",
          credentialLocation: "Azure Key Vault",
          credentialDetails: "EventHubConnStr (Key Vault Secret)",
          accessControlDetails: "Uses Managed Identity for device emulator stream ingestion. SAS authorization rule fallback is stored in Key Vault."
        },
        {
          name: "stfhirxyz",
          category: "Azure",
          type: "StorageAccounts",
          identity: "System-Assigned Managed Identity",
          credentialLocation: "None",
          credentialDetails: "None (Entra ID RBAC / Service-to-Service)",
          accessControlDetails: "Service-to-service communication is handled securely via Azure Managed Identity without stored secrets."
        },
        {
          name: "Population Health & Quality Dashboard",
          category: "Fabric",
          type: "SemanticModel",
          identity: "Service Principal (SPN) Fixed Identity",
          credentialLocation: "Azure Key Vault (Secret)",
          credentialDetails: "SpnClientSecret (Azure Key Vault Secret)",
          accessControlDetails: "Automated Direct Lake data connections utilize the SPN secrets retrieved from Key Vault to query OneLake securely."
        },
        {
          name: "Masimo Telemetry Trigger",
          category: "Fabric",
          type: "Reflex",
          identity: "Workspace Identity",
          credentialLocation: "Workspace Boundary",
          credentialDetails: "None (Fabric Native Integration)",
          accessControlDetails: "Data Activator alerts operate entirely within the workspace security boundary to route care team notifications."
        }
      ]
    };
  }

  return requestJson(`${API_BASE}/deploy/${encodeURIComponent(instanceId)}/after-action-report`, {
    timeoutMs: 20000,
    retry: 1,
    signal,
  });
}

export interface PhaseLogEntry {
  timestamp: string;
  level: "info" | "warn" | "error" | "success";
  message: string;
  phase: string;
}

export async function getPhaseLogs(
  instanceId: string,
  phaseName: string
): Promise<PhaseLogEntry[]> {
  try {
    return await requestJson(
      `${API_BASE}/deploy/${encodeURIComponent(instanceId)}/logs?phase=${encodeURIComponent(phaseName)}`,
      { timeoutMs: 10000, retry: 1 }
    );
  } catch {
    return [];
  }
}

export function streamPhaseLogs(
  instanceId: string,
  phaseName: string,
  onLog: (entry: PhaseLogEntry) => void,
  onError?: (err: Event) => void
): { close: () => void } {
  const url = `${API_BASE}/deploy/${encodeURIComponent(instanceId)}/logs/stream?phase=${encodeURIComponent(phaseName)}`;
  const eventSource = new EventSource(url);

  eventSource.onmessage = (event) => {
    try {
      const entry = JSON.parse(event.data) as PhaseLogEntry;
      onLog(entry);
    } catch (e) {
      console.error("Failed to parse streamed log line:", e);
    }
  };

  if (onError) {
    eventSource.onerror = (err) => {
      onError(err);
    };
  }

  return {
    close: () => {
      eventSource.close();
    },
  };
}

export async function clearAllDeployments(): Promise<void> {
  return requestVoid(`${API_BASE}/deployments/clear`, {
    method: "POST",
    timeoutMs: 15000,
  });
}

export interface ExistingDeploymentInfo {
  found: boolean;
  instanceId: string;
  createdTime: string;
  workspaceName: string;
  resourceGroupName: string;
  configuredPatientCount: number;
  fhirPatientCount: number;
  fhirDeviceCount: number;
  exportedFiles: number;
  dicomStudies: number;
  emulatorRunning: boolean;
  emulatorDeviceCount?: number;
  azureRgExists: boolean;
  priorConfig?: Partial<DeploymentConfig>;
}

export async function checkExistingDeployment(
  workspaceName: string,
  resourceGroup: string,
  signal?: AbortSignal,
  deep = false,
): Promise<ExistingDeploymentInfo | null> {
  const params = new URLSearchParams();
  if (workspaceName) params.set("workspace_name", workspaceName);
  if (resourceGroup) params.set("resource_group", resourceGroup);
  if (deep) params.set("deep", "1");
  try {
    const data = await requestJson<ExistingDeploymentInfo & { found?: boolean }>(
      `${API_BASE}/deploy/check-existing?${params}`,
      { timeoutMs: 12000, retry: 1, signal }
    );
    return data?.found ? data : null;
  } catch {
    return null;
  }
}

export interface FabricCapacity {
  name: string;
  id: string;
  state: string;
  sku: string;
  resourceGroup: string;
  location: string;
  subscription: string;
  subscriptionName?: string;
}

export async function listCapacities(
  subscriptionId = "",
  force = false
): Promise<FabricCapacity[]> {
  const params = new URLSearchParams();
  if (subscriptionId) params.set("subscription_id", subscriptionId);
  if (force) params.set("force", "true");
  const query = params.toString() ? `?${params.toString()}` : "";
  try {
    return await requestJson(`${API_BASE}/scan/capacities${query}`, {
      timeoutMs: 60000,
      retry: 1,
      retryDelayMs: 1000,
    });
  } catch {
    return [];
  }
}

export async function resumeCapacity(
  subscriptionId: string,
  resourceGroup: string,
  name: string,
): Promise<void> {
  const params = new URLSearchParams({ subscription_id: subscriptionId, resource_group: resourceGroup, name });
  return requestVoid(`${API_BASE}/capacity/resume?${params}`, { method: "POST", timeoutMs: 20000 });
}

export interface DeploymentCapacityMapping {
  capacityName: string;
  capacityResourceGroup: string;
  capacitySubscriptionId: string;
  workspaceName: string;
}

export async function getDeploymentCapacity(
  rgName: string
): Promise<DeploymentCapacityMapping | null> {
  try {
    const data = await requestJson<DeploymentCapacityMapping | null>(
      `${API_BASE}/deployment-capacity/${encodeURIComponent(rgName)}`,
      { timeoutMs: 10000, retry: 1 }
    );
    return data || null;
  } catch {
    return null;
  }
}

export async function listAhdsRegions(): Promise<string[]> {
  try {
    return await requestJson(`${API_BASE}/scan/ahds-regions`, { timeoutMs: 15000, retry: 1 });
  } catch {
    return [];
  }
}

export interface ResourceScanJob {
  scanId: string;
  status: "idle" | "running" | "completed" | "failed" | "missing";
  phase?: string;
  message?: string;
  candidates?: unknown[];
  counts?: { fabric: number; azure: number; spn: number };
  startedAt?: string | null;
  completedAt?: string | null;
  error?: string;
}

export async function listSubscriptions(): Promise<Subscription[]> {
  return requestJson(`${API_BASE}/scan/subscriptions`, { timeoutMs: 15000, retry: 1 });
}

export async function startResourceScan(subscriptionId: string): Promise<{ scanId: string }> {
  return requestJson(`${API_BASE}/scan/resources/start?subscription_id=${encodeURIComponent(subscriptionId)}`, {
    method: "POST",
    timeoutMs: 20000,
  });
}

export async function getResourceScan(scanId: string): Promise<ResourceScanJob> {
  return requestJson(`${API_BASE}/scan/resources/${encodeURIComponent(scanId)}`, {
    timeoutMs: 10000,
    retry: 1,
  });
}

export async function getLocks(): Promise<string[]> {
  return requestJson(`${API_BASE}/locks`, { timeoutMs: 10000, retry: 1 });
}

export async function setLock(id: string, locked: boolean): Promise<void> {
  return requestVoid(`${API_BASE}/locks/${id.replace(/^\//, "")}`, {
    method: locked ? "POST" : "DELETE",
    timeoutMs: 10000,
  });
}
