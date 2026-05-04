/**
 * API client for the Durable Functions backend.
 */

const API_BASE = "/api";

async function fetchWithTimeout(
  input: string,
  init: RequestInit = {},
  timeoutMs = 15000
): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(input, { ...init, signal: controller.signal });
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error(`Request timed out after ${Math.round(timeoutMs / 1000)}s`);
    }
    throw error;
  } finally {
    window.clearTimeout(timeoutId);
  }
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
  // ── Granular component toggles ──
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
  } | null;
  createdTime: string | null;
  lastUpdatedTime: string | null;
}

export interface PhaseInfo {
  phase: string;
  status: string;
  duration?: number;
  warnings?: string[];
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

export async function startDeployment(
  config: DeploymentConfig
): Promise<{ instanceId: string; statusUrl: string }> {
  const resp = await fetch(`${API_BASE}/deploy/start`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  if (!resp.ok) {
    const err = await resp.json();
    const issues = Array.isArray(err.issues) ? err.issues : [];
    const detail = err.detail;
    const detailMessages = Array.isArray(detail)
      ? detail
          .map((entry: { loc?: unknown[]; msg?: string }) => {
            const path = Array.isArray(entry?.loc) ? entry.loc.slice(1).join(".") : "";
            return path ? `${path}: ${entry?.msg ?? "Invalid value"}` : entry?.msg ?? "Invalid request";
          })
          .filter(Boolean)
      : typeof detail === "string"
        ? [detail]
        : [];
    const messageParts = [err.error || "Failed to start deployment", ...issues, ...detailMessages].filter(Boolean);
    throw new Error(messageParts.join(" "));
  }
  return resp.json();
}

export async function getAuthContext(): Promise<AuthContext> {
  const resp = await fetchWithTimeout(`${API_BASE}/auth/context`, {}, 8000);
  if (!resp.ok) {
    throw new Error("Failed to get auth context");
  }
  return resp.json();
}

export async function getDeploymentStatus(
  instanceId: string
): Promise<DeploymentStatus> {
  const resp = await fetch(`${API_BASE}/deploy/${instanceId}/status`);
  if (!resp.ok) throw new Error("Failed to get status");
  return resp.json();
}

export async function resumeAfterHds(instanceId: string): Promise<void> {
  const resp = await fetch(`${API_BASE}/deploy/${instanceId}/resume-hds`, {
    method: "POST",
  });
  if (!resp.ok) throw new Error("Failed to resume");
}

export async function cancelDeployment(instanceId: string): Promise<void> {
  const resp = await fetch(`${API_BASE}/deploy/${instanceId}/cancel`, {
    method: "POST",
  });
  if (!resp.ok) throw new Error("Failed to cancel");
}

export async function startTeardown(config: {
  fabric_workspace_name: string;
  resource_group_name: string;
  delete_workspace: boolean;
  delete_azure_rg: boolean;
}): Promise<{ instanceId: string }> {
  const resp = await fetch(`${API_BASE}/teardown/start`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config),
  });
  if (!resp.ok) throw new Error("Failed to start teardown");
  return resp.json();
}

export async function listDeployments(): Promise<DeploymentSummary[]> {
  const resp = await fetch(`${API_BASE}/deployments`);
  if (!resp.ok) throw new Error("Failed to list deployments");
  return resp.json();
}

export async function deleteDeployment(instanceId: string): Promise<void> {
  const resp = await fetch(`${API_BASE}/deploy/${instanceId}`, {
    method: "DELETE",
  });
  if (!resp.ok) throw new Error("Failed to delete");
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
  instanceId: string
): Promise<DeployedResourcesResult> {
  const resp = await fetch(`${API_BASE}/deploy/${instanceId}/deployed-resources`);
  if (!resp.ok) throw new Error("Failed to get deployed resources");
  return resp.json();
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
  const resp = await fetch(
    `${API_BASE}/deploy/${instanceId}/logs?phase=${encodeURIComponent(phaseName)}`
  );
  if (!resp.ok) return [];
  return resp.json();
}

export async function clearAllDeployments(): Promise<void> {
  const resp = await fetch(`${API_BASE}/deployments/clear`, {
    method: "POST",
  });
  if (!resp.ok) throw new Error("Failed to clear");
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
): Promise<ExistingDeploymentInfo | null> {
  const params = new URLSearchParams();
  if (workspaceName) params.set("workspace_name", workspaceName);
  if (resourceGroup) params.set("resource_group", resourceGroup);
  const resp = await fetch(`${API_BASE}/deploy/check-existing?${params}`, { signal });
  if (!resp.ok) return null;
  const data = await resp.json();
  return data?.found ? data : null;
}

// ── Fabric Capacity API ───────────────────────────────────────────────

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
  subscriptionId = ""
): Promise<FabricCapacity[]> {
  const query = subscriptionId
    ? `?subscription_id=${encodeURIComponent(subscriptionId)}`
    : "";
  // Capacity scan invokes `az` subprocesses; cold calls can take 10-30s when
  // the Resource Graph fallback fans out across multiple subscriptions.
  const resp = await fetchWithTimeout(
    `${API_BASE}/scan/capacities${query}`,
    {},
    60000
  );
  if (!resp.ok) return [];
  return resp.json();
}

export async function resumeCapacity(
  subscriptionId: string,
  resourceGroup: string,
  name: string,
): Promise<void> {
  const params = new URLSearchParams({ subscription_id: subscriptionId, resource_group: resourceGroup, name });
  const resp = await fetch(`${API_BASE}/capacity/resume?${params}`, { method: "POST" });
  if (!resp.ok) throw new Error("Failed to resume capacity");
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
  const resp = await fetch(`${API_BASE}/deployment-capacity/${encodeURIComponent(rgName)}`);
  if (!resp.ok) return null;
  const data = await resp.json();
  return data || null;
}

/**
 * Fetch Azure regions where Azure Health Data Services (AHDS) is available.
 * Returns normalised ARM location names (e.g. "eastus", "northcentralus").
 */
export async function listAhdsRegions(): Promise<string[]> {
  try {
    const resp = await fetch(`${API_BASE}/scan/ahds-regions`);
    if (!resp.ok) return [];
    return resp.json();
  } catch {
    return [];
  }
}
