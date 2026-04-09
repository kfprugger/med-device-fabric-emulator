import { useEffect, useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import {
  Badge,
  Button,
  Field,
  Input,
  makeStyles,
  Option,
  Dropdown,
  Text,
  Title2,
  tokens,
} from "@fluentui/react-components";
import {
  EyeRegular,
  ArrowSyncRegular,
  DeleteRegular,
  DismissRegular,
  ChevronDownRegular,
  ChevronUpRegular,
  OpenRegular,
} from "@fluentui/react-icons";
import { listDeployments, deleteDeployment, clearAllDeployments, type DeploymentSummary } from "../api";
import { listMockDeployments } from "../mockDeployment";
import { MockDataBanner } from "../components/MockDataBanner";
import { AzureBadge, FabricBadge } from "../components/TypeBadges";

const useStyles = makeStyles({
  header: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: tokens.spacingVerticalM,
  },
  list: {
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalS,
    marginTop: tokens.spacingVerticalS,
  },
  card: {
    backgroundColor: tokens.colorNeutralBackground1,
    border: `1px solid ${tokens.colorNeutralStroke2}`,
    borderRadius: tokens.borderRadiusMedium,
    overflow: "hidden",
    transition: "box-shadow 0.2s ease",
    ":hover": {
      boxShadow: tokens.shadow4,
    },
  },
  cardRow: {
    display: "flex",
    alignItems: "center",
    padding: `${tokens.spacingVerticalS} ${tokens.spacingHorizontalL}`,
    gap: tokens.spacingHorizontalM,
  },
  instanceId: {
    fontFamily: "'Cascadia Code', 'Consolas', monospace",
    fontSize: tokens.fontSizeBase200,
    color: tokens.colorNeutralForeground2,
    minWidth: "180px",
  },
  workspace: {
    flex: 1,
    fontWeight: tokens.fontWeightSemibold,
  },
  actions: {
    display: "flex",
    gap: tokens.spacingHorizontalXS,
    alignItems: "center",
  },
  infoPanel: {
    padding: `${tokens.spacingVerticalS} ${tokens.spacingHorizontalL}`,
    backgroundColor: tokens.colorNeutralBackground3,
    borderTop: `1px solid ${tokens.colorNeutralStroke2}`,
    display: "flex",
    gap: tokens.spacingHorizontalXL,
    flexWrap: "wrap",
    fontSize: tokens.fontSizeBase200,
  },
  linkGroup: {
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalXXS,
  },
  linkLabel: {
    color: tokens.colorNeutralForeground3,
    fontSize: tokens.fontSizeBase100,
    fontWeight: tokens.fontWeightSemibold,
  },
  link: {
    display: "inline-flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalXS,
    color: tokens.colorBrandForeground1,
    textDecoration: "none",
    fontSize: tokens.fontSizeBase200,
    ":hover": {
      textDecoration: "underline",
    },
  },
  filterRow: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    gap: tokens.spacingHorizontalM,
    marginBottom: tokens.spacingVerticalS,
    flexWrap: "wrap" as const,
  },
});

function statusColor(
  s: string
): "success" | "danger" | "informative" | "warning" | "subtle" {
  if (s === "Completed") return "success";
  if (s === "Failed" || s === "Terminated") return "danger";
  if (s === "Running") return "informative";
  if (s === "Suspended") return "warning";
  return "subtle";
}

export function DeploymentHistory() {
  const styles = useStyles();
  const navigate = useNavigate();
  const [deployments, setDeployments] = useState<DeploymentSummary[]>([]);
  const [usingMock, setUsingMock] = useState(false);
  const [loading, setLoading] = useState(true);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [runFilter, setRunFilter] = useState<"all" | "deployment" | "teardown">("all");
  const [nameFilter, setNameFilter] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");

  const toggleExpanded = (id: string) => {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const refresh = useCallback(() => {
    const mockDeps = listMockDeployments();
    listDeployments()
      .then((real) => {
        setDeployments([...mockDeps, ...real]);
        setUsingMock(mockDeps.length > 0 && real.length === 0);
      })
      .catch(() => {
        setDeployments(mockDeps);
        setUsingMock(mockDeps.length > 0);
      })
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  }, [refresh]);

  const handleClearAll = async () => {
    if (!window.confirm(`Delete ALL ${deployments.length} deployment records? This cannot be undone.`)) return;
    try { await clearAllDeployments(); } catch { /* ignore */ }
    refresh();
  };

  const handleDelete = async (instanceId: string) => {
    if (!window.confirm(`Delete deployment record "${instanceId}"?`)) return;
    try { await deleteDeployment(instanceId); } catch { /* ignore */ }
    refresh();
  };

  const filteredDeployments = deployments.filter((deployment) => {
    const cs = deployment.customStatus as Record<string, unknown> | null;
    const isTeardown = (cs?.runType as string) === "teardown"
      || deployment.name === "teardown_orchestrator"
      || deployment.instanceId.toLowerCase().startsWith("teardown");

    if (runFilter === "teardown" && !isTeardown) return false;
    if (runFilter === "deployment" && isTeardown) return false;

    // Name filter: match against instanceId, workspace name, or RG name
    if (nameFilter) {
      const q = nameFilter.toLowerCase();
      const ws = ((cs?.workspaceName as string) || "").toLowerCase();
      const rg = ((cs?.resourceGroupName as string) || "").toLowerCase();
      const displayName = ((cs?.displayName as string) || "").toLowerCase();
      const id = deployment.instanceId.toLowerCase();
      if (!ws.includes(q) && !rg.includes(q) && !displayName.includes(q) && !id.includes(q)) return false;
    }

    // Date range filter
    if (dateFrom && deployment.createdTime) {
      const created = new Date(deployment.createdTime);
      const from = new Date(dateFrom);
      if (created < from) return false;
    }
    if (dateTo && deployment.createdTime) {
      const created = new Date(deployment.createdTime);
      const to = new Date(dateTo);
      to.setDate(to.getDate() + 1); // include the full end day
      if (created >= to) return false;
    }

    return true;
  }).sort((a, b) => {
    const ta = a.createdTime ? new Date(a.createdTime).getTime() : 0;
    const tb = b.createdTime ? new Date(b.createdTime).getTime() : 0;
    return tb - ta;
  });

  return (
    <div>
      <div className={styles.header}>
        {usingMock && <MockDataBanner />}
        <Title2>Run History</Title2>
        <div style={{ display: "flex", gap: tokens.spacingHorizontalS }}>
          <Button appearance="subtle" icon={<ArrowSyncRegular />} onClick={refresh}>
            Refresh
          </Button>
          {deployments.length > 0 && (
            <Button appearance="subtle" icon={<DeleteRegular />} onClick={handleClearAll}>
              Clear All
            </Button>
          )}
        </div>
      </div>

      <div className={styles.filterRow}>
        <Field label="Type" style={{ minWidth: 160 }}>
          <Dropdown
            value={runFilter === "all" ? "All runs" : runFilter === "deployment" ? "Deployments" : "Teardowns"}
            selectedOptions={[runFilter]}
            onOptionSelect={(_, data) => setRunFilter((data.optionValue as "all" | "deployment" | "teardown") ?? "all")}
          >
            <Option value="all">All runs</Option>
            <Option value="deployment">Deployments</Option>
            <Option value="teardown">Teardowns</Option>
          </Dropdown>
        </Field>
        <Field label="Search" style={{ minWidth: 200, flex: 1 }}>
          <Input
            value={nameFilter}
            onChange={(_, d) => setNameFilter(d.value)}
            placeholder="Filter by name, workspace, or RG..."
            type="search"
          />
        </Field>
        <Field label="From" style={{ minWidth: 150 }}>
          <Input
            type="date"
            value={dateFrom}
            onChange={(_, d) => setDateFrom(d.value)}
          />
        </Field>
        <Field label="To" style={{ minWidth: 150 }}>
          <Input
            type="date"
            value={dateTo}
            onChange={(_, d) => setDateTo(d.value)}
          />
        </Field>
        <Text size={200} style={{ color: tokens.colorNeutralForeground3, alignSelf: "flex-end", paddingBottom: 6 }}>
          {filteredDeployments.length} of {deployments.length}
        </Text>
      </div>

      <div className={styles.list}>
        {loading && (
          <Text style={{ textAlign: "center", padding: tokens.spacingVerticalXL, color: tokens.colorNeutralForeground3 }}>
            Loading deployment history...
          </Text>
        )}
        {!loading && filteredDeployments.map((d) => {
          const cs = d.customStatus as Record<string, unknown> | null;
          const workspace = (cs?.workspaceName as string) || "";
          const rgName = (cs?.resourceGroupName as string) || "";
          const isTeardown = (cs?.runType as string) === "teardown"
            || d.name === "teardown_orchestrator"
            || d.instanceId.toLowerCase().startsWith("teardown");
          const displayName = (cs?.displayName as string)
            || (workspace && rgName ? `${workspace} + ${rgName}` : workspace || rgName || "—");
          const completed = (cs?.completedPhases as number) ?? 0;
          const total = (cs?.totalPhases as number) ?? 0;
          const isMock = d.instanceId.startsWith("mock-");
          const isExpanded = expandedIds.has(d.instanceId);
          const links = cs?.links as Record<string, string> | undefined;

          return (
            <div key={d.instanceId} className={styles.card}>
              <div className={styles.cardRow}>
                <div className={styles.instanceId}>
                  {d.instanceId}
                  {isMock && (
                    <Badge color="informative" size="small" style={{ marginLeft: 6 }}>mock</Badge>
                  )}
                </div>
                <Text className={styles.workspace}>{displayName}</Text>
                <Badge color={isTeardown ? "warning" : "brand"}>
                  {isTeardown ? "Teardown" : "Deployment"}
                </Badge>
                <Badge color={statusColor(d.runtimeStatus)}>{d.runtimeStatus}</Badge>
                {total > 0 && (
                  <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>
                    {completed}/{total}
                  </Text>
                )}
                <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>
                  {d.createdTime ? new Date(d.createdTime).toLocaleString() : "—"}
                </Text>
                <div className={styles.actions}>
                  <Button
                    appearance="subtle"
                    icon={isExpanded ? <ChevronUpRegular /> : <ChevronDownRegular />}
                    onClick={() => toggleExpanded(d.instanceId)}
                    size="small"
                  >
                    Info
                  </Button>
                  <Button
                    appearance="subtle"
                    icon={<EyeRegular />}
                    onClick={() => navigate(`/monitor/${d.instanceId}`)}
                    size="small"
                  >
                    View
                  </Button>
                  <Button
                    appearance="subtle"
                    icon={<DismissRegular />}
                    onClick={() => handleDelete(d.instanceId)}
                    size="small"
                  />
                </div>
              </div>

              {isExpanded && (
                <div className={styles.infoPanel}>
                  {rgName && (
                    <div className={styles.linkGroup}>
                      <span className={styles.linkLabel}>Azure Resource Group</span>
                      <a
                        href={links?.azurePortal || `https://portal.azure.com/#browse/resourcegroups/filterValue/${rgName}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className={styles.link}
                      >
                        <AzureBadge /> {rgName} <OpenRegular fontSize={12} />
                      </a>
                    </div>
                  )}
                  {workspace && workspace !== "—" && (
                    <div className={styles.linkGroup}>
                      <span className={styles.linkLabel}>Fabric Workspace</span>
                      <a
                        href={links?.fabricWorkspace || `https://app.fabric.microsoft.com/?experience=fabric-developer`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className={styles.link}
                      >
                        <FabricBadge /> {workspace} <OpenRegular fontSize={12} />
                      </a>
                    </div>
                  )}
                  <div className={styles.linkGroup}>
                    <span className={styles.linkLabel}>Instance ID</span>
                    <Text size={200} font="monospace">{d.instanceId}</Text>
                  </div>
                  <div className={styles.linkGroup}>
                    <span className={styles.linkLabel}>Created</span>
                    <Text size={200}>
                      {d.createdTime ? new Date(d.createdTime).toLocaleString() : "—"}
                    </Text>
                  </div>
                </div>
              )}
            </div>
          );
        })}
        {filteredDeployments.length === 0 && (
          <Text style={{ textAlign: "center", padding: tokens.spacingVerticalXL, color: tokens.colorNeutralForeground3 }}>
            No runs found for the selected filter.
          </Text>
        )}
      </div>
    </div>
  );
}
