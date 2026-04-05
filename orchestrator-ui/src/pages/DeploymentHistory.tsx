import { useEffect, useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import {
  Badge,
  Button,
  makeStyles,
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
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());

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
      });
  }, []);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  }, [refresh]);

  const handleClearAll = async () => {
    try { await clearAllDeployments(); } catch { /* ignore */ }
    refresh();
  };

  const handleDelete = async (instanceId: string) => {
    try { await deleteDeployment(instanceId); } catch { /* ignore */ }
    refresh();
  };

  return (
    <div>
      <div className={styles.header}>
        {usingMock && <MockDataBanner />}
        <Title2>Deployment History</Title2>
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

      <div className={styles.list}>
        {deployments.map((d) => {
          const cs = d.customStatus as Record<string, unknown> | null;
          const workspace = (cs?.workspaceName as string) || "—";
          const rgName = (cs?.resourceGroupName as string) || "";
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
                <Text className={styles.workspace}>{workspace}</Text>
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
        {deployments.length === 0 && (
          <Text style={{ textAlign: "center", padding: tokens.spacingVerticalXL, color: tokens.colorNeutralForeground3 }}>
            No deployments yet.
          </Text>
        )}
      </div>
    </div>
  );
}
