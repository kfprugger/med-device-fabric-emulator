import { useEffect, useState, useCallback } from "react";
import {
  Badge,
  Button,
  Card,
  CardHeader,
  Checkbox,
  makeStyles,
  Text,
  Title2,
  Tooltip,
  tokens,
} from "@fluentui/react-components";
import {
  DismissCircleFilled,
  DismissRegular,
  CheckmarkCircleFilled,
  ArrowSyncCircleRegular,
} from "@fluentui/react-icons";
import { typeBadge } from "../components/TypeBadges";
import { requestJson, requestVoid, listDeployments } from "../api";
import {
  listMockTeardowns,
  getMockTeardownInstance,
  type TeardownInstance,
} from "../mockDeployment";

const TRACK_HEIGHT = 6;
const DOT_SIZE = 22;
const DOT_BORDER = 3;
const DOT_TOTAL = DOT_SIZE + DOT_BORDER * 2;
const TRACK_CENTER = 32;
const TRACK_TOP = TRACK_CENTER - TRACK_HEIGHT / 2;
const DOT_TOP = TRACK_CENTER - DOT_TOTAL / 2;

const useStyles = makeStyles({
  container: {
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalXL,
  },
  instanceCard: {
    marginBottom: tokens.spacingVerticalL,
  },
  milestoneTrack: {
    position: "relative" as const,
    height: "85px",
    margin: `${tokens.spacingVerticalM} ${tokens.spacingHorizontalL}`,
  },
  trackLine: {
    position: "absolute" as const,
    top: `${TRACK_TOP}px`,
    left: "3%",
    right: "3%",
    height: `${TRACK_HEIGHT}px`,
    borderRadius: "3px",
    backgroundColor: tokens.colorNeutralStroke2,
    zIndex: 0,
  },
  trackFill: {
    position: "absolute" as const,
    top: `${TRACK_TOP}px`,
    right: "3%",
    height: `${TRACK_HEIGHT}px`,
    borderRadius: "3px",
    transition: "width 0.6s ease",
    zIndex: 1,
    filter: "drop-shadow(0 0 6px currentColor)",
  },
  milestoneContainer: {
    position: "absolute" as const,
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    zIndex: 2,
    transform: "translateX(-50%)",
    top: `${DOT_TOP}px`,
  },
  milestoneDot: {
    width: `${DOT_TOTAL}px`,
    height: `${DOT_TOTAL}px`,
    boxSizing: "border-box",
    borderRadius: "50%",
    border: `${DOT_BORDER}px solid ${tokens.colorNeutralBackground1}`,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontSize: "13px",
    fontWeight: tokens.fontWeightBold,
    transition: "all 0.3s ease",
    boxShadow: tokens.shadow2,
  },
  dotPending: {
    backgroundColor: tokens.colorNeutralStroke2,
    color: tokens.colorNeutralForeground4,
  },
  dotRunning: {
    backgroundColor: tokens.colorPaletteRedForeground1,
    color: tokens.colorNeutralForegroundOnBrand,
    boxShadow: `0 0 0 3px ${tokens.colorPaletteRedBackground1}, ${tokens.shadow4}`,
  },
  dotDeleted: {
    backgroundColor: tokens.colorPaletteRedForeground1,
    color: tokens.colorNeutralForegroundOnBrand,
  },
  dotSkipped: {
    backgroundColor: tokens.colorNeutralStroke2,
    color: tokens.colorNeutralForeground4,
  },
  milestoneLabel: {
    marginTop: tokens.spacingVerticalS,
    fontSize: tokens.fontSizeBase200,
    color: tokens.colorNeutralForeground3,
    textAlign: "center" as const,
    whiteSpace: "normal" as const,
    maxWidth: "100px",
    lineHeight: tokens.lineHeightBase200,
  },
  labelActive: {
    color: tokens.colorPaletteRedForeground1,
    fontWeight: tokens.fontWeightSemibold,
  },
  logPanel: {
    maxHeight: "200px",
    overflowY: "auto" as const,
    padding: `${tokens.spacingVerticalS} ${tokens.spacingHorizontalL}`,
    backgroundColor: tokens.colorNeutralBackground3,
    borderTop: `1px solid ${tokens.colorNeutralStroke2}`,
    fontFamily: "'Cascadia Code', 'Consolas', monospace",
    fontSize: tokens.fontSizeBase200,
    lineHeight: "1.6",
  },
  logInfo: { color: tokens.colorNeutralForeground2 },
  logSuccess: { color: tokens.colorPaletteGreenForeground1 },
  statusRow: {
    display: "flex",
    justifyContent: "space-between",
    padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalL}`,
    borderTop: `1px solid ${tokens.colorNeutralStroke2}`,
  },
  emptyState: {
    padding: tokens.spacingVerticalXXL,
    textAlign: "center" as const,
    color: tokens.colorNeutralForeground3,
  },
  headerRow: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalM,
  },
});

function TeardownInstanceCard({ instance, onDismiss }: { instance: TeardownInstance; onDismiss?: () => void }) {
  const styles = useStyles();

  const deletedCount = instance.steps.filter((s) => s.status === "deleted").length;
  const total = instance.steps.length;
  const progressPct = total > 0 ? deletedCount / total : 0;
  
  const typeLabel = typeBadge(instance.candidateType);

  const statusBadge = instance.status === "completed"
    ? <Badge color="danger">Deleted</Badge>
    : instance.status === "failed"
      ? <Badge color="warning">Failed</Badge>
      : <Badge color="danger" appearance="outline">Deleting…</Badge>;

  return (
    <Card className={styles.instanceCard}>
      <CardHeader
        image={
          instance.status === "completed"
            ? <CheckmarkCircleFilled color={tokens.colorPaletteGreenForeground1} fontSize={24} />
            : instance.status === "running"
              ? <ArrowSyncCircleRegular color={tokens.colorPaletteRedForeground1} fontSize={24} />
              : <DismissCircleFilled color={tokens.colorPaletteRedForeground1} fontSize={24} />
        }
        header={
          <div className={styles.headerRow}>
            {typeLabel}
            <Text weight="bold" size={400}>{instance.candidateName}</Text>
            {statusBadge}
          </div>
        }
        action={
          instance.status === "completed" && onDismiss ? (
            <Tooltip content="Dismiss and archive this teardown" relationship="label">
              <Button
                appearance="subtle"
                icon={<DismissRegular />}
                size="small"
                onClick={onDismiss}
              >
                Dismiss
              </Button>
            </Tooltip>
          ) : undefined
        }
      />

      {instance.status === "running" && (
        <div style={{ padding: `0 ${tokens.spacingHorizontalM}` }}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: tokens.spacingVerticalXS }}>
             <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>Teardown Progress</Text>
             <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>{deletedCount} / {total} phases</Text>
          </div>
          <div style={{ width: "100%", height: "6px", backgroundColor: tokens.colorNeutralBackground3, borderRadius: "3px", overflow: "hidden" }}>
             <div style={{ width: `${progressPct * 100}%`, height: "100%", backgroundColor: tokens.colorPaletteRedForeground1, transition: "width 0.3s ease" }} />
          </div>
        </div>
      )}
    </Card>
  );
}

export function TeardownMonitor() {
  const styles = useStyles();
  const [instances, setInstances] = useState<TeardownInstance[]>([]);
  const [dismissedIds, setDismissedIds] = useState<Set<string>>(new Set());
  const [operatorMode, setOperatorMode] = useState(false);

  // Load dismissed from backend on mount
  useEffect(() => {
    requestJson<string[]>("/api/dismissed-teardowns", { timeoutMs: 5000, retry: 1 })
      .then((ids: string[]) => {
        if (ids.length > 0) setDismissedIds(new Set(ids));
      })
      .catch(() => {
        try {
          const saved = localStorage.getItem("teardown-dismissed");
          if (saved) setDismissedIds(new Set(JSON.parse(saved)));
        } catch { /* ignore */ }
      });
  }, []);

  const dismiss = (id: string) => {
    setDismissedIds((prev) => {
      const next = new Set(prev);
      next.add(id);
      // Persist to backend
      requestVoid(`/api/dismissed-teardowns/${encodeURIComponent(id)}`, { method: "POST", timeoutMs: 5000 }).catch(() => {});
      localStorage.setItem("teardown-dismissed", JSON.stringify([...next]));
      return next;
    });
  };

  const poll = useCallback(() => {
    // 1. Gather mock teardowns
    const allMocks = listMockTeardowns();
    const mockInstances = allMocks.map((inst) => getMockTeardownInstance(inst.instanceId) ?? inst);

    // 2. Fetch real teardowns from the backend
    listDeployments()
      .then((realDeployments) => {
        const realTeardowns = realDeployments.filter((d) => {
          const cs = d.customStatus;
          return (
            (cs?.runType as string) === "teardown" ||
            d.name === "teardown_orchestrator" ||
            d.instanceId.toLowerCase().startsWith("teardown")
          );
        });

        const mappedReal: TeardownInstance[] = realTeardowns.map((d) => {
          const cs = d.customStatus || {};
          const logs = (cs.logs as Array<{ level: "info" | "warn" | "error" | "success"; message: string }>) || [];
          const outputPhases = (d as any).output?.phases || [];

          // Map backend runtimeStatus to teardown instance status
          let status: "running" | "completed" | "failed" = "running";
          if (d.runtimeStatus === "Completed") status = "completed";
          else if (d.runtimeStatus === "Failed" || d.runtimeStatus === "Terminated") status = "failed";

          // Map candidate type
          let candidateType: "fabric" | "azure" | "spn" = "azure";
          if (d.instanceId.toLowerCase().includes("fabric")) candidateType = "fabric";
          else if (d.instanceId.toLowerCase().includes("spn")) candidateType = "spn";

          return {
            instanceId: d.instanceId,
            candidateName: (cs.displayName as string) || d.instanceId,
            candidateType,
            status,
            steps: outputPhases.map((p: any) => ({
              name: p.phase,
              status: p.status === "succeeded" ? "deleted" : p.status === "running" ? "running" : p.status === "skipped" ? "skipped" : "pending",
              logs: logs.map((l) => ({
                timestamp: d.lastUpdatedTime || new Date().toISOString(),
                level: l.level || "info",
                message: l.message
              }))
            })),
            startedAt: d.createdTime || new Date().toISOString()
          };
        });

        // Combine both real and mock instances
        setInstances([...mappedReal, ...mockInstances]);
      })
      .catch(() => {
        // Fall back to just mock instances if API is down
        setInstances(mockInstances);
      });
  }, []);

  useEffect(() => {
    poll();
    const interval = setInterval(poll, 500);
    return () => clearInterval(interval);
  }, [poll]);

  const visible = instances.filter((i) => !dismissedIds.has(i.instanceId));
  const activeCount = visible.filter((i) => i.status === "running").length;
  const completedCount = visible.filter((i) => i.status === "completed").length;

  return (
    <div className={styles.container}>
      <div>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: tokens.spacingHorizontalM }}>
          <Title2>Teardown Monitor</Title2>
          <Checkbox checked={operatorMode} onChange={(_, d) => setOperatorMode(!!d.checked)} label="Operator mode" />
        </div>
        <Text size={200} block style={{ marginTop: tokens.spacingVerticalXS }}>
          {visible.length === 0
            ? "No teardowns in progress. Start one from the Teardown tab."
            : `${activeCount} active, ${completedCount} completed`}
        </Text>
      </div>

      {visible.length === 0 && (
        <div className={styles.emptyState}>
          <Text>Select resources on the Teardown tab and click Delete to start teardown.</Text>
        </div>
      )}

      {visible.map((inst) => (
        <TeardownInstanceCard
          key={inst.instanceId}
          instance={inst}
          onDismiss={() => dismiss(inst.instanceId)}
        />
      ))}
    </div>
  );
}
