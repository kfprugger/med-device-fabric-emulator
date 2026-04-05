import { useEffect, useState, useCallback } from "react";
import {
  Badge,
  Button,
  Card,
  CardHeader,
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
import {
  listMockTeardowns,
  getMockTeardownInstance,
  type TeardownInstance,
  type TeardownStep,
  type PhaseLog,
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
    color: "white",
    boxShadow: `0 0 0 3px ${tokens.colorPaletteRedBackground1}, ${tokens.shadow4}`,
  },
  dotDeleted: {
    backgroundColor: tokens.colorPaletteRedForeground1,
    color: "white",
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
  logSuccess: { color: tokens.colorPaletteRedForeground1 },
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

function dotClass(styles: ReturnType<typeof useStyles>, status: TeardownStep["status"]) {
  switch (status) {
    case "deleted": return styles.dotDeleted;
    case "running": return styles.dotRunning;
    case "skipped": return styles.dotSkipped;
    default: return styles.dotPending;
  }
}

function dotContent(status: TeardownStep["status"]) {
  switch (status) {
    case "deleted": return "✗";
    case "running": return <span style={{ width: 8, height: 8, borderRadius: "50%", backgroundColor: "currentColor", display: "block" }} />;
    case "skipped": return "—";
    default: return "";
  }
}

function TeardownInstanceCard({ instance, onDismiss }: { instance: TeardownInstance; onDismiss?: () => void }) {
  const styles = useStyles();

  const deletedCount = instance.steps.filter((s) => s.status === "deleted").length;
  const total = instance.steps.length;
  // Progress fills from RIGHT to LEFT (reverse)
  const progressPct = total > 0 ? (deletedCount / total) * 94 : 0;
  const progressColor = instance.status === "completed"
    ? tokens.colorPaletteRedForeground1
    : tokens.colorPaletteRedBackground3;

  // All logs combined for the expandable panel
  const allLogs: PhaseLog[] = instance.steps.flatMap((s) => s.logs);

  // Position milestones evenly across the track (reversed order visually)
  const stepPositions = instance.steps.map((_, i) =>
    3 + ((i + 0.5) / total) * 94
  );

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
            ? <DismissCircleFilled color={tokens.colorPaletteRedForeground1} fontSize={24} />
            : instance.status === "running"
              ? <ArrowSyncCircleRegular color={tokens.colorPaletteRedForeground1} fontSize={24} />
              : <CheckmarkCircleFilled color={tokens.colorPaletteGreenForeground1} fontSize={24} />
        }
        header={
          <div className={styles.headerRow}>
            {typeLabel}
            <Text weight="bold" size={400}>{instance.candidateName}</Text>
            {statusBadge}
          </div>
        }
      />

      {/* Reverse milestone track */}
      <div className={styles.milestoneTrack}>
        <div className={styles.trackLine} />
        <div
          className={styles.trackFill}
          style={{
            width: `${progressPct}%`,
            backgroundColor: progressColor,
          }}
        />
        {instance.steps.map((step, i) => (
          <div
            key={step.name}
            className={styles.milestoneContainer}
            style={{ left: `${stepPositions[i]}%` }}
          >
            <div className={`${styles.milestoneDot} ${dotClass(styles, step.status)}`}>
              {dotContent(step.status)}
            </div>
            <span className={`${styles.milestoneLabel} ${
              step.status === "running" ? styles.labelActive : ""
            }`}>
              {step.name}
            </span>
          </div>
        ))}
      </div>

      {/* Status summary */}
      <div className={styles.statusRow}>
        <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>
          {deletedCount}/{total} resources deleted
        </Text>
        <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalS }}>
          <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>
            {instance.status === "completed" ? "Teardown complete" : "In progress…"}
          </Text>
          {instance.status === "completed" && onDismiss && (
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
          )}
        </div>
      </div>

      {/* Log stream */}
      {allLogs.length > 0 && (
        <div className={styles.logPanel}>
          {allLogs.map((log, i) => (
            <div key={i}>
              <span style={{ color: tokens.colorNeutralForeground4, marginRight: 8 }}>
                {new Date(log.timestamp).toLocaleTimeString("en-US", { hour12: false })}
              </span>
              <span className={log.level === "success" ? styles.logSuccess : styles.logInfo}>
                {log.level === "success" ? "✗" : "›"} {log.message}
              </span>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}

export function TeardownMonitor() {
  const styles = useStyles();
  const [instances, setInstances] = useState<TeardownInstance[]>([]);
  const [dismissedIds, setDismissedIds] = useState<Set<string>>(new Set());

  // Load dismissed from backend on mount
  useEffect(() => {
    fetch("/api/dismissed-teardowns")
      .then((r) => r.json())
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
      fetch(`/api/dismissed-teardowns/${encodeURIComponent(id)}`, { method: "POST" }).catch(() => {});
      localStorage.setItem("teardown-dismissed", JSON.stringify([...next]));
      return next;
    });
  };

  const poll = useCallback(() => {
    const all = listMockTeardowns();
    const updated = all.map((inst) => getMockTeardownInstance(inst.instanceId) ?? inst);
    setInstances(updated);
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
        <Title2>Teardown Monitor</Title2>
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
