import { useEffect, useState, useCallback } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  Button,
  Card,
  CardHeader,
  MessageBar,
  MessageBarBody,
  Subtitle1,
  Text,
  Title2,
  Tooltip,
  Badge,
  makeStyles,
  tokens,
} from "@fluentui/react-components";
import {
  PlayRegular,
  DismissRegular,
  ArrowDownRegular,
  PauseRegular,
  ChevronDownRegular,
  ChevronUpRegular,
  OpenRegular,
  ArrowRepeatAllRegular,
} from "@fluentui/react-icons";
import { PhaseCard } from "../components/PhaseCard";
import {
  getDeploymentStatus,
  resumeAfterHds,
  cancelDeployment,
  startDeployment,
  getDeployedResources,
  type DeploymentStatus,
  type DeploymentConfig,
  type PhaseInfo,
  type DeployedResourcesResult,
} from "../api";
import {
  isMockInstance,
  getMockStatus,
  getMockPhases,
  resumeMockHds,
  cancelMockDeployment,
  type PhaseLog,
} from "../mockDeployment";

const TRACK_HEIGHT = 6;
const DOT_SIZE = 22; // CSS width/height (excluding border)
const DOT_BORDER = 3;
const DOT_TOTAL = DOT_SIZE + DOT_BORDER * 2; // actual rendered size = 28px
const TRACK_CENTER = 32; // y-center of the track line in the track area
const TRACK_TOP = TRACK_CENTER - TRACK_HEIGHT / 2;
const DOT_TOP = TRACK_CENTER - DOT_TOTAL / 2; // vertically center dots on track

const useStyles = makeStyles({
  header: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: tokens.spacingVerticalS,
  },
  progressSection: {
    marginBottom: tokens.spacingVerticalXL,
    padding: `${tokens.spacingVerticalL} ${tokens.spacingHorizontalXL}`,
    backgroundColor: tokens.colorNeutralBackground1,
    borderRadius: tokens.borderRadiusLarge,
    boxShadow: `${tokens.shadow8}, 0 0 12px rgba(96, 233, 208, 0.25)`,
    border: `1px solid ${tokens.colorNeutralStroke1}`,
    position: "sticky" as const,
    top: "0",
    zIndex: 10,
  },
  milestoneTrack: {
    position: "relative" as const,
    height: "90px",
    marginTop: tokens.spacingVerticalS,
  },
  trackLine: {
    position: "absolute" as const,
    top: `${TRACK_TOP}px`,
    left: "4%",
    right: "4%",
    height: `${TRACK_HEIGHT}px`,
    borderRadius: "3px",
    backgroundColor: tokens.colorNeutralStroke2,
    zIndex: 0,
  },
  trackFill: {
    position: "absolute" as const,
    top: `${TRACK_TOP}px`,
    left: "4%",
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
  milestoneDotPending: {
    backgroundColor: tokens.colorNeutralStroke2,
    color: tokens.colorNeutralForeground4,
  },
  milestoneDotActive: {
    backgroundColor: tokens.colorBrandForeground1,
    color: tokens.colorNeutralForegroundOnBrand,
    boxShadow: `0 0 0 3px ${tokens.colorBrandBackground2}, ${tokens.shadow4}`,
  },
  milestoneDotDone: {
    backgroundColor: tokens.colorPaletteGreenForeground1,
    color: "white",
  },
  milestoneDotWaiting: {
    backgroundColor: tokens.colorPaletteYellowForeground1,
    color: tokens.colorNeutralForeground1,
    boxShadow: `0 0 0 3px ${tokens.colorPaletteYellowBackground1}, ${tokens.shadow4}`,
  },
  milestoneLabel: {
    marginTop: tokens.spacingVerticalS,
    fontSize: tokens.fontSizeBase300,
    color: tokens.colorNeutralForeground3,
    textAlign: "center" as const,
    whiteSpace: "normal" as const,
    maxWidth: "180px",
    lineHeight: tokens.lineHeightBase200,
    overflow: "hidden",
    textOverflow: "ellipsis",
  },
  milestoneLabelActive: {
    color: tokens.colorBrandForeground1,
    fontWeight: tokens.fontWeightSemibold,
  },
  milestoneCallout: {
    position: "absolute" as const,
    bottom: "100%",
    left: "50%",
    transform: "translateX(-50%)",
    marginBottom: "8px",
    padding: `${tokens.spacingVerticalXXS} ${tokens.spacingHorizontalM}`,
    backgroundColor: tokens.colorBrandForeground1,
    color: tokens.colorNeutralForegroundOnBrand,
    borderRadius: tokens.borderRadiusMedium,
    fontSize: tokens.fontSizeBase200,
    fontWeight: tokens.fontWeightSemibold,
    whiteSpace: "nowrap" as const,
    boxShadow: tokens.shadow8,
  },
  milestoneCalloutArrow: {
    position: "absolute" as const,
    top: "100%",
    left: "50%",
    transform: "translateX(-50%)",
    width: "0",
    height: "0",
    borderLeft: "6px solid transparent",
    borderRight: "6px solid transparent",
    borderTop: `6px solid ${tokens.colorBrandForeground1}`,
  },
  progressSummary: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginTop: tokens.spacingVerticalS,
    paddingTop: tokens.spacingVerticalXS,
    borderTop: `1px solid ${tokens.colorNeutralStroke2}`,
  },
  phases: {
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalXS,
  },
  hdsGate: {
    marginTop: tokens.spacingVerticalL,
    marginBottom: tokens.spacingVerticalL,
    padding: tokens.spacingHorizontalL,
    backgroundColor: tokens.colorStatusWarningBackground1,
    borderLeft: `4px solid ${tokens.colorStatusWarningBorderActive}`,
    borderRadius: tokens.borderRadiusMedium,
  },
  resources: {
    marginTop: tokens.spacingVerticalXXL,
  },
  resourceGrid: {
    display: "grid",
    gridTemplateColumns: "1fr 2fr",
    gap: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalM}`,
    fontSize: tokens.fontSizeBase200,
  },
  resourceSection: {
    marginBottom: tokens.spacingVerticalL,
  },
  resourceSectionHeader: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalS,
    marginBottom: tokens.spacingVerticalS,
  },
  resourceTable: {
    width: "100%",
    borderCollapse: "collapse" as const,
    fontSize: tokens.fontSizeBase200,
  },
  resourceRow: {
    borderBottom: `1px solid ${tokens.colorNeutralStroke2}`,
    ":hover": {
      backgroundColor: tokens.colorNeutralBackground1Hover,
    },
  },
  resourceCell: {
    padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalS}`,
    verticalAlign: "middle" as const,
  },
  resourceType: {
    color: tokens.colorNeutralForeground3,
    fontSize: tokens.fontSizeBase100,
  },
  resourceLoading: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalS,
    padding: tokens.spacingVerticalM,
    color: tokens.colorNeutralForeground3,
  },
  actions: {
    display: "flex",
    gap: tokens.spacingHorizontalS,
    marginTop: tokens.spacingVerticalL,
  },
  floatingScrollBtn: {
    position: "fixed" as const,
    right: "32px",
    bottom: "48px",
    zIndex: 20,
    boxShadow: tokens.shadow16,
  },
  floatingCancelBtn: {
    position: "fixed" as const,
    left: "32px",
    bottom: "48px",
    zIndex: 20,
    boxShadow: tokens.shadow16,
  },
});

const ALL_PHASES: PhaseInfo[] = [
  // Phase 1
  { phase: "Step 1: Fabric Workspace", status: "pending" },
  { phase: "Step 1b: Base Azure Infrastructure", status: "pending" },
  { phase: "Step 2: FHIR Service & Data Loading", status: "pending" },
  { phase: "Step 2b: DICOM Infrastructure & Loading", status: "pending" },
  { phase: "Step 3: Fabric RTI Phase 1", status: "pending" },
  { phase: "Step 4: HDS Detection", status: "pending" },
  // Phase 2
  { phase: "Step 5: Fabric RTI Phase 2", status: "pending" },
  { phase: "Step 5b: HDS Pipelines", status: "pending" },
  { phase: "Step 6: Data Agents", status: "pending" },
  // Phase 3
  { phase: "Step 7: Imaging Toolkit", status: "pending" },
  // Phase 4
  { phase: "Step 8: Ontology", status: "pending" },
  { phase: "Step 9: Data Activator", status: "pending" },
];

export function PhaseMonitor() {
  const styles = useStyles();
  const { instanceId } = useParams<{ instanceId: string }>();
  const navigate = useNavigate();
  const [status, setStatus] = useState<DeploymentStatus | null>(null);
  const [mockPhaseLogs, setMockPhaseLogs] = useState<
    Map<string, PhaseLog[]>
  >(new Map());
  const [error, setError] = useState("");
  const [autoScroll, setAutoScroll] = useState(true);
  const [redeploying, setRedeploying] = useState(false);
  const [deployedResources, setDeployedResources] = useState<DeployedResourcesResult | null>(null);
  const [resourcesLoading, setResourcesLoading] = useState(false);
  const [lastResourceFetch, setLastResourceFetch] = useState(0);
  const [frozenElapsed, setFrozenElapsed] = useState<number | null>(null);
  const [tick, setTick] = useState(0);
  const [fabricExpanded, setFabricExpanded] = useState(false);
  const [azureExpanded, setAzureExpanded] = useState(false);

  const isMock = instanceId ? isMockInstance(instanceId) : false;

  const poll = useCallback(async () => {
    if (!instanceId) return;
    try {
      if (isMock) {
        const s = getMockStatus(instanceId);
        if (s) setStatus(s);
        // Collect logs from mock phases
        const phases = getMockPhases(instanceId);
        const logMap = new Map<string, PhaseLog[]>();
        for (const p of phases) {
          logMap.set(p.phase, p.logs ?? []);
        }
        setMockPhaseLogs(logMap);
      } else {
        const s = await getDeploymentStatus(instanceId);
        setStatus(s);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to fetch status");
    }
  }, [instanceId, isMock]);

  useEffect(() => {
    poll();
    const interval = setInterval(poll, isMock ? 500 : 5000);
    return () => clearInterval(interval);
  }, [poll, isMock]);

  const isRunning = status?.runtimeStatus === "Running";
  const isWaitingForHds =
    status?.customStatus?.status === "waiting_for_input";
  const isCancelled = status?.runtimeStatus === "Terminated";
  const isFailed = status?.runtimeStatus === "Failed";
  const isComplete =
    status?.runtimeStatus === "Completed" ||
    isCancelled || isFailed;

  // Merge completed phases with the full phase list
  const completedPhases = status?.output?.phases ?? [];
  const currentPhase = status?.customStatus?.currentPhase ?? "";

  // For mock mode, use the mock phases directly (they have real-time status)
  // For real backend, use output.phases if available, otherwise ALL_PHASES
  let phases: PhaseInfo[];
  if (isMock && instanceId) {
    const mp = getMockPhases(instanceId);
    phases = mp.length > 0 ? mp : ALL_PHASES;
  } else if (completedPhases.length > 0) {
    // Real backend has reported phases — use them directly
    phases = completedPhases;
    // Add a "running" phase for the current step if deployment is still running
    if (isRunning && currentPhase && !completedPhases.find((p) => p.phase === currentPhase)) {
      phases = [...completedPhases, { phase: currentPhase, status: "running" }];
    }
  } else if (isRunning && currentPhase) {
    // Backend is running but hasn't parsed any steps yet — show current phase
    phases = [{ phase: currentPhase, status: "running" }];
  } else {
    phases = ALL_PHASES.map((p) => {
      const completed = completedPhases.find((cp) => cp.phase === p.phase);
      if (completed) return completed;
      if (p.phase === currentPhase || p.phase.includes(currentPhase))
        return {
          ...p,
          status: isWaitingForHds ? "waiting_for_input" : "running",
        };
      return p;
    });
  }

  // Get logs from backend customStatus.logs (for real deployments)
  const backendLogs = (status?.customStatus as Record<string, unknown>)?.logs as Array<{timestamp: string; level: string; message: string}> | undefined;

  // Compute elapsed time — freeze when deployment is no longer running
  useEffect(() => {
    if (isRunning && !isWaitingForHds) {
      const t = setInterval(() => setTick((v) => v + 1), 1000);
      return () => clearInterval(t);
    }
  }, [isRunning, isWaitingForHds]);

  useEffect(() => {
    if (!isRunning && status && frozenElapsed === null) {
      // Priority 1: backend-computed durationSeconds
      const backendDuration = (status.customStatus as Record<string, unknown>)?.durationSeconds;
      if (typeof backendDuration === "number" && backendDuration > 0) {
        setFrozenElapsed(backendDuration);
        return;
      }
      // Priority 2: sum of phase durations
      const phaseDurationSum = phases.reduce((sum, p) => {
        return sum + (typeof p.duration === "number" ? p.duration : 0);
      }, 0);
      if (phaseDurationSum > 0) {
        setFrozenElapsed(phaseDurationSum);
        return;
      }
      // Priority 3: lastUpdatedTime - createdTime
      if (status.createdTime && status.lastUpdatedTime) {
        const created = new Date(status.createdTime).getTime();
        const updated = new Date(status.lastUpdatedTime).getTime();
        setFrozenElapsed(Math.max(0, (updated - created) / 1000));
      }
    }
  }, [isRunning, status, frozenElapsed, phases]);

  // Fetch deployed resources from Azure/Fabric APIs when phases complete
  useEffect(() => {
    if (!instanceId || isMock) return;
    const completedNow = phases.filter(
      (p) => p.status === "succeeded" || p.status === "skipped"
    ).length;
    const shouldFetch = completedNow > 0 || isCancelled || isFailed;
    if (!shouldFetch) return;

    // Re-fetch when a new phase completes or on terminal state
    const key = isCancelled || isFailed || !isRunning ? -1 : completedNow;
    if (key === lastResourceFetch && deployedResources) return;

    setResourcesLoading(true);
    getDeployedResources(instanceId)
      .then((res) => {
        setDeployedResources(res);
        setLastResourceFetch(key);
      })
      .catch(() => {}) // Silently ignore — non-critical
      .finally(() => setResourcesLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [instanceId, isMock, phases.length, isCancelled, isFailed, isRunning]);

  const elapsedSeconds = frozenElapsed !== null
    ? frozenElapsed
    : status?.createdTime
      ? (Date.now() - new Date(status.createdTime).getTime()) / 1000
      : 0;
  void tick; // suppress unused warning
  const elapsedFormatted = elapsedSeconds > 0
    ? `${Math.floor(elapsedSeconds / 60)}m ${Math.floor(elapsedSeconds % 60)}s`
    : "";

  const handleResume = async () => {
    if (!instanceId) return;
    try {
      if (isMock) {
        resumeMockHds(instanceId);
      } else {
        await resumeAfterHds(instanceId);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to resume");
    }
  };

  const handleCancel = async () => {
    if (!instanceId) return;
    try {
      if (isMock) {
        cancelMockDeployment(instanceId);
      } else {
        await cancelDeployment(instanceId);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to cancel");
    }
  };

  const completedCount = phases.filter(
    (p) => p.status === "succeeded" || p.status === "skipped"
  ).length;

  // ── Weighted progress based on typical step durations (minutes) ──
  // Each step gets a weight proportional to how long it typically takes.
  // This gives accurate progress bar fill instead of equal step weighting.
  const STEP_WEIGHTS: Array<{ patterns: string[]; weight: number }> = [
    // Phase 1 — ~40 min total
    { patterns: ["Fabric Workspace"], weight: 1 },
    { patterns: ["Azure Infrastructure"], weight: 10 },
    { patterns: ["FHIR"], weight: 15 },
    { patterns: ["DICOM"], weight: 8 },
    { patterns: ["Fabric RTI"], weight: 5 },
    { patterns: ["HDS Detection"], weight: 1 },
    // Phase 2 — ~20 min total
    { patterns: ["RTI Phase 2"], weight: 5 },
    { patterns: ["HDS Pipeline"], weight: 10 },
    { patterns: ["Data Agent"], weight: 5 },
    // Phase 3 — ~10 min total
    { patterns: ["Imaging", "Cohorting", "DICOM Viewer"], weight: 10 },
    // Phase 4 — ~10 min total
    { patterns: ["Ontology"], weight: 5 },
    { patterns: ["Activator", "Reflex"], weight: 5 },
  ];
  const TOTAL_WEIGHT = STEP_WEIGHTS.reduce((s, w) => s + w.weight, 0); // 80

  function getStepWeight(phaseName: string): number {
    for (const sw of STEP_WEIGHTS) {
      if (sw.patterns.some((pat) => phaseName.toUpperCase().includes(pat.toUpperCase()))) {
        return sw.weight;
      }
    }
    return 1; // Unknown step gets minimal weight
  }

  // Compute weighted progress
  let weightedCompleted = 0;
  let weightedRunning = 0;
  for (const p of phases) {
    const w = getStepWeight(p.phase);
    if (p.status === "succeeded" || p.status === "skipped") {
      weightedCompleted += w;
    } else if (p.status === "running") {
      weightedRunning += w * 0.3; // 30% credit for in-progress step
    }
  }

  // Map weighted progress to visual bar position using piecewise-linear interpolation
  // Weight thresholds → visual positions: 0→0, 40→25, 60→50, 70→75, 80→96
  function weightToVisualPct(w: number): number {
    const segments = [
      { wStart: 0, wEnd: 40, vStart: 0, vEnd: 25 },
      { wStart: 40, wEnd: 60, vStart: 25, vEnd: 50 },
      { wStart: 60, wEnd: 70, vStart: 50, vEnd: 75 },
      { wStart: 70, wEnd: 80, vStart: 75, vEnd: 92 },
    ];
    for (const seg of segments) {
      if (w <= seg.wEnd) {
        const t = (w - seg.wStart) / (seg.wEnd - seg.wStart);
        return seg.vStart + t * (seg.vEnd - seg.vStart);
      }
    }
    return 92;
  }

  const progressPct = isComplete
    ? 100
    : TOTAL_WEIGHT > 0
      ? weightToVisualPct(weightedCompleted + weightedRunning)
      : 0;

  const progressColor = (isCancelled || isFailed)
    ? tokens.colorPaletteRedForeground1
    : isComplete
      ? tokens.colorPaletteGreenForeground1
      : isWaitingForHds
      ? tokens.colorPaletteYellowForeground1
      : tokens.colorBrandForeground1;

  // Milestone positions evenly spaced for visual clarity, with weight thresholds for progress
  // Visual positions: 25%, 50%, 75%, 96%
  // Weight thresholds: when cumulative weight reaches endWeight, that milestone is done
  const MILESTONES = [
    { label: "Phase 1: Infra & Ingestion", phaseIndices: [0, 1, 2, 3, 4, 5], namePatterns: ["Fabric Workspace", "Azure Infrastructure", "FHIR", "DICOM", "Fabric RTI", "HDS Detection"], position: 25, endWeight: 40 },
    { label: "Phase 2: Enrichment & Agents", phaseIndices: [6, 7, 8], namePatterns: ["RTI Phase 2", "HDS Pipeline", "Data Agent"], position: 50, endWeight: 60 },
    { label: "Phase 3: Imaging Toolkit", phaseIndices: [9], namePatterns: ["Imaging", "Cohorting", "DICOM Viewer"], position: 75, endWeight: 70 },
    { label: "Phase 4: Ontology & Activator", phaseIndices: [10, 11], namePatterns: ["Ontology", "Activator", "Reflex"], position: 92, endWeight: 80 },
  ];

  function getMilestoneStatus(ms: typeof MILESTONES[0]): "done" | "active" | "waiting" | "pending" | "cancelled" {
    if (isMock) {
      // Mock mode: use array indices
      const relevantPhases = ms.phaseIndices.map((i) => phases[i]).filter(Boolean);
      if (relevantPhases.length === 0) return "pending";
      const allDone = relevantPhases.every((p) => p.status === "succeeded" || p.status === "skipped");
      if (allDone) return "done";
      const hasWaiting = relevantPhases.some((p) => p.status === "waiting_for_input");
      if (hasWaiting) return "waiting";
      return "pending";
    }

    // Real mode: a milestone is "done" only when the progress bar has passed its position
    // This prevents lighting up a milestone while still executing steps within it
    const doneWeight = weightedCompleted;
    if (doneWeight >= ms.endWeight) return "done";

    // Check for HDS waiting gate
    const matchedPhases = phases.filter((p) =>
      ms.namePatterns.some((pat) => p.phase.toUpperCase().includes(pat.toUpperCase()))
    );
    const hasWaiting = matchedPhases.some((p) => p.status === "waiting_for_input");
    if (hasWaiting) return "waiting";

    // If cancelled/failed and we haven't reached this milestone
    if ((isCancelled || isFailed) && doneWeight < ms.endWeight) {
      // If any steps in this milestone ran, show as cancelled; otherwise pending
      return matchedPhases.length > 0 ? "cancelled" : "pending";
    }

    return "pending";
  }

  function getDotClass(status: string) {
    switch (status) {
      case "done": return styles.milestoneDotDone;
      case "active": return styles.milestoneDotActive;
      case "waiting": return styles.milestoneDotWaiting;
      case "cancelled": return styles.milestoneDotWaiting;  // Reuse yellow for now
      default: return styles.milestoneDotPending;
    }
  }

  function getDotContent(status: string) {
    switch (status) {
      case "done": return "✓";
      case "active": return <span style={{ width: 8, height: 8, borderRadius: "50%", backgroundColor: "currentColor", display: "block" }} />;
      case "waiting": return "⏸";
      default: return "";
    }
  }
  // Compute milestone-level counts for the pill (4 phases, not 12 steps)
  const milestoneStatuses = MILESTONES.map((ms) => getMilestoneStatus(ms));
  const milestonesDone = milestoneStatuses.filter((s) => s === "done").length;
  const totalMilestones = MILESTONES.length;
  return (
    <div>
      <div className={styles.header}>
        <div>
          <Title2>Deployment Monitor</Title2>
          <Text size={200} block>
            Instance: {instanceId}
            {isMock && (
              <Badge color="informative" style={{ marginLeft: 8 }}>
                Mock Mode
              </Badge>
            )}
          </Text>
        </div>

        <div className={styles.actions}>
          <Badge
            color={isCancelled ? "warning" : isFailed ? "danger" : isComplete ? "success" : isRunning ? "informative" : "subtle"}
            size="large"
          >
            {milestonesDone}/{totalMilestones} phases{" "}
            {isCancelled ? "cancelled" : isComplete ? "complete" : ""}{" "}
            {elapsedFormatted && `(${elapsedFormatted})`}
          </Badge>
          {isRunning && (
            <>
              <Tooltip
                content={autoScroll ? "Disable auto-scroll to bottom" : "Enable auto-scroll to bottom"}
                relationship="label"
              >
                <Button
                  appearance={autoScroll ? "subtle" : "outline"}
                  icon={autoScroll ? <PauseRegular /> : <ArrowDownRegular />}
                  onClick={() => setAutoScroll((v) => !v)}
                >
                  {autoScroll ? "Auto-scroll On" : "Auto-scroll Off"}
                </Button>
              </Tooltip>
              <Button
                appearance="subtle"
                icon={<DismissRegular />}
                onClick={handleCancel}
              >
                Cancel
              </Button>
            </>
          )}
        </div>
      </div>

      {/* Milestone progress track */}
      <div className={styles.progressSection}>
        <div className={styles.milestoneTrack}>
          {/* Background track line */}
          <div className={styles.trackLine} />
          {/* Filled track line — scaled to the 92% inner track (4% to 96%) */}
          <div
            className={styles.trackFill}
            style={{
              width: `${(progressPct / 100) * 92}%`,
              backgroundColor: progressColor,
            }}
          />
          {/* Milestone nodes */}
          {MILESTONES.map((ms) => {
            const msStatus = getMilestoneStatus(ms);
            // Map position to inner track: track spans 4% to 96% (92% width)
            const trackLeft = 4 + (ms.position / 100) * 92;
            return (
              <div
                key={ms.label}
                className={styles.milestoneContainer}
                style={{ left: `${trackLeft}%` }}
              >
                {/* Callout label above done milestone */}
                {msStatus === "done" && (
                  <div className={styles.milestoneCallout}>
                    {ms.label}
                    <div className={styles.milestoneCalloutArrow} />
                  </div>
                )}
                {/* Dot */}
                <div
                  className={`${styles.milestoneDot} ${getDotClass(msStatus)}`}
                >
                  {getDotContent(msStatus)}
                </div>
                {/* Label below */}
                <span
                  className={`${styles.milestoneLabel} ${
                    msStatus === "done" || msStatus === "waiting"
                      ? styles.milestoneLabelActive
                      : ""
                  }`}
                >
                  {ms.label}
                </span>
              </div>
            );
          })}
        </div>
        <div className={styles.progressSummary}>
          <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>
            {milestonesDone}/{totalMilestones} phases
            {isComplete ? " complete" : ""}
            {isRunning && currentPhase ? ` · ${currentPhase}` : ""}
          </Text>
          <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>
            {elapsedFormatted || "0m 0s"}
          </Text>
        </div>
      </div>

      {error && (
        <MessageBar intent="error">
          <MessageBarBody>{error}</MessageBarBody>
        </MessageBar>
      )}

      {/* HDS Manual Step Gate */}
      {isWaitingForHds && (
        <div className={styles.hdsGate}>
          <Subtitle1>Action Required: Deploy HDS</Subtitle1>
          <Text block>
            {status?.customStatus?.detail ||
              "Deploy Healthcare Data Solutions (HDS) in the Fabric portal, install scipy in the environment, run pipelines, then click Continue."}
          </Text>
          <Button
            appearance="primary"
            icon={<PlayRegular />}
            onClick={handleResume}
            style={{ marginTop: tokens.spacingVerticalM }}
          >
            Continue — HDS is deployed
          </Button>
        </div>
      )}

      {/* Phase Cards with logs */}
      <div className={styles.phases}>
        {phases.map((phase) => (
          <PhaseCard
            key={phase.phase}
            phase={phase}
            logs={
              isMock
                ? (mockPhaseLogs.get(phase.phase) ?? [])
                : ((backendLogs ?? []) as Array<{timestamp: string; level: "info" | "warn" | "error" | "success"; message: string}>)
            }
            autoScroll={autoScroll}
          />
        ))}
      </div>

      {/* Deployed Resources */}
      {(isComplete || completedCount > 0) && !isMock && (
        <Card className={styles.resources}>
          <CardHeader header={<Subtitle1>Deployed Resources</Subtitle1>} />

          {resourcesLoading && !deployedResources && (
            <div className={styles.resourceLoading}>
              <span style={{ display: "inline-block" }}>⟳</span>
              <Text size={200}>Scanning Azure &amp; Fabric APIs…</Text>
            </div>
          )}

          {!resourcesLoading && !deployedResources && (
            <Text size={200} style={{ color: tokens.colorNeutralForeground3, padding: tokens.spacingVerticalS, display: "block" }}>
              Restart the backend server to enable live resource scanning.
            </Text>
          )}

          {deployedResources && (
            <>
              {/* Fabric Workspace */}
              {deployedResources.workspace && (
                <div className={styles.resourceSection}>
                  <div
                    className={styles.resourceSectionHeader}
                    onClick={() => setFabricExpanded((v) => !v)}
                    style={{ cursor: "pointer", userSelect: "none" }}
                  >
                    <Badge color="brand" size="small">Fabric</Badge>
                    <Text weight="semibold" size={300}>
                      Workspace: {deployedResources.workspace.name}
                    </Text>
                    <Badge color="subtle" size="small">{deployedResources.fabric.length} items</Badge>
                    <Button
                      as="a"
                      appearance="subtle"
                      icon={<OpenRegular />}
                      size="small"
                      href={deployedResources.workspace.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      onClick={(e: React.MouseEvent) => e.stopPropagation()}
                    >
                      Open in Fabric
                    </Button>
                    <Button
                      appearance="subtle"
                      icon={fabricExpanded ? <ChevronUpRegular /> : <ChevronDownRegular />}
                      size="small"
                      onClick={(e) => { e.stopPropagation(); setFabricExpanded((v) => !v); }}
                      style={{ marginLeft: "auto" }}
                    />
                  </div>
                  {fabricExpanded && deployedResources.fabric.length > 0 && (
                    <table className={styles.resourceTable}>
                      <thead>
                        <tr>
                          <th className={styles.resourceCell} style={{ textAlign: "left", color: tokens.colorNeutralForeground3 }}>Name</th>
                          <th className={styles.resourceCell} style={{ textAlign: "left", color: tokens.colorNeutralForeground3 }}>Type</th>
                        </tr>
                      </thead>
                      <tbody>
                        {deployedResources.fabric.map((item) => (
                          <tr key={item.id} className={styles.resourceRow}>
                            <td className={styles.resourceCell}>
                              <Text size={200}>{item.name}</Text>
                            </td>
                            <td className={styles.resourceCell}>
                              <span className={styles.resourceType}>{item.type}</span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                  {fabricExpanded && deployedResources.fabric.length === 0 && (
                    <Text size={200} style={{ color: tokens.colorNeutralForeground3, padding: tokens.spacingVerticalS }}>
                      No Fabric items found yet
                    </Text>
                  )}
                </div>
              )}

              {/* Azure Resources */}
              {deployedResources.azure.length > 0 && (
                <div className={styles.resourceSection}>
                  <div
                    className={styles.resourceSectionHeader}
                    onClick={() => setAzureExpanded((v) => !v)}
                    style={{ cursor: "pointer", userSelect: "none" }}
                  >
                    <Badge color="informative" size="small">Azure</Badge>
                    <Text weight="semibold" size={300}>
                      Resource Group: {status?.customStatus?.resourceGroupName || ""}
                    </Text>
                    <Badge color="subtle" size="small">{deployedResources.azure.length} resources</Badge>
                    <Button
                      as="a"
                      appearance="subtle"
                      icon={<OpenRegular />}
                      size="small"
                      href={`https://portal.azure.com/#@/resource/subscriptions//resourceGroups/${status?.customStatus?.resourceGroupName || ""}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      onClick={(e: React.MouseEvent) => e.stopPropagation()}
                    >
                      Open in Azure
                    </Button>
                    <Button
                      appearance="subtle"
                      icon={azureExpanded ? <ChevronUpRegular /> : <ChevronDownRegular />}
                      size="small"
                      onClick={(e) => { e.stopPropagation(); setAzureExpanded((v) => !v); }}
                      style={{ marginLeft: "auto" }}
                    />
                  </div>
                  {azureExpanded && (
                  <table className={styles.resourceTable}>
                    <thead>
                      <tr>
                        <th className={styles.resourceCell} style={{ textAlign: "left", color: tokens.colorNeutralForeground3 }}>Name</th>
                        <th className={styles.resourceCell} style={{ textAlign: "left", color: tokens.colorNeutralForeground3 }}>Type</th>
                        <th className={styles.resourceCell} style={{ textAlign: "left", color: tokens.colorNeutralForeground3 }}>Location</th>
                      </tr>
                    </thead>
                    <tbody>
                      {deployedResources.azure.map((r) => (
                        <tr key={r.id} className={styles.resourceRow}>
                          <td className={styles.resourceCell}>
                            <Text size={200}>{r.name}</Text>
                          </td>
                          <td className={styles.resourceCell}>
                            <span className={styles.resourceType}>{r.type}</span>
                          </td>
                          <td className={styles.resourceCell}>
                            <span className={styles.resourceType}>{r.location}</span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  )}
                </div>
              )}

              {/* Nothing found at all */}
              {!deployedResources.workspace && deployedResources.azure.length === 0 && (
                <Text size={200} style={{ color: tokens.colorNeutralForeground3, padding: tokens.spacingVerticalS }}>
                  No deployed resources detected yet
                </Text>
              )}
            </>
          )}
        </Card>
      )}

      {/* Floating cancel button - bottom left */}
      {isRunning && (
        <Button
          className={styles.floatingCancelBtn}
          appearance="outline"
          icon={<DismissRegular />}
          onClick={handleCancel}
          size="medium"
          style={{ color: tokens.colorPaletteRedForeground1, border: `1px solid ${tokens.colorPaletteRedForeground1}` }}
        >
          Cancel Deployment
        </Button>
      )}

      {/* Floating redeploy button - bottom left (shown on failure/cancel) */}
      {(isFailed || isCancelled) && !isRunning && (
        <Button
          className={styles.floatingCancelBtn}
          appearance="primary"
          icon={<ArrowRepeatAllRegular />}
          onClick={async () => {
            const deployConfig = (status?.customStatus as Record<string, unknown>)?.deployConfig as DeploymentConfig | undefined;
            if (!deployConfig) {
              setError("Original deployment config not available. Please start a new deployment from the Deploy tab.");
              return;
            }
            setRedeploying(true);
            try {
              const { instanceId: newId } = await startDeployment(deployConfig);
              navigate(`/monitor/${newId}`);
            } catch (e) {
              setError(e instanceof Error ? e.message : "Failed to redeploy");
            } finally {
              setRedeploying(false);
            }
          }}
          disabled={redeploying}
          size="medium"
        >
          {redeploying ? "Starting…" : "Redeploy with Same Parameters"}
        </Button>
      )}

      {/* Floating auto-scroll toggle - bottom right */}
      {isRunning && (
        <Button
          className={styles.floatingScrollBtn}
          appearance={autoScroll ? "primary" : "outline"}
          icon={autoScroll ? <PauseRegular /> : <ArrowDownRegular />}
          onClick={() => setAutoScroll((v) => !v)}
          size="medium"
        >
          {autoScroll ? "Auto-scroll On" : "Auto-scroll Off"}
        </Button>
      )}
    </div>
  );
}
