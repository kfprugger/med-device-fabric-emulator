import { useEffect, useState, useCallback, useRef } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  Button,
  Card,
  CardHeader,
  Checkbox,
  MessageBar,
  MessageBarBody,
  Subtitle1,
  Text,
  Title2,
  Tooltip,
  Badge,
  makeStyles,
  tokens,
  Dialog,
  DialogSurface,
  DialogTitle,
  DialogBody,
  DialogContent,
  DialogActions,
  Input,
} from "@fluentui/react-components";
import {
  PlayRegular,
  DismissRegular,
  ArrowDownRegular,
  PauseRegular,
  ArrowRepeatAllRegular,
  TextBulletListRegular,
  ArrowLeftRegular,
  ClipboardRegular,
  OpenRegular,
  ShieldRegular,
  WarningRegular,
  ErrorCircleRegular,
  CopyRegular,
} from "@fluentui/react-icons";
import { PhaseCard } from "../components/PhaseCard";
import { AllLogsStream } from "../components/AllLogsStream";
import { DeployedResourcesPanel } from "../components/DeployedResourcesPanel";
import {
  getDeploymentStatus,
  resumeAfterHds,
  cancelDeployment,
  startDeployment,
  getDeployedResources,
  getAfterActionReport,
  type DeploymentStatus,
  type DeploymentConfig,
  type PhaseInfo,
  type DeployedResourcesResult,
  type AfterActionReportResult,
} from "../api";
import {
  isMockInstance,
  getMockStatus,
  getMockPhases,
  resumeMockHds,
  cancelMockDeployment,
  startMockDeployment,
  type PhaseLog,
} from "../mockDeployment";
import { useReducedMotion } from "../hooks/useReducedMotion";

const TRACK_HEIGHT = 6;
const DOT_SIZE = 22; // CSS width/height (excluding border)
const DOT_BORDER = 3;
const DOT_TOTAL = DOT_SIZE + DOT_BORDER * 2; // actual rendered size = 28px
const TRACK_CENTER = 32; // y-center of the track line in the track area
const TRACK_TOP = TRACK_CENTER - TRACK_HEIGHT / 2;
const DOT_TOP = TRACK_CENTER - DOT_TOTAL / 2; // vertically center dots on track

const MILESTONE_ANIMATION_CSS = `
@keyframes milestone-pulse-standard {
  0% {
    box-shadow: 0 0 0 3px ${tokens.colorNeutralBackground1}, 0 0 0 4px rgba(0, 163, 153, 0.4), 0 0 0 6px rgba(0, 163, 153, 0);
  }
  50% {
    box-shadow: 0 0 0 3px ${tokens.colorNeutralBackground1}, 0 0 0 6px rgba(0, 163, 153, 0.45), 0 0 0 12px rgba(0, 163, 153, 0.25);
  }
  100% {
    box-shadow: 0 0 0 3px ${tokens.colorNeutralBackground1}, 0 0 0 8px rgba(0, 163, 153, 0.35), 0 0 0 16px rgba(0, 163, 153, 0);
  }
}
@keyframes milestone-pulse-teardown {
  0% {
    box-shadow: 0 0 0 3px ${tokens.colorNeutralBackground1}, 0 0 0 4px rgba(255, 185, 0, 0.4), 0 0 0 6px rgba(255, 185, 0, 0);
  }
  50% {
    box-shadow: 0 0 0 3px ${tokens.colorNeutralBackground1}, 0 0 0 6px rgba(255, 185, 0, 0.45), 0 0 0 12px rgba(255, 185, 0, 0.25);
  }
  100% {
    box-shadow: 0 0 0 3px ${tokens.colorNeutralBackground1}, 0 0 0 8px rgba(255, 185, 0, 0.35), 0 0 0 16px rgba(255, 185, 0, 0);
  }
}
.milestone-pulse-done {
  animation: milestone-pulse-standard 2.2s infinite cubic-bezier(0.4, 0, 0.2, 1) !important;
}
.milestone-pulse-teardown-done {
  animation: milestone-pulse-teardown 2.2s infinite cubic-bezier(0.4, 0, 0.2, 1) !important;
}
@keyframes gantt-stripes {
  from { background-position: 0 0; }
  to { background-position: 40px 0; }
}
.gantt-running-striped {
  background-image: linear-gradient(45deg, rgba(0,0,0,0.18) 25%, transparent 25%, transparent 50%, rgba(0,0,0,0.18) 50%, rgba(0,0,0,0.18) 75%, transparent 75%, transparent) !important;
  background-size: 40px 40px !important;
  animation: gantt-stripes 1.2s linear infinite !important;
}
@keyframes springOut {
  0% {
    transform: scale(0.9) translateY(40px);
    opacity: 0;
  }
  55% {
    transform: scale(1.04) translateY(-8px);
    opacity: 0.85;
  }
  75% {
    transform: scale(0.98) translateY(3px);
    opacity: 0.95;
  }
  100% {
    transform: scale(1) translateY(0);
    opacity: 1;
  }
}
.spring-active {
  animation: springOut 0.8s cubic-bezier(0.175, 0.885, 0.32, 1.275) both !important;
}
`;

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
    height: "105px",
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
    right: "4%",
    height: `${TRACK_HEIGHT}px`,
    transform: "scaleX(0)",
    transformOrigin: "left center",
    borderRadius: "3px",
    transition: "transform 0.6s ease",
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
    backgroundColor: tokens.colorBrandForeground1,
    color: tokens.colorNeutralForegroundOnBrand,
    boxShadow: `0 0 0 3px ${tokens.colorNeutralBackground1}, 0 0 0 6px rgba(0, 163, 153, 0.35), ${tokens.shadow4}`,
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
    lineHeight: tokens.lineHeightBase300,
    paddingBottom: "2px",
  },
  milestoneLabelActive: {
    color: tokens.colorBrandForeground1,
    fontWeight: tokens.fontWeightSemibold,
  },
  milestoneLabelDone: {
    marginTop: tokens.spacingVerticalS,
    padding: `${tokens.spacingVerticalXXS} ${tokens.spacingHorizontalM}`,
    backgroundColor: tokens.colorBrandForeground1,
    color: "#ffffff",
    textShadow: "0 3px 3px rgba(0, 0, 0, 0.4)",
    borderRadius: tokens.borderRadiusMedium,
    fontSize: tokens.fontSizeBase200,
    fontWeight: tokens.fontWeightSemibold,
    whiteSpace: "nowrap" as const,
    maxWidth: "none",
    lineHeight: tokens.lineHeightBase200,
    boxShadow: tokens.shadow8,
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
  configCard: {
    marginBottom: tokens.spacingVerticalM,
    padding: tokens.spacingHorizontalM,
  },
  configGrid: {
    display: "flex",
    flexWrap: "wrap" as const,
    gap: `${tokens.spacingVerticalXXS} ${tokens.spacingHorizontalL}`,
    marginTop: tokens.spacingVerticalS,
  },
  configItem: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalXS,
    fontSize: tokens.fontSizeBase200,
    minWidth: "200px",
  },
  clickableConfigItem: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalXS,
    fontSize: tokens.fontSizeBase200,
    minWidth: "200px",
    cursor: "pointer",
    transition: "color 0.2s, transform 0.2s",
    ":hover": {
      color: tokens.colorBrandForeground1,
      transform: "translateY(-1.5px)",
    },
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
    alignItems: "center",
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
  { phase: "1. Data Fabric Foundation: Fabric Workspace", status: "pending" },
  { phase: "1. Data Fabric Foundation: Base Azure Infrastructure", status: "pending" },
  { phase: "1. Data Fabric Foundation: FHIR Service + Synthea + Loader", status: "pending" },
  { phase: "3. Multimodal Cohorting & Imaging: DICOM Service + Loader", status: "pending" },
  { phase: "2. Active Patient Telemetry: Fabric RTI Ingest", status: "pending" },
  { phase: "1. Data Fabric Foundation: HDS Detection", status: "pending" },
  { phase: "2. Active Patient Telemetry: Fabric RTI Enrichment", status: "pending" },
  { phase: "3. Multimodal Cohorting & Imaging: DICOM Shortcut + HDS Pipelines", status: "pending" },
  { phase: "4. Connected Semantic Intelligence: Conversational Data Agents", status: "pending" },
  { phase: "3. Multimodal Cohorting & Imaging: Custom SWA Viewer & Direct Lake", status: "pending" },
  { phase: "4. Connected Semantic Intelligence: Clinical Device Ontology", status: "pending" },
  { phase: "5. Bedside Alerting & Action: Real-Time Reflex alerts", status: "pending" },
  { phase: "6. Population Health & Quality: Full analytics pipeline", status: "pending" },
];

export function PhaseMonitor() {
  const styles = useStyles();
  const reducedMotion = useReducedMotion();
  const { instanceId } = useParams<{ instanceId: string }>();
  const navigate = useNavigate();
  const [status, setStatus] = useState<DeploymentStatus | null>(null);
  const [mockPhaseLogs, setMockPhaseLogs] = useState<
    Map<string, PhaseLog[]>
  >(new Map());
  const [error, setError] = useState("");
  const [autoScroll, setAutoScroll] = useState(true);
  const [showAllLogs, setShowAllLogs] = useState(false);
  const [redeploying, setRedeploying] = useState(false);
  const [deployedResources, setDeployedResources] = useState<DeployedResourcesResult | null>(null);
  const [resourcesLoading, setResourcesLoading] = useState(false);
  const [lastResourceFetch, setLastResourceFetch] = useState(0);
  const [frozenElapsed, setFrozenElapsed] = useState<number | null>(null);
  const [tick, setTick] = useState(0);
  const [operatorMode, setOperatorMode] = useState(false);
  const [drilldownType, setDrilldownType] = useState<"error" | "warn" | null>(null);
  const [drilldownSearch, setDrilldownSearch] = useState("");
  const [copiedLogId, setCopiedLogId] = useState<string | null>(null);
  const [copiedAll, setCopiedAll] = useState(false);
  const [notificationsEnabled, setNotificationsEnabled] = useState(false);
  const [notificationPermissionGranted, setNotificationPermissionGranted] = useState(
    typeof window !== "undefined" && "Notification" in window
      ? window.Notification.permission === "granted"
      : false
  );
  const [resourceErrorNotified, setResourceErrorNotified] = useState(false);
  const [showAfterActionReport, setShowAfterActionReport] = useState(false);
  const [showGantt, setShowGantt] = useState(true);
  const [compressCompleted, setCompressCompleted] = useState(false);
  const [afterActionReport, setAfterActionReport] = useState<AfterActionReportResult | null>(null);
  const [afterActionLoading, setAfterActionLoading] = useState(false);
  const afterActionCardRef = useRef<HTMLDivElement>(null);

  // Scroll to After Action report card when it is opened
  useEffect(() => {
    if (showAfterActionReport) {
      const t = setTimeout(() => {
        if (afterActionCardRef.current) {
          afterActionCardRef.current.scrollIntoView({
            behavior: "smooth",
            block: "start",
          });
        }
      }, 150);
      return () => clearTimeout(t);
    }
  }, [showAfterActionReport]);

  const isMock = instanceId ? isMockInstance(instanceId) : false;

  // Reset all deployment-specific states on instanceId change
  useEffect(() => {
    setStatus(null);
    setMockPhaseLogs(new Map());
    setError("");
    setDeployedResources(null);
    setFrozenElapsed(null);
    setTick(0);
    setShowAfterActionReport(false);
    setShowGantt(true);
    setCompressCompleted(false);
    setAfterActionReport(null);
    setResourceErrorNotified(false);
    setLastResourceFetch(0);
    setDrilldownType(null);
    setDrilldownSearch("");
    setCopiedLogId(null);
    setCopiedAll(false);
  }, [instanceId]);

  const statusIsTerminalForPolling =
    status?.runtimeStatus === "Completed" ||
    status?.runtimeStatus === "Terminated" ||
    status?.runtimeStatus === "Failed";

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
    if (statusIsTerminalForPolling && !isMock) return;
    poll();
    const interval = setInterval(() => {
      if (typeof document !== "undefined" && document.visibilityState === "hidden") return;
      poll();
    }, isMock ? 500 : 5000);
    return () => clearInterval(interval);
  }, [poll, isMock, statusIsTerminalForPolling]);

  const isRunning = status?.runtimeStatus === "Running";
  const isWaitingForHds =
    status?.customStatus?.status === "waiting_for_input";
  const isCancelled = status?.runtimeStatus === "Terminated";
  const isFailed = status?.runtimeStatus === "Failed";
  const isComplete =
    status?.runtimeStatus === "Completed" ||
    isCancelled || isFailed;

  // Detect if this is a teardown run
  const isTeardown = (status?.customStatus as Record<string, unknown>)?.runType === "teardown"
    || (instanceId ?? "").toLowerCase().startsWith("teardown");

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
  const backendLogs = (status?.customStatus as Record<string, unknown>)?.logs as Array<{timestamp: string; level: string; message: string; phase?: number}> | undefined;

  // Drilldown log filtering
  const errorLogs = (backendLogs ?? []).filter(
    (log) => (log.level ?? "").toLowerCase() === "error"
  );
  const warnLogs = (backendLogs ?? []).filter(
    (log) =>
      (log.level ?? "").toLowerCase() === "warn" ||
      (log.level ?? "").toLowerCase() === "warning"
  );
  const filteredDrilldownLogs = (drilldownType === "error" ? errorLogs : warnLogs).filter(
    (log) => log.message.toLowerCase().includes(drilldownSearch.toLowerCase())
  );

  // Compute elapsed time — freeze when deployment is no longer running
  useEffect(() => {
    if (isRunning && !isWaitingForHds) {
      const t = setInterval(() => setTick((v) => v + 1), 1000);
      return () => clearInterval(t);
    }
  }, [isRunning, isWaitingForHds]);

  useEffect(() => {
    if (!isRunning && status && frozenElapsed === null) {
      // Priority 1: sum of phase durations (excludes HDS manual wait)
      const phaseDurationSum = phases.reduce((sum, p) => {
        if (typeof p.duration === "number") return sum + p.duration;
        return sum;
      }, 0);
      if (phaseDurationSum > 0) {
        setFrozenElapsed(phaseDurationSum);
        return;
      }
      // Priority 2: backend-computed durationSeconds
      const backendDuration = (status.customStatus as Record<string, unknown>)?.durationSeconds;
      if (typeof backendDuration === "number" && backendDuration > 0) {
        setFrozenElapsed(backendDuration);
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
      .catch(() => {
        if (!resourceErrorNotified) {
          setError("Unable to refresh deployed resources right now.");
          setResourceErrorNotified(true);
        }
      })
      .finally(() => setResourcesLoading(false));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [instanceId, isMock, phases.length, isCancelled, isFailed, isRunning, resourceErrorNotified]);

  // Fetch After Action report when requested
  useEffect(() => {
    if (!instanceId || !showAfterActionReport || afterActionReport) return;

    setAfterActionLoading(true);
    getAfterActionReport(instanceId)
      .then((res) => {
        setAfterActionReport(res);
      })
      .catch(() => {
        setError("Unable to retrieve the After Action Security & Resources Report.");
      })
      .finally(() => {
        setAfterActionLoading(false);
      });
  }, [instanceId, showAfterActionReport, afterActionReport]);

  const elapsedSeconds = frozenElapsed !== null
    ? frozenElapsed
    : status?.createdTime
      ? (Date.now() - new Date(status.createdTime).getTime()) / 1000
      : 0;
  void tick; // suppress unused warning
  const elapsedFormatted = elapsedSeconds > 0
    ? `${Math.floor(elapsedSeconds / 60)}m ${Math.floor(elapsedSeconds % 60)}s`
    : "";

  const logCounts = (backendLogs ?? []).reduce((acc, log) => {
    const level = (log.level || "info").toLowerCase();
    acc[level] = (acc[level] ?? 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  const elapsedMinutes = elapsedSeconds / 60;
  const completedBeforeActions = phases.filter((p) => p.status === "succeeded" || p.status === "skipped").length;
  const completedOrPartial = completedBeforeActions + (phases.some((p) => p.status === "running") ? 0.35 : 0);
  const remainingPhases = Math.max(phases.length - completedOrPartial, 0);
  const etaMinutes = isRunning && elapsedMinutes > 0 && completedOrPartial > 0
    ? Math.min(180, Math.max(1, Math.round((remainingPhases / completedOrPartial) * elapsedMinutes)))
    : 0;

  const copyDiagnostics = () => {
    const diagnostics = {
      instanceId,
      runtimeStatus: status?.runtimeStatus,
      currentPhase,
      elapsed: elapsedFormatted,
      etaMinutes,
      completedPhases: completedBeforeActions,
      totalPhases: phases.length,
      logCounts,
      resources: status?.customStatus?.resources ?? status?.output?.resources ?? {},
    };
    navigator.clipboard?.writeText(JSON.stringify(diagnostics, null, 2)).catch(() => undefined);
  };

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
    if (!window.confirm("Cancel this deployment? Running processes will be terminated.")) return;
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

  // Trigger OS Notifications on deployment completion
  useEffect(() => {
    if (!status || !notificationsEnabled || !notificationPermissionGranted || !instanceId) return;

    const runtimeStatus = status.runtimeStatus;
    const isFinished = runtimeStatus === "Completed" || runtimeStatus === "Failed" || runtimeStatus === "Terminated";
    
    if (isFinished) {
      const sessionKey = `notified-${instanceId}-${runtimeStatus}`;
      if (sessionStorage.getItem(sessionKey)) return;
      
      sessionStorage.setItem(sessionKey, "true");

      const title = runtimeStatus === "Completed" ? "Deployment Successful!" : `Deployment ${runtimeStatus}`;
      const elapsedText = elapsedFormatted ? ` in ${elapsedFormatted}` : "";
      new Notification(title, {
        body: `Instance: ${instanceId}\nStatus: ${runtimeStatus}${elapsedText}\nTotal completed phases: ${completedCount}/${phases.length}.`,
        tag: instanceId,
        requireInteraction: true
      });
    }
  }, [status, notificationsEnabled, notificationPermissionGranted, instanceId, elapsedFormatted, completedCount, phases.length]);

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
    // Phase 5 — ~12 min total
    { patterns: ["Quality", "Claims", "CMS", "Scorecard", "PDC"], weight: 12 },
  ];

  function getStepWeight(phaseName: string): number {
    // Prefer explicit phase buckets when present in backend phase names to avoid
    // keyword collisions (for example "DICOM" appearing in a Phase 2 label).
    const phaseMatch = phaseName.match(/PHASE\s*(\d+)/i);
    if (phaseMatch) {
      const n = Number(phaseMatch[1]);
      if (n === 1) return 40 / 6;
      if (n === 2) return 20 / 3;
      if (n === 3) return 10;
      if (n === 4) return 5;
    }

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

  // Map weighted progress to visual bar position using piecewise-linear interpolation.
  // Segments are built dynamically from active milestones.
  function weightToVisualPct(w: number, segments: Array<{wStart: number; wEnd: number; vStart: number; vEnd: number}>): number {
    for (const seg of segments) {
      if (w <= seg.wEnd) {
        const t = seg.wEnd === seg.wStart ? 1 : (w - seg.wStart) / (seg.wEnd - seg.wStart);
        return seg.vStart + t * (seg.vEnd - seg.vStart);
      }
    }
    return 92;
  }

  // Milestone definitions (static templates)
  type MilestoneDef = { label: string; phaseIndices: number[]; namePatterns: string[]; position: number; endWeight: number; phaseNumber?: number };

  // Teardown-specific milestones: reverse order of deployment phases.
  // Teardown-specific milestones: describe actual teardown operations.
  const TEARDOWN_MILESTONES: MilestoneDef[] = [
    { label: "Workspace Items", phaseIndices: [0], namePatterns: ["Fabric Workspace Items"], position: 8, endWeight: 20 },
    { label: "Workspace Identity", phaseIndices: [1], namePatterns: ["Workspace Identity"], position: 36, endWeight: 40 },
    { label: "Workspace Deletion", phaseIndices: [2], namePatterns: ["Delete Workspace"], position: 64, endWeight: 60 },
    { label: "Azure Resources", phaseIndices: [3], namePatterns: ["Azure Resource Group"], position: 90, endWeight: 80 },
  ];

  const MILESTONES: MilestoneDef[] = [
    { label: "1. Data Fabric Foundation", phaseIndices: [0, 1, 2, 3, 5], namePatterns: ["Fabric Workspace", "Base Azure Infrastructure", "FHIR", "DICOM Service", "HDS"], position: 8, endWeight: 20, phaseNumber: 1 },
    { label: "2. Active Patient Telemetry", phaseIndices: [4, 6], namePatterns: ["Fabric RTI", "Fabric RTI (auto)", "Telemetry"], position: 24, endWeight: 40, phaseNumber: 2 },
    { label: "3. Multimodal Cohorting & Imaging", phaseIndices: [7, 9], namePatterns: ["DICOM Shortcut", "HDS Pipelines", "Imaging", "DICOM Viewer"], position: 40, endWeight: 60, phaseNumber: 3 },
    { label: "4. Connected Semantic Intelligence", phaseIndices: [8, 10], namePatterns: ["Data Agent", "Ontology"], position: 56, endWeight: 75, phaseNumber: 4 },
    { label: "5. Bedside Alerting & Action", phaseIndices: [11], namePatterns: ["Activator", "Reflex"], position: 75, endWeight: 85, phaseNumber: 5 },
    { label: "6. Population Health & Quality", phaseIndices: [12], namePatterns: ["Quality", "Claims", "CMS", "Scorecard", "PDC", "Adherence", "HCC", "RAF", "Readmission", "Utilization", "PMPM", "Star Rating"], position: 92, endWeight: 95, phaseNumber: 6 },
  ];

  // ── Adaptive milestones: determine active milestones from instance ID ──
  // Instance ID format: P<milestone-digits>-<timestamp> (e.g. P12345-20260406-195906)
  // Legacy formats: ALLPHASES-*, PHASE2+-*, FABRIC-*, teardown*
  function getActiveMilestoneNumbers(): Set<number> {
    const id = instanceId ?? "";

    // New format: P followed by milestone digits (P12345, P2345, P3, etc.)
    const pMatch = id.match(/^P(\d+)-/i);
    if (pMatch) {
      const nums = pMatch[1].split("").map(Number);
      const set = new Set(nums.filter((n) => n >= 1 && n <= 6));
      if (set.has(5) && !set.has(6)) {
        // If it had the 5-digit full deploy, map to all 6 milestones under the new model
        set.add(6);
      }
      return set;
    }

    // Legacy formats
    if (id.startsWith("ALLPHASES")) return new Set([1, 2, 3, 4, 5, 6]);
    if (id.startsWith("PHASE2+")) return new Set([1, 2, 3, 4, 5, 6]); 
    if (id.startsWith("FABRIC")) return new Set([1, 2, 3, 4, 5, 6]);   

    // Default: show all
    return new Set([1, 2, 3, 4, 5, 6]);
  }


  const activeMilestoneNumbers = getActiveMilestoneNumbers();

  const allMilestonesTemplate = isTeardown ? TEARDOWN_MILESTONES : MILESTONES;
  // Filter milestones to those whose phaseNumber is in the active set.
  // For teardown: hide Fabric milestones when only an Azure RG is being torn down.
  const teardownHasFabric = !!(status?.customStatus as Record<string, unknown>)?.workspaceName;
  const teardownHasAzure = !!(status?.customStatus as Record<string, unknown>)?.resourceGroupName;
  const FABRIC_TEARDOWN_PATTERNS = new Set(["Fabric Workspace Items", "Workspace Identity", "Delete Workspace"]);
  const AZURE_TEARDOWN_PATTERNS = new Set(["Azure Resource Group"]);
  const filteredMilestones = isTeardown
    ? allMilestonesTemplate.filter((ms) => {
        const isFabricMilestone = ms.namePatterns.some((p) => FABRIC_TEARDOWN_PATTERNS.has(p));
        const isAzureMilestone = ms.namePatterns.some((p) => AZURE_TEARDOWN_PATTERNS.has(p));
        if (isFabricMilestone && !teardownHasFabric) return false;
        if (isAzureMilestone && !teardownHasAzure) return false;
        return true;
      })
    : isMock
      ? allMilestonesTemplate
      : allMilestonesTemplate.filter((ms) => activeMilestoneNumbers.has(ms.phaseNumber ?? 0));

  // Redistribute positions evenly for the surviving milestones
  // Positions: evenly spaced between 8% and 88%
  const POSITION_MIN = 8;
  const POSITION_MAX = 88;
  const activeMilestones = filteredMilestones.map((ms, i) => {
    const n = filteredMilestones.length;
    const position = n === 1
      ? (POSITION_MIN + POSITION_MAX) / 2
      : POSITION_MIN + (i / (n - 1)) * (POSITION_MAX - POSITION_MIN);
    return { ...ms, position };
  });

  // Recompute weight segments dynamically from active milestones
  // Build dynamic weight→visual segments from active milestones
  const dynamicSegments: Array<{wStart: number; wEnd: number; vStart: number; vEnd: number}> = [];
  {
    let cumWeight = 0;
    let prevVisual = 0;
    for (let i = 0; i < activeMilestones.length; i++) {
      const ms = activeMilestones[i];
      // Sum weights for this milestone's steps
      let msWeight = 0;
      for (const sw of STEP_WEIGHTS) {
        if (sw.patterns.some((pat) =>
          ms.namePatterns.some((mp) => mp.toUpperCase().includes(pat.toUpperCase()) || pat.toUpperCase().includes(mp.toUpperCase()))
        )) {
          msWeight += sw.weight;
        }
      }
      if (msWeight === 0) msWeight = 1; // Ensure non-zero
      const visualEnd = i < activeMilestones.length - 1
        ? (activeMilestones[i].position + activeMilestones[i + 1].position) / 2
        : 92;
      dynamicSegments.push({
        wStart: cumWeight,
        wEnd: cumWeight + msWeight,
        vStart: prevVisual,
        vEnd: visualEnd,
      });
      cumWeight += msWeight;
      prevVisual = visualEnd;
    }
  }

  const weightedProgressPct = isComplete
    ? 100
    : isTeardown
      ? (() => {
          const tdCompleted = phases.filter((p) => p.status === "succeeded" || p.status === "skipped").length;
          const tdRunning = phases.filter((p) => p.status === "running").length;
          const tdTotal = Math.max(phases.length, activeMilestones.length);
          return ((tdCompleted + tdRunning * 0.3) / tdTotal) * 92;
        })()
      : dynamicSegments.length > 0
        ? weightToVisualPct(weightedCompleted + weightedRunning, dynamicSegments)
        : 0;

  const phaseNumberMatch = currentPhase.match(/PHASE\s*(\d+)/i);
  const currentPhaseNumber = phaseNumberMatch ? Number(phaseNumberMatch[1]) : 0;

  // Build minimumVisualByPhase dynamically from active milestone positions
  const minimumVisualByPhase: Record<number, number> = {};
  for (const ms of activeMilestones) {
    const pn = ms.phaseNumber;
    if (pn) {
      minimumVisualByPhase[pn] = ms.position + 2;
    }
  }

  const phaseFloorPct = isTeardown ? 0 : (minimumVisualByPhase[currentPhaseNumber] ?? 0);
  const progressPct = Math.max(weightedProgressPct, phaseFloorPct);

  const progressColor = (isCancelled || isFailed)
    ? tokens.colorPaletteRedForeground1
    : isTeardown
      ? tokens.colorPaletteYellowForeground1
      : isComplete
        ? tokens.colorPaletteGreenForeground1
        : isWaitingForHds
        ? tokens.colorPaletteYellowForeground1
        : tokens.colorBrandForeground1;

  function getMilestoneStatus(ms: MilestoneDef): "done" | "active" | "waiting" | "pending" | "cancelled" {
    if (isTeardown) {
      // For teardown: match phases by namePatterns against the teardown phase names
      const matchedPhases = phases.filter((p) =>
        ms.namePatterns.some((pat) => p.phase.toUpperCase().includes(pat.toUpperCase()))
      );
      if (matchedPhases.length === 0) return "pending";
      const allDone = matchedPhases.every((p) => p.status === "succeeded" || p.status === "skipped");
      if (allDone) return "done";
      const anyRunning = matchedPhases.some((p) => p.status === "running");
      if (anyRunning) return "active";
      return "pending";
    }

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

    const milestoneIndex = activeMilestones.findIndex((m) => m.label === ms.label);

    // Phase-number progression from backend is authoritative for milestone transitions.
    const msPhaseNumber = ms.phaseNumber ?? 0;
    if (currentPhaseNumber > 0 && msPhaseNumber > 0) {
      if (currentPhaseNumber > msPhaseNumber) return "done";
      if (currentPhaseNumber === msPhaseNumber && isRunning) return "active";
    }

    // Real mode fallback: check if all phases matching this milestone are done.
    const matchedPhases = phases.filter((p) =>
      ms.namePatterns.some((pat) => p.phase.toUpperCase().includes(pat.toUpperCase()))
    );
    const allDone = matchedPhases.length > 0 && matchedPhases.every((p) => p.status === "succeeded" || p.status === "skipped");
    if (allDone) return "done";

    // Weight-based fallback: find the dynamic segment for this milestone
    if (milestoneIndex >= 0 && milestoneIndex < dynamicSegments.length) {
      if (weightedCompleted >= dynamicSegments[milestoneIndex].wEnd) return "done";
    }
    const hasWaiting = matchedPhases.some((p) => p.status === "waiting_for_input");
    if (hasWaiting) return "waiting";

    // If cancelled/failed, check if this milestone had any activity
    if (isCancelled || isFailed) {
      const anyRan = matchedPhases.some((p) => p.status !== "pending");
      return anyRan ? "cancelled" : "pending";
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
  const milestoneStatuses = activeMilestones.map((ms: MilestoneDef) => getMilestoneStatus(ms));
  const milestonesDone = milestoneStatuses.filter((s: string) => s === "done").length;
  const totalMilestones = activeMilestones.length;
  return (
    <div>
      <style>{MILESTONE_ANIMATION_CSS}</style>
      <div className={styles.header}>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalS }}>
            <Button
              appearance="subtle"
              icon={<ArrowLeftRegular />}
              onClick={() => navigate("/history")}
              size="small"
            />
            <Title2>{isTeardown ? "Teardown Monitor" : "Deployment Monitor"}</Title2>
          </div>
          <Text size={200} block style={{ marginLeft: 36 }}>
            {instanceId}
            {isMock && (
              <Badge color="informative" style={{ marginLeft: 8 }}>
                Mock Mode
              </Badge>
            )}
          </Text>
        </div>

        <div className={styles.actions}>
          <Checkbox
            checked={operatorMode}
            onChange={(_, data) => setOperatorMode(!!data.checked)}
            label="Operator mode"
          />
          <Checkbox
            checked={notificationsEnabled}
            onChange={async (_, data) => {
              const enabled = !!data.checked;
              if (enabled && typeof window !== "undefined" && "Notification" in window) {
                const permission = await window.Notification.requestPermission();
                if (permission === "granted") {
                  setNotificationPermissionGranted(true);
                  setNotificationsEnabled(true);
                  new Notification("System Notifications Enabled", {
                    body: "You will receive desktop alerts when the deployment finishes.",
                  });
                } else {
                  setNotificationPermissionGranted(false);
                  setNotificationsEnabled(false);
                  alert("Please enable notification permissions in your browser settings to receive alerts.");
                }
              } else {
                setNotificationsEnabled(enabled);
              }
            }}
            label="OS Notifications"
          />
          <Badge
            color={isCancelled ? "warning" : isFailed ? "danger" : isComplete ? (isTeardown ? "warning" : "success") : isRunning ? "informative" : "subtle"}
            size="large"
            style={{ transform: "translateY(1px)" }}
          >
            {milestonesDone}/{totalMilestones} phases{" "}
            {isCancelled ? "cancelled" : isComplete ? (isTeardown ? "torn down" : "complete") : ""}{" "}
            {elapsedFormatted && `(${elapsedFormatted})`}
          </Badge>
          {isComplete && !isTeardown && (
            <Button
              appearance={showAfterActionReport ? "primary" : "outline"}
              icon={<ShieldRegular />}
              onClick={() => setShowAfterActionReport((prev) => !prev)}
              style={showAfterActionReport ? {} : {
                borderColor: tokens.colorPaletteBlueBorderActive,
                color: tokens.colorPaletteBlueBorderActive,
                boxShadow: `0 0 4px ${tokens.colorPaletteBlueBorderActive}`
              }}
            >
              {showAfterActionReport ? "Hide Artifacts & Security" : "Deployment Artifacts & Security"}
            </Button>
          )}
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
                appearance="outline"
                icon={<DismissRegular />}
                onClick={handleCancel}
                style={{
                  borderColor: tokens.colorPaletteRedForeground1,
                  color: tokens.colorPaletteRedForeground1,
                  boxShadow: `0 0 8px ${tokens.colorPaletteRedForeground1}, 0 0 2px ${tokens.colorPaletteRedForeground1}`,
                }}
              >
                Cancel
              </Button>
            </>
          )}
        </div>
      </div>

      {/* Milestone progress track */}
      <div className={styles.progressSection} style={{
        ...(isTeardown ? { boxShadow: `${tokens.shadow8}, 0 0 12px rgba(255, 185, 0, 0.25)` } : {}),
        ...(operatorMode ? { padding: `${tokens.spacingVerticalM} ${tokens.spacingHorizontalL}` } : {}),
      }}>
        <div className={styles.milestoneTrack}>
          {/* Background track line */}
          <div className={styles.trackLine} />
          {/* Filled track line; width is expressed in container % (track starts at 4%) */}
          <div
            className={styles.trackFill}
            style={{
              transform: `scaleX(${Math.max(0, Math.min(progressPct, 100)) / 100})`,
              backgroundColor: progressColor,
              transition: reducedMotion ? "none" : undefined,
            }}
          />
          {/* Milestone nodes */}
          {activeMilestones.map((ms) => {
            const msStatus = getMilestoneStatus(ms);
            return (
              <div
                key={ms.label}
                className={styles.milestoneContainer}
                style={{ left: `${ms.position}%` }}
              >
                {/* Dot */}
                <div
                  className={`${styles.milestoneDot} ${getDotClass(msStatus)} ${
                    msStatus === "done" && !reducedMotion
                      ? isTeardown
                        ? "milestone-pulse-teardown-done"
                        : "milestone-pulse-done"
                      : ""
                  }`}
                  style={isTeardown && msStatus === "done"
                    ? { backgroundColor: tokens.colorPaletteYellowForeground1 }
                    : isTeardown && msStatus === "active"
                      ? { backgroundColor: tokens.colorPaletteYellowForeground1, boxShadow: `0 0 0 3px rgba(255, 185, 0, 0.3), ${tokens.shadow4}` }
                      : undefined
                  }
                >
                  {getDotContent(msStatus)}
                </div>
                {/* Label below */}
                <span
                  className={`${styles.milestoneLabel} ${
                    msStatus === "done"
                      ? styles.milestoneLabelDone
                      : msStatus === "waiting"
                      ? styles.milestoneLabelActive
                      : ""
                  }`}
                  style={isTeardown && msStatus === "done"
                    ? { backgroundColor: tokens.colorPaletteYellowForeground1, color: "#000000", textShadow: "none" }
                    : isTeardown && msStatus === "active"
                      ? { color: tokens.colorPaletteYellowForeground1 }
                      : undefined
                  }
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
            {isComplete ? (isTeardown ? " torn down" : " complete") : ""}
            {isRunning && currentPhase ? ` · ${currentPhase}` : ""}
          </Text>
          <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>
            {elapsedFormatted || "0m 0s"}{isRunning && etaMinutes > 0 ? ` · ETA ~${etaMinutes}m` : ""}
          </Text>
        </div>

        {/* Gantt Timeline Analysis Section (Sliding Animation) */}
        {(() => {
          // Define a function to parse duration
          const parseDurationMinutes = (durationStr: string | number | undefined): number => {
            if (typeof durationStr === "number") return durationStr;
            if (!durationStr) return 0;
            const match = durationStr.toString().match(/([\d.]+)\s*m/i);
            return match ? parseFloat(match[1]) : 0;
          };

          // Build a normalized list of all phases in order (Completed, Current, Future)
          const allPhasesNormalized = ALL_PHASES.map((ap) => {
            const resolved = phases.find((p) => {
              if (p.phase === ap.phase) return true;
              
              const clean = (name: string) => name.toLowerCase()
                .replace(/^(phase\s*\d+:|[\d.]+\s*[^:]+:)/i, "")
                .replace(/\s*\(auto\)\s*/i, "")
                .replace(/\s*\(manual\)\s*/i, "")
                .trim();
                
              const pClean = clean(p.phase);
              const apClean = clean(ap.phase);
              
              if (pClean === apClean) return true;
              
              // Custom precise mapping logic for tricky names:
              if (pClean.includes("workspace") && apClean.includes("workspace")) return true;
              if (pClean.includes("base azure") && apClean.includes("base azure")) return true;
              if (pClean.includes("fhir service") && apClean.includes("fhir service")) return true;
              if (pClean.includes("dicom service") && apClean.includes("dicom service")) return true;
              if ((pClean.includes("healthcare data solutions") || pClean.includes("hds guidance")) && apClean.includes("hds detection")) return true;
              if (pClean.includes("dicom shortcut") && apClean.includes("dicom shortcut")) return true;
              if (pClean.includes("conversational") && apClean.includes("conversational")) return true;
              if (pClean.includes("swa viewer") && apClean.includes("swa viewer")) return true;
              if (pClean.includes("ontology") && apClean.includes("ontology")) return true;
              if (pClean.includes("reflex alerts") && apClean.includes("reflex alerts")) return true;
              if (pClean.includes("analytics pipeline") && apClean.includes("analytics pipeline")) return true;

              // Handle "fabric rti" cases very carefully to avoid collision.
              if (apClean === "fabric rti ingest") {
                const isEnrichment = p.phase.toLowerCase().includes("phase 2") || 
                                     p.phase.toLowerCase().includes("auto") || 
                                     p.phase.toLowerCase().includes("enrichment");
                return pClean.includes("fabric rti") && !isEnrichment;
              }
              
              if (apClean === "fabric rti enrichment") {
                const isEnrichment = p.phase.toLowerCase().includes("phase 2") || 
                                     p.phase.toLowerCase().includes("auto") || 
                                     p.phase.toLowerCase().includes("enrichment");
                return pClean.includes("fabric rti") && isEnrichment;
              }
              
              return pClean.includes(apClean) || apClean.includes(pClean);
            });
            if (resolved) return { ...resolved, phase: ap.phase };
            return { ...ap, status: "pending" as const };
          });

          // Separate phases by status
          const completedPhases = allPhasesNormalized.filter(
            (p) => p.status === "succeeded" || p.status === "skipped"
          );
          const activePhases = allPhasesNormalized.filter(
            (p) => p.status === "running" || p.status === "waiting_for_input"
          );
          const futurePhases = allPhasesNormalized.filter(
            (p) => p.status === "pending"
          );

          // Get total elapsed / durations
          const completedPhasesSumMins = completedPhases.reduce(
            (acc, p) => acc + parseDurationMinutes(p.duration),
            0
          );
          const runningPhaseDurationMins = Math.max(
            0.1,
            (elapsedSeconds / 60) - completedPhasesSumMins
          );

          // Helper to scroll to a phase card and highlight it
          const scrollToCard = (phaseName: string, isGreen = false) => {
            const matchingPhase = phases.find((p) => {
              if (p.phase === phaseName) return true;
              
              const clean = (name: string) => name.toLowerCase()
                .replace(/^(phase\s*\d+:|[\d.]+\s*[^:]+:)/i, "")
                .replace(/\s*\(auto\)\s*/i, "")
                .replace(/\s*\(manual\)\s*/i, "")
                .trim();
                
              return clean(p.phase) === clean(phaseName);
            });
            const targetPhaseName = matchingPhase ? matchingPhase.phase : phaseName;
            const cardId = `phase-card-${targetPhaseName.replace(/\s+/g, "-")}`;
            const element = document.getElementById(cardId);
            if (element) {
              element.scrollIntoView({ behavior: "smooth", block: "center" });
              const activeColor = isGreen ? tokens.colorPaletteGreenBorderActive : tokens.colorPaletteBlueBorderActive;
              element.style.outline = `3px solid ${activeColor}`;
              element.style.boxShadow = `0 0 16px ${activeColor}`;
              element.style.transition = "all 0.15s ease";
              setTimeout(() => {
                element.style.outline = "";
                element.style.boxShadow = "";
              }, 2200);
            }
          };

          // Build the visual blocks to render
          // 1. Calculate NORMAL (uncompressed) widths
          const normalItems = allPhasesNormalized.filter(p => p.status === "succeeded" || p.status === "skipped" || p.status === "running" || p.status === "waiting_for_input");
          const normalMinPct = 5.0;
          const normalN = normalItems.length || 1;
          const normalReserved = normalN * normalMinPct;
          const normalRemaining = Math.max(0, 100 - normalReserved);
          const normalDurations = normalItems.map(p => {
            if (p.status === "running" || p.status === "waiting_for_input") return runningPhaseDurationMins;
            const m = parseDurationMinutes(p.duration);
            return m > 0 ? m : 0.1;
          });
          const normalTotalDur = normalDurations.reduce((s, d) => s + d, 0) || 1;
          const normalPcts = normalDurations.map(d => normalMinPct + (d / normalTotalDur) * normalRemaining);

          // 2. Calculate COMPRESSED widths
          const compressedMinPct = 5.0;
          const compressedRestCount = activePhases.length + futurePhases.length;
          const hasCompletedSummary = completedPhases.length > 0;
          const summaryWidth = hasCompletedSummary ? 16.0 : 0.0;
          
          let compressedPctsMap = new Map<string, number>();
          if (compressedRestCount === 0) {
            if (hasCompletedSummary) {
              compressedPctsMap.set("summary", 100.0);
            }
          } else {
            const reserved = compressedRestCount * compressedMinPct;
            const remaining = Math.max(0, (100.0 - summaryWidth) - reserved);
            const activeAndFutureDurations = [...activePhases, ...futurePhases].map(p => {
              if (p.status === "running" || p.status === "waiting_for_input") return runningPhaseDurationMins;
              return 0.1;
            });
            const totalRestDur = activeAndFutureDurations.reduce((s, d) => s + d, 0) || 1;
            const restPcts = activeAndFutureDurations.map(d => compressedMinPct + (d / totalRestDur) * remaining);
            
            let rIdx = 0;
            activePhases.forEach(p => {
              compressedPctsMap.set(p.phase, restPcts[rIdx++]);
            });
            futurePhases.forEach(p => {
              compressedPctsMap.set(p.phase, restPcts[rIdx++]);
            });
          }

          return (
            <>
              <div style={{
                maxHeight: showGantt ? "180px" : "0px",
                opacity: showGantt ? 1 : 0,
                overflow: "hidden",
                transition: "max-height 0.4s cubic-bezier(0.4, 0, 0.2, 1), opacity 0.3s ease, margin-top 0.4s ease, padding-top 0.4s ease",
                marginTop: showGantt ? tokens.spacingVerticalM : "0px",
                borderTop: showGantt ? `1px dashed ${tokens.colorNeutralStroke2}` : "none",
                paddingTop: showGantt ? tokens.spacingVerticalS : "0px"
              }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <div style={{ display: "flex", alignItems: "center" }}>
                    <Text size={100} weight="semibold" style={{ color: tokens.colorNeutralForeground4, textTransform: "uppercase", letterSpacing: "0.5px" }}>
                      Phase Duration Timeline Analysis (Gantt)
                    </Text>
                    <Checkbox
                      label={<span style={{ fontSize: "10px" }}>Summarize Completed</span>}
                      checked={compressCompleted}
                      onChange={(_, data) => setCompressCompleted(!!data.checked)}
                      style={{
                        marginLeft: tokens.spacingHorizontalM,
                        color: tokens.colorNeutralForeground3,
                      }}
                    />
                  </div>
                  <Button
                    size="small"
                    appearance="subtle"
                    onClick={() => setShowGantt(false)}
                    style={{ height: "auto", padding: "2px 4px", fontSize: "10px", color: tokens.colorNeutralForeground4 }}
                  >
                    Hide
                  </Button>
                </div>
                <div style={{
                  display: "flex",
                  height: "26px",
                  borderRadius: tokens.borderRadiusMedium,
                  overflow: "hidden",
                  backgroundColor: tokens.colorNeutralBackground3,
                  marginTop: tokens.spacingVerticalXXS,
                  boxShadow: "inset 0 1px 3px rgba(0,0,0,0.2)",
                  border: `1px solid ${tokens.colorNeutralStroke1}`
                }}>
                  {/* 1. Completed Summary Block */}
                  {(() => {
                    const hasSummary = completedPhases.length > 0;
                    const summaryPct = compressCompleted && hasSummary ? (compressedRestCount === 0 ? 100.0 : 16.0) : 0.0;
                    const summaryOpacity = compressCompleted && hasSummary ? 1 : 0;
                    
                    const tooltipNode = (
                      <div style={{ padding: "6px" }}>
                        <Text weight="bold" style={{ display: "block", marginBottom: "4px" }}>
                          Completed Phases ({completedPhases.length}):
                        </Text>
                        <div style={{ display: "flex", flexDirection: "column", gap: "3px" }}>
                          {completedPhases.map((cp) => {
                            const mins = parseDurationMinutes(cp.duration);
                            const durStr = cp.status === "skipped" ? "skipped" : mins > 0.1 ? `${mins.toFixed(1)} min` : "<0.1 min";
                            return (
                              <div key={cp.phase} style={{ display: "flex", justifyContent: "space-between", gap: "16px", fontSize: "11px" }}>
                                <span style={{ color: tokens.colorNeutralForeground2 }}>{cp.phase}:</span>
                                <span style={{ fontWeight: tokens.fontWeightBold }}>{durStr}</span>
                              </div>
                            );
                          })}
                        </div>
                        <div style={{ borderTop: `1px solid ${tokens.colorNeutralStroke2}`, marginTop: "6px", paddingTop: "6px", display: "flex", justifyContent: "space-between", fontSize: "11px", fontWeight: tokens.fontWeightBold }}>
                          <span>Total Time:</span>
                          <span>{completedPhasesSumMins.toFixed(1)} min</span>
                        </div>
                      </div>
                    );

                    const handleSummaryClick = () => {
                      const firstCompleted = completedPhases[0];
                      if (firstCompleted) {
                        scrollToCard(firstCompleted.phase, true);
                      }
                    };

                    return (
                      <Tooltip key="completed-summary-block" content={tooltipNode} relationship="label">
                        <div
                          onClick={handleSummaryClick}
                          style={{
                            width: `${summaryPct}%`,
                            opacity: summaryOpacity,
                            pointerEvents: summaryPct > 0.5 ? "auto" : "none",
                            backgroundColor: tokens.colorPaletteGreenBackground2,
                            borderRight: summaryPct > 0.5 ? `1.5px solid ${tokens.colorNeutralBackground1}` : "0px solid transparent",
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "center",
                            overflow: "hidden",
                            cursor: "pointer",
                            transition: "width 0.4s cubic-bezier(0.4, 0, 0.2, 1), opacity 0.3s ease, border-right-width 0.4s ease",
                            position: "relative",
                            boxShadow: "inset 0 0 6px rgba(0, 0, 0, 0.15)",
                            height: "100%",
                            flexShrink: 0
                          }}
                        >
                          <span style={{
                            fontSize: "9px",
                            fontWeight: tokens.fontWeightBold,
                            color: "#ffffff",
                            textShadow: "0 1px 2px rgba(0, 0, 0, 0.6)",
                            whiteSpace: "nowrap",
                            textOverflow: "ellipsis",
                            overflow: "hidden",
                            padding: "0 4px"
                          }}>
                            {summaryPct < 7.0 ? "✓" : `✓ ${completedPhases.length} Done`}
                          </span>
                        </div>
                      </Tooltip>
                    );
                  })()}

                  {/* 2. Individual Phase Blocks */}
                  {allPhasesNormalized.map((p, pIdx) => {
                    const isComp = p.status === "succeeded" || p.status === "skipped";
                    const isActive = p.status === "running" || p.status === "waiting_for_input";
                    const isFut = p.status === "pending";

                    // Determine normal and compressed widths
                    let normalPct = 0;
                    const nIdx = normalItems.findIndex(ni => ni.phase === p.phase);
                    if (nIdx >= 0) {
                      normalPct = normalPcts[nIdx];
                    }

                    let compressedPct = 0;
                    if (compressCompleted) {
                      if (isComp) {
                        compressedPct = 0;
                      } else {
                        compressedPct = compressedPctsMap.get(p.phase) ?? 5.0;
                      }
                    } else {
                      compressedPct = 0;
                    }

                    const pct = compressCompleted ? compressedPct : normalPct;
                    const opacity = compressCompleted ? (isComp ? 0 : 1) : (isFut ? 0 : 1);
                    const pointerEvents = pct > 0.5 && opacity > 0.1 ? "auto" : "none";

                    // Styles and color coding
                    let bgColor = tokens.colorPaletteBlueBackground2;
                    let textColor = "#ffffff";
                    const mins = isActive ? runningPhaseDurationMins : parseDurationMinutes(p.duration);
                    let label = `${mins.toFixed(1)}m`;

                    if (isActive) {
                      bgColor = tokens.colorPaletteYellowBackground2;
                      textColor = "#111111";
                      label = "⋯";
                    } else if (p.status === "skipped") {
                      bgColor = tokens.colorNeutralBackground3;
                      textColor = "#333333";
                      label = "—";
                    } else if (isFut) {
                      bgColor = tokens.colorNeutralBackground2;
                      textColor = tokens.colorNeutralForeground4;
                      label = "⏱";
                    } else if (mins > 6.0) {
                      bgColor = tokens.colorPaletteRedBackground2;
                    }

                    const tooltipContent = isFut 
                      ? `${p.phase}: Pending / Not started yet (Click to scroll)`
                      : `${p.phase}: ${mins > 0.1 ? `${mins.toFixed(1)} min` : isActive ? "active / in progress" : p.status === "skipped" ? "skipped" : "completed"} (Click to scroll)`;

                    return (
                      <Tooltip key={`gantt-item-${p.phase}-${pIdx}`} content={tooltipContent} relationship="label">
                        <div
                          onClick={() => scrollToCard(p.phase)}
                          className={isActive ? "gantt-running-striped" : ""}
                          style={{
                            width: `${pct}%`,
                            opacity: opacity,
                            pointerEvents,
                            backgroundColor: bgColor,
                            borderRight: pct > 0.5 ? `1.5px solid ${tokens.colorNeutralBackground1}` : "0px solid transparent",
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "center",
                            overflow: "hidden",
                            cursor: "pointer",
                            transition: "width 0.4s cubic-bezier(0.4, 0, 0.2, 1), opacity 0.3s ease, border-right-width 0.4s ease, background-color 0.3s ease",
                            position: "relative",
                            flexShrink: 0,
                            height: "100%",
                            ...(mins > 6.0 && !isActive && !isFut ? { boxShadow: "inset 0 0 8px rgba(255, 77, 77, 0.4)" } : {}),
                            ...(isFut ? { border: `1px dashed ${tokens.colorNeutralStroke1}`, boxSizing: "border-box" } : {})
                          }}
                        >
                          <span style={{
                            fontSize: "9px",
                            fontWeight: tokens.fontWeightBold,
                            color: textColor,
                            textShadow: textColor === "#ffffff" ? "0 1px 2px rgba(0, 0, 0, 0.6)" : "none",
                            whiteSpace: "nowrap",
                            textOverflow: "ellipsis",
                            overflow: "hidden",
                            padding: "0 4px"
                          }}>
                            {pct < 7.0 ? (isActive ? "⋯" : isComp ? "✓" : "—") : label}
                          </span>
                        </div>
                      </Tooltip>
                    );
                  })}
                </div>
                
                {/* Gantt Timeline Status Legend */}
                <div style={{ display: "flex", flexWrap: "wrap", gap: tokens.spacingHorizontalM, marginTop: tokens.spacingVerticalS, justifyContent: "center" }}>
                  {compressCompleted && completedPhases.length > 0 && (
                    <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS }}>
                      <div style={{ width: "12px", height: "12px", borderRadius: "3px", backgroundColor: tokens.colorPaletteGreenBackground2 }} />
                      <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>Completed Summary</Text>
                    </div>
                  )}
                  <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS }}>
                    <div style={{ width: "12px", height: "12px", borderRadius: "3px", backgroundColor: tokens.colorPaletteBlueBackground2 }} />
                    <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>Succeeded</Text>
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS }}>
                    <div className="gantt-running-striped" style={{
                      width: "12px",
                      height: "12px",
                      borderRadius: "3px",
                      backgroundColor: tokens.colorPaletteYellowBackground2
                    }} />
                    <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>In Progress (Live Growth)</Text>
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS }}>
                    <div style={{ width: "12px", height: "12px", borderRadius: "3px", backgroundColor: tokens.colorNeutralBackground3, border: `1px solid ${tokens.colorNeutralStroke2}` }} />
                    <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>Skipped</Text>
                  </div>
                  {compressCompleted && futurePhases.length > 0 && (
                    <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS }}>
                      <div style={{ width: "12px", height: "12px", borderRadius: "3px", backgroundColor: tokens.colorNeutralBackground2, border: `1px dashed ${tokens.colorNeutralStroke1}` }} />
                      <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>Pending</Text>
                    </div>
                  )}
                  <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS }}>
                    <div style={{ width: "12px", height: "12px", borderRadius: "3px", backgroundColor: tokens.colorPaletteRedBackground2, boxShadow: "0 0 4px rgba(255, 77, 77, 0.4)" }} />
                    <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>Slow Phase ({">"}6m)</Text>
                  </div>
                </div>
              </div>

              {!showGantt && (
                <div style={{ display: "flex", justifyContent: "flex-end", marginTop: tokens.spacingVerticalS }}>
                  <Button
                    size="small"
                    appearance="subtle"
                    onClick={() => setShowGantt(true)}
                    style={{ height: "auto", padding: "2px 4px", fontSize: "10px", color: tokens.colorBrandForeground1 }}
                  >
                    Show Duration Timeline (Gantt)
                  </Button>
                </div>
              )}
            </>
          );
        })()}
      </div>

      <Card className={styles.configCard} size="small">
        <CardHeader
          header={<Text weight="semibold" size={300}>Operator diagnostics</Text>}
          action={<Button size="small" appearance="subtle" icon={<ClipboardRegular />} onClick={copyDiagnostics}>Copy diagnostics</Button>}
        />
        <div className={styles.configGrid}>
          <span className={styles.configItem}><Badge color="informative" size="small">Elapsed</Badge> {elapsedFormatted || "0m 0s"}</span>
          <span className={styles.configItem}><Badge color="brand" size="small">ETA</Badge> {isRunning && etaMinutes > 0 ? `~${etaMinutes}m` : "—"}</span>
          <span className={styles.configItem}><Badge color="subtle" size="small">Logs</Badge> {(backendLogs ?? []).length}</span>
          <span
            className={styles.clickableConfigItem}
            onClick={() => {
              setDrilldownType("error");
              setDrilldownSearch("");
            }}
            title="Click to view error log details"
          >
            <Badge color={logCounts.error ? "danger" : "success"} size="small">Errors</Badge> {logCounts.error ?? 0}
          </span>
          <span
            className={styles.clickableConfigItem}
            onClick={() => {
              setDrilldownType("warn");
              setDrilldownSearch("");
            }}
            title="Click to view warning log details"
          >
            <Badge color={logCounts.warn || logCounts.warning ? "warning" : "subtle"} size="small">Warnings</Badge> {(logCounts.warn ?? 0) + (logCounts.warning ?? 0)}
          </span>
        </div>
      </Card>

      {/* Deployment / Teardown Configuration Summary */}
      {(() => {
        const cs = status?.customStatus as Record<string, unknown> | null;
        const cfg = cs?.deployConfig as Record<string, unknown> | undefined;
        if (isTeardown) {
          // Teardown config card
          const wsName = cs?.workspaceName as string || "";
          const rgName = cs?.resourceGroupName as string || "";
          const targets = cs?.teardownTargets as string[] | undefined;
          return (
            <Card className={styles.configCard} size="small">
              <CardHeader header={<Text weight="semibold" size={300}>Teardown Configuration</Text>} />
              <div className={styles.configGrid}>
                {wsName && <span className={styles.configItem}><Badge color="brand" size="small">Workspace</Badge> {wsName}</span>}
                {rgName && <span className={styles.configItem}><Badge color="informative" size="small">Resource Group</Badge> {rgName}</span>}
                {targets && targets.map((t, i) => <span key={i} className={styles.configItem}><Badge color="warning" size="small">Target</Badge> {t}</span>)}
              </div>
            </Card>
          );
        }
        if (!cfg) return null;
        // Deployment config card
        const COMPONENTS = [
          { key: "skip_base_infra", label: "Azure Emulator Infra", phase: 1 },
          { key: "skip_fhir", label: "FHIR Service + Loader", phase: 1 },
          { key: "skip_synthea", label: "Synthea Patients", phase: 1 },
          { key: "skip_device_assoc", label: "Device Associations", phase: 1 },
          { key: "skip_dicom", label: "DICOM Download", phase: 1 },
          { key: "skip_fabric", label: "Fabric RTI", phase: 1 },
          { key: "skip_fhir_export", label: "FHIR $export", phase: 1 },
          { key: "skip_rti_phase2", label: "RTI Phase 2", phase: 2 },
          { key: "skip_hds_pipelines", label: "HDS Pipelines", phase: 2 },
          { key: "skip_data_agents", label: "Data Agents", phase: 2 },
          { key: "skip_imaging", label: "Imaging Toolkit", phase: 3 },
          { key: "skip_ontology", label: "Ontology", phase: 4 },
          { key: "skip_activator", label: "Data Activator", phase: 4 },
          { key: "skip_quality_measures", label: "Population Health & Quality Dashboard", phase: 5 },
        ];
        const enabled = COMPONENTS.filter((c) => !cfg[c.key]);
        const skipped = COMPONENTS.filter((c) => cfg[c.key]);
        return (
          <Card className={styles.configCard} size="small">
            <CardHeader header={<Text weight="semibold" size={300}>Deployment Configuration</Text>} />
            <div className={styles.configGrid}>
              {(cfg.fabric_workspace_name as string) && (
                <span className={styles.configItem}><Badge color="brand" size="small">Workspace</Badge> {cfg.fabric_workspace_name as string}</span>
              )}
              {(cfg.resource_group_name as string) && (
                <span className={styles.configItem}><Badge color="informative" size="small">RG</Badge> {cfg.resource_group_name as string}</span>
              )}
              {(cfg.patient_count as number) > 0 && (
                <span className={styles.configItem}><Badge color="subtle" size="small">Patients</Badge> {cfg.patient_count as number}</span>
              )}
              {(cfg.alert_email as string) && (
                <span className={styles.configItem}><Badge color="subtle" size="small">Alerts</Badge> {cfg.alert_email as string}</span>
              )}
            </div>
            <div className={styles.configGrid} style={{ marginTop: tokens.spacingVerticalXS }}>
              {enabled.map((c) => (
                <span key={c.key} className={styles.configItem}>
                  <span style={{ color: tokens.colorPaletteGreenForeground1 }}>✓</span> {c.label}
                </span>
              ))}
              {skipped.map((c) => (
                <span key={c.key} className={styles.configItem} style={{ color: tokens.colorNeutralForeground4 }}>
                  <span>—</span> {c.label}
                </span>
              ))}
            </div>
          </Card>
        );
      })()}

      {error && (
        <MessageBar intent="error">
          <MessageBarBody>{error}</MessageBarBody>
        </MessageBar>
      )}

      {/* Direct Lake Connection Authorization Prompt */}
      {(() => {
        const cs = status?.customStatus as Record<string, unknown> | null;
        const links = cs?.links as Record<string, string> | undefined;
        const settingsUrl = links?.imagingReportSettings;
        if (!settingsUrl) return null;
        return (
          <MessageBar intent="warning" style={{ marginBottom: tokens.spacingVerticalM, border: `1px solid ${tokens.colorPaletteYellowBorder1}` }}>
            <MessageBarBody>
              <Text weight="semibold">Action Required:</Text> Authorize the Direct Lake connection to populate the dashboard with data.
              <Button
                as="a"
                appearance="subtle"
                href={settingsUrl}
                target="_blank"
                rel="noopener noreferrer"
                icon={<OpenRegular />}
                style={{ marginLeft: tokens.spacingHorizontalS }}
              >
                Sign in to Fabric Portal
              </Button>
            </MessageBarBody>
          </MessageBar>
        );
      })()}

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

      {/* Log view toggle */}
      {backendLogs && backendLogs.length > 0 && (
        <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: tokens.spacingVerticalS }}>
          <Tooltip
            content={showAllLogs ? "Show logs grouped by phase" : "Show all logs in a single stream"}
            relationship="label"
          >
            <Button
              appearance={showAllLogs ? "primary" : "outline"}
              icon={<TextBulletListRegular />}
              onClick={() => setShowAllLogs((v) => !v)}
              size="small"
            >
              {showAllLogs ? "Viewing: All Logs" : "View: All Logs"}
            </Button>
          </Tooltip>
        </div>
      )}

      {/* All Logs stream view */}
      {showAllLogs && backendLogs && backendLogs.length > 0 && (
        <AllLogsStream logs={backendLogs} />
      )}

      {/* Phase Cards with logs */}
      {!showAllLogs && (
      <div className={styles.phases} style={operatorMode ? { gap: tokens.spacingVerticalXXS } : undefined}>
        {(() => {
          // Build log-to-phase mapping by scanning for step transition markers.
          // Logs are a rolling buffer — we find phase boundaries by matching
          // phase names in the log messages and assigning ranges.
          const logPhaseMap = new Map<number, number[]>(); // phaseIndex → log indices
          if (!isMock && backendLogs && backendLogs.length > 0) {
            let currentPhaseIdx = -1;
            for (let logIdx = 0; logIdx < backendLogs.length; logIdx++) {
              const msg = backendLogs[logIdx].message.toUpperCase();
              // Check if this log starts a new phase by matching any phase name
              for (let pIdx = 0; pIdx < phases.length; pIdx++) {
                const pName = phases[pIdx].phase.toUpperCase();
                // Match step banners like "| STEP N: PHASE 1: FABRIC WORKSPACE |"
                // or phase names appearing in log lines
                if (msg.includes(pName) && (msg.includes("STEP") || msg.includes("╔") || msg.includes("───"))) {
                  currentPhaseIdx = pIdx;
                  break;
                }
              }
              if (currentPhaseIdx >= 0) {
                if (!logPhaseMap.has(currentPhaseIdx)) logPhaseMap.set(currentPhaseIdx, []);
                logPhaseMap.get(currentPhaseIdx)!.push(logIdx);
              }
            }
            // If no markers found (log buffer rotated past them), assign all logs
            // to the currently running phase or the last phase
            if (logPhaseMap.size === 0) {
              const runningIdx = phases.findIndex((p) => p.status === "running");
              const targetIdx = runningIdx >= 0 ? runningIdx : phases.length - 1;
              if (targetIdx >= 0) {
                logPhaseMap.set(targetIdx, backendLogs.map((_, i) => i));
              }
            }
          }

          return phases.map((phase, phaseIdx) => {
            let filteredLogs: Array<{timestamp: string; level: "info" | "warn" | "error" | "success"; message: string}>;
            if (isMock) {
              filteredLogs = (mockPhaseLogs.get(phase.phase) ?? []);
            } else if (logPhaseMap.has(phaseIdx) && backendLogs) {
              const indices = logPhaseMap.get(phaseIdx)!;
              filteredLogs = indices.map((i) => backendLogs[i]) as Array<{timestamp: string; level: "info" | "warn" | "error" | "success"; message: string}>;
            } else {
              filteredLogs = [];
            }

            return (
              <PhaseCard
                key={phase.phase}
                phase={phase}
                logs={filteredLogs}
                autoScroll={autoScroll}
                instanceId={instanceId}
              />
            );
          });
        })()}
      </div>
      )}

      {showAfterActionReport && (
        <Card
          ref={afterActionCardRef}
          className="spring-active"
          style={{ marginTop: tokens.spacingVerticalL, padding: tokens.spacingVerticalL, border: `1px solid ${tokens.colorPaletteBlueBorderActive}`, boxShadow: `0 0 16px ${tokens.colorPaletteBlueBorderActive}` }}
        >
          <CardHeader
            header={
              <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                <ShieldRegular style={{ color: tokens.colorPaletteBlueBorderActive, fontSize: "24px" }} />
                <Subtitle1 style={{ fontWeight: "bold" }}>Deployment Artifacts & Security</Subtitle1>
              </div>
            }
            description={
              <Text size={200} style={{ color: tokens.colorNeutralForeground4 }}>
                Governance, audit, and credential mappings for the deployed cloud environment
              </Text>
            }
          />

          {afterActionLoading ? (
            <div style={{ padding: "20px", textAlign: "center" }}>
              <Text>Compiling Live Environment Report...</Text>
            </div>
          ) : afterActionReport ? (
            <div style={{ display: "flex", flexDirection: "column", gap: "16px", marginTop: "12px" }}>
              <MessageBar intent="success" layout="multiline">
                <MessageBarBody>
                  <Text weight="semibold">Service-to-Service Security Architecture:</Text> Services
                  prefer <Text weight="bold">Managed Identities / Workspace Identities</Text> (no stored secrets) for cross-resource data flows. A 
                  dedicated <Text weight="bold">Service Principal (SPN)</Text> is utilized strictly for automated MS Fabric Direct Lake 
                  semantic model data connection authentication.
                </MessageBarBody>
              </MessageBar>

              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(250px, 1fr))", gap: "16px" }}>
                <Card style={{ backgroundColor: tokens.colorNeutralBackground2 }}>
                  <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>Active Governance Group</Text>
                  <Text size={500} weight="bold" style={{ color: tokens.colorPaletteBlueForeground2 }}>{afterActionReport.adminGroup}</Text>
                  <Text size={100} style={{ color: tokens.colorNeutralForeground4, marginTop: "4px" }}>
                    Members of this security group have full administrative and secret access.
                  </Text>
                </Card>
                <Card style={{ backgroundColor: tokens.colorNeutralBackground2 }}>
                  <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>Secret Key Store</Text>
                  <Text size={500} weight="bold" style={{ color: tokens.colorPaletteBlueForeground2 }}>{afterActionReport.keyVaultName}</Text>
                  <Text size={100} style={{ color: tokens.colorNeutralForeground4, marginTop: "4px" }}>
                    Azure Key Vault storing SPN appId/appKey and connection strings securely.
                  </Text>
                </Card>
              </div>

              <div style={{ marginTop: "8px" }}>
                <Text weight="semibold" block style={{ marginBottom: "8px" }}>Deployed Cloud Resource Identity Matrix</Text>
                <div style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", textAlign: "left", fontSize: "12px" }}>
                    <thead>
                      <tr style={{ borderBottom: `2px solid ${tokens.colorNeutralStroke2}`, paddingBottom: "8px" }}>
                        <th style={{ padding: "8px" }}>Resource / Item</th>
                        <th style={{ padding: "8px" }}>Platform</th>
                        <th style={{ padding: "8px" }}>Type</th>
                        <th style={{ padding: "8px" }}>Active Identity Strategy</th>
                        <th style={{ padding: "8px" }}>Secrets/Credentials Stored</th>
                        <th style={{ padding: "8px" }}>Access Governance & Role</th>
                      </tr>
                    </thead>
                    <tbody>
                      {afterActionReport.resources.map((res, i) => (
                        <tr key={i} style={{ borderBottom: `1px solid ${tokens.colorNeutralStroke1}`, backgroundColor: i % 2 === 0 ? "transparent" : tokens.colorNeutralBackground2 }}>
                          <td style={{ padding: "8px", fontWeight: "semibold" }}>{res.name}</td>
                          <td style={{ padding: "8px" }}>
                            <Badge color={res.category === "Azure" ? "informative" : "brand"}>{res.category}</Badge>
                          </td>
                          <td style={{ padding: "8px", color: tokens.colorNeutralForeground3 }}>{res.type}</td>
                          <td style={{ padding: "8px", color: tokens.colorPaletteBlueForeground2 }}>{res.identity}</td>
                          <td style={{ padding: "8px" }}>
                            <code style={{ fontSize: "10px", padding: "2px 4px", backgroundColor: tokens.colorNeutralBackground3, borderRadius: "4px" }}>
                              {res.credentialDetails}
                            </code>
                          </td>
                          <td style={{ padding: "8px", color: tokens.colorNeutralForeground2, maxWidth: "250px" }}>{res.accessControlDetails}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div style={{ display: "flex", gap: "12px", marginTop: "8px" }}>
                {afterActionReport.azurePortalUrl && (
                  <Button
                    appearance="subtle"
                    icon={<OpenRegular />}
                    onClick={() => window.open(afterActionReport.azurePortalUrl, "_blank")}
                  >
                    View in Azure Portal
                  </Button>
                )}
                {afterActionReport.fabricWorkspaceUrl && (
                  <Button
                    appearance="subtle"
                    icon={<OpenRegular />}
                    onClick={() => window.open(afterActionReport.fabricWorkspaceUrl, "_blank")}
                  >
                    Open Fabric Workspace
                  </Button>
                )}
              </div>

              {/* Deployed Resources */}
              {!isMock && (
                <DeployedResourcesPanel
                  deployedResources={deployedResources}
                  resourcesLoading={resourcesLoading}
                  resourceGroupName={status?.customStatus?.resourceGroupName as string || ""}
                  azurePortalUrl={(status?.customStatus as Record<string, unknown>)?.links
                    ? ((status?.customStatus as Record<string, unknown>)?.links as Record<string, string>)?.azurePortal
                    : undefined}
                />
              )}
            </div>
          ) : (
            <div style={{ padding: "20px", textAlign: "center" }}>
              <Text>No security report available for this instance.</Text>
            </div>
          )}
        </Card>
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
              if (isMock) {
                const newId = startMockDeployment(deployConfig);
                navigate(`/monitor/${newId}`);
              } else {
                const { instanceId: newId } = await startDeployment(deployConfig);
                navigate(`/monitor/${newId}`);
              }
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

      {/* Errors / Warnings Drilldown Dialog */}
      <Dialog open={drilldownType !== null} onOpenChange={(_, data) => { if (!data.open) setDrilldownType(null); }}>
        <DialogSurface style={{ maxWidth: "800px", width: "90%", backgroundColor: tokens.colorNeutralBackground1 }}>
          <DialogBody>
            <DialogTitle action={<Button appearance="subtle" icon={<DismissRegular />} onClick={() => setDrilldownType(null)} />}>
              <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                {drilldownType === "error" ? (
                  <ErrorCircleRegular style={{ color: tokens.colorPaletteRedBorderActive, fontSize: "24px" }} />
                ) : (
                  <WarningRegular style={{ color: tokens.colorPaletteYellowBorderActive, fontSize: "24px" }} />
                )}
                <Text weight="bold" size={400}>
                  {drilldownType === "error" ? `Drilldown: Errors (${filteredDrilldownLogs.length})` : `Drilldown: Warnings (${filteredDrilldownLogs.length})`}
                </Text>
              </div>
            </DialogTitle>
            <DialogContent style={{ display: "flex", flexDirection: "column", gap: tokens.spacingVerticalM, marginTop: tokens.spacingVerticalM }}>
              {/* Header Action Row */}
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: tokens.spacingHorizontalM }}>
                <Input
                  placeholder="Filter messages..."
                  value={drilldownSearch}
                  onChange={(_, data) => setDrilldownSearch(data.value)}
                  style={{ flex: 1 }}
                  size="small"
                />
                {filteredDrilldownLogs.length > 0 && (
                  <Button
                    size="small"
                    appearance="primary"
                    icon={<ClipboardRegular />}
                    onClick={() => {
                      const text = filteredDrilldownLogs
                        .map(log => `[${log.timestamp}] [${log.level.toUpperCase()}] ${log.message}`)
                        .join("\n");
                      navigator.clipboard?.writeText(text).catch(() => undefined);
                      setCopiedAll(true);
                      setTimeout(() => setCopiedAll(false), 2000);
                    }}
                  >
                    {copiedAll ? "Copied!" : drilldownType === "error" ? "Copy All Errors" : "Copy All Warnings"}
                  </Button>
                )}
              </div>

              {/* Scrollable list */}
              <div style={{ maxHeight: "400px", overflowY: "auto", paddingRight: "4px" }}>
                {filteredDrilldownLogs.length === 0 ? (
                  <div style={{ display: "flex", justifyContent: "center", padding: "40px 0", color: tokens.colorNeutralForeground4 }}>
                    <Text italic>No {drilldownType === "error" ? "errors" : "warnings"} found matching current filter.</Text>
                  </div>
                ) : (
                  filteredDrilldownLogs.map((log, index) => {
                    const logId = `${log.timestamp}-${index}`;
                    const fullLogStr = `[${log.timestamp}] [${log.level.toUpperCase()}] ${log.message}`;
                    return (
                      <div
                        key={logId}
                        style={{
                          display: "flex",
                          flexDirection: "column",
                          gap: "4px",
                          padding: "10px 12px",
                          borderRadius: "6px",
                          backgroundColor: tokens.colorNeutralBackground2,
                          borderLeft: `4px solid ${drilldownType === "error" ? tokens.colorPaletteRedBorderActive : tokens.colorPaletteYellowBorderActive}`,
                          marginBottom: "8px",
                          position: "relative",
                        }}
                      >
                        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }}>
                          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                            <Badge appearance="outline" color={drilldownType === "error" ? "danger" : "warning"} size="small">
                              {log.timestamp}
                            </Badge>
                            {log.phase !== undefined && (
                              <Text size={100} style={{ color: tokens.colorNeutralForeground3, fontWeight: tokens.fontWeightSemibold }}>
                                Phase {log.phase}
                              </Text>
                            )}
                          </div>
                          <Button
                            size="small"
                            appearance="subtle"
                            icon={<CopyRegular />}
                            title="Copy single message"
                            onClick={() => {
                              navigator.clipboard?.writeText(fullLogStr).catch(() => undefined);
                              setCopiedLogId(logId);
                              setTimeout(() => setCopiedLogId(null), 2000);
                            }}
                            style={{ height: "24px", minWidth: "55px", padding: "0 4px" }}
                          >
                            <Text size={100}>{copiedLogId === logId ? "Copied" : "Copy"}</Text>
                          </Button>
                        </div>
                        <Text style={{ fontFamily: "Cascadia Code, Consolas, monospace", fontSize: tokens.fontSizeBase100, color: tokens.colorNeutralForeground1, wordBreak: "break-all", whiteSpace: "pre-wrap" }}>
                          {log.message}
                        </Text>
                      </div>
                    );
                  })
                )}
              </div>
            </DialogContent>
            <DialogActions style={{ marginTop: tokens.spacingVerticalS }}>
              <Button appearance="secondary" onClick={() => setDrilldownType(null)}>Close</Button>
            </DialogActions>
          </DialogBody>
        </DialogSurface>
      </Dialog>
    </div>
  );
}
