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
  TextBulletListRegular,
  ArrowLeftRegular,
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
  // Phase 1: Infrastructure & Data
  { phase: "Phase 1: Fabric Workspace", status: "pending" },
  { phase: "Phase 1: Base Azure Infrastructure", status: "pending" },
  { phase: "Phase 1: FHIR Service + Synthea + Loader", status: "pending" },
  { phase: "Phase 1: DICOM Service + Loader", status: "pending" },
  { phase: "Phase 1: Fabric RTI", status: "pending" },
  { phase: "Phase 1: HDS Detection", status: "pending" },
  // Phase 2: Analytics & AI Agents
  { phase: "Phase 2: Fabric RTI", status: "pending" },
  { phase: "Phase 2: DICOM Shortcut + HDS Pipelines", status: "pending" },
  { phase: "Phase 2: Data Agents", status: "pending" },
  // Phase 3: Imaging & Reporting
  { phase: "Phase 3: Imaging & Reporting", status: "pending" },
  // Phase 4: Semantic Layer & Alerts
  { phase: "Phase 4: Ontology", status: "pending" },
  { phase: "Phase 4: Data Activator", status: "pending" },
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
  const [showAllLogs, setShowAllLogs] = useState(false);
  const [redeploying, setRedeploying] = useState(false);
  const [deployedResources, setDeployedResources] = useState<DeployedResourcesResult | null>(null);
  const [resourcesLoading, setResourcesLoading] = useState(false);
  const [lastResourceFetch, setLastResourceFetch] = useState(0);
  const [frozenElapsed, setFrozenElapsed] = useState<number | null>(null);
  const [tick, setTick] = useState(0);

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
        if (typeof p.duration === "string") {
          const m = p.duration.match(/([\d.]+)\s*min/i);
          if (m) return sum + parseFloat(m[1]) * 60;
        }
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
    { label: "Phase 1: Infrastructure & Data", phaseIndices: [0, 1, 2, 3, 4, 5], namePatterns: ["Fabric Workspace", "Azure Infrastructure", "FHIR", "DICOM", "Fabric RTI", "HDS Detection", "HDS Guidance"], position: 8, endWeight: 40, phaseNumber: 1 },
    { label: "Phase 2: Analytics & AI Agents", phaseIndices: [6, 7, 8], namePatterns: ["RTI Phase 2", "HDS Pipeline", "Data Agent", "Fabric RTI (auto)", "DICOM Shortcut"], position: 36, endWeight: 60, phaseNumber: 2 },
    { label: "Phase 3: Imaging & Reporting", phaseIndices: [9], namePatterns: ["Imaging", "Cohorting", "DICOM Viewer", "Reporting"], position: 64, endWeight: 70, phaseNumber: 3 },
    { label: "Phase 4: Semantic Layer & Alerts", phaseIndices: [10, 11], namePatterns: ["Ontology", "Activator", "Reflex", "Data Activator"], position: 90, endWeight: 80, phaseNumber: 4 },
  ];

  // ── Adaptive milestones: determine active milestones from instance ID ──
  // Instance ID format: P<milestone-digits>-<timestamp> (e.g. P1234-20260406-195906)
  // Legacy formats: ALLPHASES-*, PHASE2+-*, FABRIC-*, teardown*
  function getActiveMilestoneNumbers(): Set<number> {
    const id = instanceId ?? "";

    // New format: P followed by milestone digits (P1234, P234, P3, etc.)
    const pMatch = id.match(/^P(\d+)-/i);
    if (pMatch) {
      return new Set(pMatch[1].split("").map(Number).filter((n) => n >= 1 && n <= 4));
    }

    // Legacy formats
    if (id.startsWith("ALLPHASES")) return new Set([1, 2, 3, 4]);
    if (id.startsWith("PHASE2+")) return new Set([1, 2, 3, 4]); // skip_base_infra still has all milestones
    if (id.startsWith("FABRIC")) return new Set([1, 2, 3, 4]);   // reduced P1 but all milestones

    // Default: show all
    return new Set([1, 2, 3, 4]);
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
          <Badge
            color={isCancelled ? "warning" : isFailed ? "danger" : isComplete ? (isTeardown ? "warning" : "success") : isRunning ? "informative" : "subtle"}
            size="large"
          >
            {milestonesDone}/{totalMilestones} phases{" "}
            {isCancelled ? "cancelled" : isComplete ? (isTeardown ? "torn down" : "complete") : ""}{" "}
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
      <div className={styles.progressSection} style={isTeardown ? { boxShadow: `${tokens.shadow8}, 0 0 12px rgba(255, 185, 0, 0.25)` } : undefined}>
        <div className={styles.milestoneTrack}>
          {/* Background track line */}
          <div className={styles.trackLine} />
          {/* Filled track line; width is expressed in container % (track starts at 4%) */}
          <div
            className={styles.trackFill}
            style={{
              transform: `scaleX(${Math.max(0, Math.min(progressPct, 100)) / 100})`,
              backgroundColor: progressColor,
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
                  className={`${styles.milestoneDot} ${getDotClass(msStatus)}`}
                  style={isTeardown && msStatus === "done"
                    ? { backgroundColor: tokens.colorPaletteYellowForeground1, boxShadow: `0 0 0 3px ${tokens.colorNeutralBackground1}, 0 0 0 6px rgba(255, 185, 0, 0.35), ${tokens.shadow4}` }
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
            {elapsedFormatted || "0m 0s"}
          </Text>
        </div>
      </div>

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
      <div className={styles.phases}>
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

      {/* Deployed Resources */}
      {(isComplete || completedCount > 0) && !isMock && (
        <DeployedResourcesPanel
          deployedResources={deployedResources}
          resourcesLoading={resourcesLoading}
          resourceGroupName={status?.customStatus?.resourceGroupName as string || ""}
          azurePortalUrl={(status?.customStatus as Record<string, unknown>)?.links
            ? ((status?.customStatus as Record<string, unknown>)?.links as Record<string, string>)?.azurePortal
            : undefined}
        />
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
