import { useEffect, useState, useCallback, useRef } from "react";
import { useParams, useNavigate } from "react-router-dom";
import * as XLSX from "xlsx";
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
  DocumentTableRegular,
  DocumentTextRegular,
} from "@fluentui/react-icons";
import { PhaseCard } from "../components/PhaseCard";
import { AllLogsStream } from "../components/AllLogsStream";
import { DeployedResourcesPanel } from "../components/DeployedResourcesPanel";
import { AzureBadge, FabricBadge } from "../components/TypeBadges";
import {
  getDeploymentStatus,
  resumeAfterHds,
  cancelDeployment,
  startDeployment,
  continueFailedDeployment,
  getDeployedResources,
  getAfterActionReport,
  getCloudState,
  validateRun,
  continuePhase7,
  type DeploymentStatus,
  type DeploymentConfig,
  type PhaseInfo,
  type DeployedResourcesResult,
  type AfterActionReportResult,
  type CloudStateResult,
  type ValidationResult,
  type PhaseSubStep,
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
    height: "148px",
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
    width: "132px",
    minHeight: "44px",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    gap: "6px",
    padding: `${tokens.spacingVerticalXXS} ${tokens.spacingHorizontalXS}`,
    color: tokens.colorNeutralForeground2,
    textAlign: "center" as const,
    whiteSpace: "normal" as const,
    lineHeight: tokens.lineHeightBase200,
    borderRadius: tokens.borderRadiusMedium,
    backgroundColor: tokens.colorNeutralBackground1,
    border: `1px solid ${tokens.colorNeutralStroke2}`,
    boxShadow: tokens.shadow2,
    boxSizing: "border-box" as const,
  },
  milestoneLabelNumber: {
    flex: "0 0 auto",
    minWidth: "18px",
    height: "18px",
    padding: "0 4px",
    borderRadius: tokens.borderRadiusCircular,
    backgroundColor: tokens.colorNeutralBackground3,
    color: tokens.colorNeutralForeground2,
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    fontSize: tokens.fontSizeBase100,
    fontWeight: tokens.fontWeightBold,
    lineHeight: "18px",
  },
  milestoneLabelText: {
    minWidth: 0,
    maxWidth: "96px",
    fontSize: tokens.fontSizeBase100,
    fontWeight: tokens.fontWeightSemibold,
    lineHeight: tokens.lineHeightBase100,
    overflowWrap: "anywhere" as const,
  },
  milestoneLabelActive: {
    color: tokens.colorBrandForeground1,
    border: `1px solid ${tokens.colorBrandStroke1}`,
    backgroundColor: tokens.colorBrandBackground2,
  },
  milestoneLabelDone: {
    backgroundColor: tokens.colorBrandForeground1,
    color: "#ffffff",
    border: `1px solid ${tokens.colorBrandForeground1}`,
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
  actionRequired: {
    marginBottom: tokens.spacingVerticalL,
    padding: tokens.spacingHorizontalL,
    backgroundColor: tokens.colorStatusWarningBackground1,
    border: `1px solid ${tokens.colorPaletteYellowBorderActive}`,
    borderLeft: `4px solid ${tokens.colorPaletteRedBorderActive}`,
    borderRadius: tokens.borderRadiusMedium,
    boxShadow: tokens.shadow4,
  },
  actionRequiredList: {
    display: "flex",
    flexDirection: "column" as const,
    gap: tokens.spacingVerticalS,
    marginTop: tokens.spacingVerticalS,
  },
  actionRequiredItem: {
    display: "flex",
    flexDirection: "column" as const,
    gap: tokens.spacingVerticalXXS,
    padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalM}`,
    backgroundColor: tokens.colorNeutralBackground1,
    borderRadius: tokens.borderRadiusMedium,
    border: `1px solid ${tokens.colorNeutralStroke2}`,
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
  floatingResumeBtn: {
    position: "fixed" as const,
    right: "32px",
    bottom: "96px",
    zIndex: 21,
    boxShadow: tokens.shadow16,
    border: `1px solid ${tokens.colorStatusWarningBorderActive}`,
  },
  floatingContinueFailedBtn: {
    position: "fixed" as const,
    left: "32px",
    bottom: "96px",
    zIndex: 21,
    boxShadow: tokens.shadow16,
    border: `1px solid ${tokens.colorPaletteRedBorderActive}`,
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
  { id: "phase_1_workspace", phase: "1a. Data Fabric Foundation: Fabric Workspace", status: "pending", milestone: 1 },
  { id: "phase_1_base_infra", phase: "1b. Data Fabric Foundation: Base Azure Infrastructure", status: "pending", milestone: 1 },
  { id: "phase_1_fhir", phase: "1c. Data Fabric Foundation: FHIR Service + Synthea + Loader", status: "pending", milestone: 1 },
  { id: "phase_1_dicom", phase: "1d. Data Fabric Foundation: DICOM Loader + ImagingStudy linkage", status: "pending", milestone: 1 },
  { id: "phase_2_rti_ingest", phase: "2a. Active Patient Telemetry: Fabric RTI Ingest", status: "pending", milestone: 2 },
  { id: "phase_2_rti_enrichment", phase: "2b. Active Patient Telemetry: Fabric RTI Enrichment", status: "pending", milestone: 2 },
  { id: "phase_3_hds_detection", phase: "3a. HDS Bridge + Row Gates: HDS Deployment Detection", status: "pending", milestone: 3 },
  { id: "phase_3_hds_pipelines", phase: "3b. HDS Bridge + Row Gates: DICOM Shortcut + HDS Pipelines", status: "pending", milestone: 3 },
  { id: "phase_4_imaging", phase: "4a. Semantic Intelligence & UX: Custom SWA Viewer & Direct Lake", status: "pending", milestone: 4 },
  { id: "phase_4_ontology", phase: "4b. Semantic Intelligence & UX: Clinical Device Ontology", status: "pending", milestone: 4 },
  { id: "phase_4_agents", phase: "4c. Semantic Intelligence & UX: Conversational Data Agents", status: "pending", milestone: 4 },
  { id: "phase_5_alerts", phase: "5. Bedside Alerting & Action: Real-Time Reflex alerts", status: "pending", milestone: 5 },
  { id: "phase_6_quality", phase: "6. Population Health & Quality: Full analytics pipeline", status: "pending", milestone: 6 },
  { id: "phase_7_payer_ops", phase: "7. Payer RTI & Ops: Claim stream, scoring, activator, and agents", status: "pending", milestone: 7 },
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
  const [continuingFailed, setContinuingFailed] = useState(false);
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
  const [cloudState, setCloudState] = useState<CloudStateResult | null>(null);
  const [validation, setValidation] = useState<ValidationResult | null>(null);
  const [validating, setValidating] = useState(false);
  const [continuingPhase7, setContinuingPhase7] = useState(false);
  const [resumingHds, setResumingHds] = useState(false);
  const [hasAutoExported, setHasAutoExported] = useState(false);
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

  const exportToCSV = () => {
    if (!afterActionReport) return;
    const headers = ["Resource / Item", "Platform", "Type", "Active Identity Strategy", "Secrets/Credentials Stored", "Access Governance & Role"];
    const rows = afterActionReport.resources.map(res => [
      res.name,
      res.category,
      res.type,
      res.identity,
      res.credentialDetails,
      res.accessControlDetails
    ]);
    const csvContent = [headers, ...rows]
      .map(e => e.map(val => `"${String(val).replace(/"/g, '""')}"`).join(","))
      .join("\n");
    
    // Add UTF-8 BOM to prevent Excel warning or encoding/corruption warnings
    const blob = new Blob(["\uFEFF" + csvContent], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.setAttribute("href", url);
    link.setAttribute("download", `security_artifacts_report_${instanceId}.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const exportToXLSX = () => {
    if (!afterActionReport) return;
    const headers = ["Resource / Item", "Platform", "Type", "Active Identity Strategy", "Secrets/Credentials Stored", "Access Governance & Role"];
    const data = afterActionReport.resources.map(res => [
      res.name,
      res.category,
      res.type,
      res.identity,
      res.credentialDetails,
      res.accessControlDetails
    ]);

    const worksheet = XLSX.utils.aoa_to_sheet([headers, ...data]);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Security & Artifacts");
    XLSX.writeFile(workbook, `security_artifacts_report_${instanceId}.xlsx`);
  };

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
  const isValidationReconciled = Boolean(
    (status?.customStatus as Record<string, unknown> | null)?.validationReconciled
  );

  // Detect if this is a teardown run
  const isTeardown = (status?.customStatus as Record<string, unknown>)?.runType === "teardown"
    || (instanceId ?? "").toLowerCase().startsWith("teardown");

  useEffect(() => {
    if (!instanceId || isMock) return;
    let cancelled = false;
    const refreshCloudState = () => {
      getCloudState(instanceId, isTeardown)
        .then((state) => { if (!cancelled) setCloudState(state); })
        .catch(() => { /* non-fatal */ });
    };
    refreshCloudState();
    const interval = window.setInterval(refreshCloudState, isTeardown && isRunning ? 10000 : 30000);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [instanceId, isMock, isTeardown, isRunning]);

  const runValidation = async () => {
    if (!instanceId) return;
    setValidating(true);
    setError("");
    try {
      setValidation(await validateRun(instanceId, isTeardown));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Validation failed");
    } finally {
      setValidating(false);
    }
  };

  const runPhase7Continuation = async () => {
    if (!instanceId) return;
    setContinuingPhase7(true);
    setError("");
    try {
      const result = await continuePhase7(instanceId);
      navigate(`/monitor/${result.instanceId}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Unable to start Phase 7 continuation");
    } finally {
      setContinuingPhase7(false);
    }
  };

  // Track completion of a new active deployment and auto-open report if auto-export is selected
  useEffect(() => {
    if (isComplete && !isTeardown && status?.runtimeStatus === "Completed" && !hasAutoExported) {
      const autoXlsx = localStorage.getItem("autoExportXlsx") === "true";
      const autoCsv = localStorage.getItem("autoExportCsv") === "true";
      if (autoXlsx || autoCsv) {
        setHasAutoExported(true);
        setShowAfterActionReport(true);
      }
    }
  }, [isComplete, isTeardown, status, hasAutoExported]);

  // Handle auto-export download triggering when data is loaded
  useEffect(() => {
    if (afterActionReport && showAfterActionReport) {
      const autoXlsx = localStorage.getItem("autoExportXlsx") === "true";
      const autoCsv = localStorage.getItem("autoExportCsv") === "true";
      
      const hasDownloadedKey = `autoExported_${instanceId}`;
      const alreadyDownloaded = sessionStorage.getItem(hasDownloadedKey) === "true";
      
      if (!alreadyDownloaded) {
        sessionStorage.setItem(hasDownloadedKey, "true");
        if (autoXlsx) {
          setTimeout(() => {
            exportToXLSX();
          }, 600);
        }
        if (autoCsv) {
          setTimeout(() => {
            exportToCSV();
          }, 800);
        }
      }
    }
  }, [afterActionReport, showAfterActionReport, instanceId]);

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

  const subStepsByPhase: Record<string, PhaseSubStep[]> = status?.customStatus?.subStepsByPhase ?? {};
  if (!isMock && Object.keys(subStepsByPhase).length > 0) {
    phases = phases.map((phase) => ({
      ...phase,
      subSteps: subStepsByPhase[phase.phase] ?? phase.subSteps,
    }));
    if (currentPhase && subStepsByPhase[currentPhase] && !phases.some((phase) => phase.phase === currentPhase)) {
      phases = [...phases, { phase: currentPhase, status: isRunning ? "running" : "succeeded", subSteps: subStepsByPhase[currentPhase] }];
    }
  }

  // Get logs from backend customStatus.logs (for real deployments).
  // Backend phase values are strings for local FastAPI runs and may be numeric in older persisted runs.
  const backendLogs = (status?.customStatus as Record<string, unknown>)?.logs as Array<{timestamp: string; level: string; message: string; phase?: string | number}> | undefined;

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

  const logExplainers = [
    { match: /GoldCareGaps|care-gap setup/i, label: "Gold care-gap fallback", detail: "Known fallback: payer care-gap functions use an empty-schema fallback when the gold table is unavailable." },
    { match: /Direct Lake|AUTHORIZE DATA CONNECTION/i, label: "Direct Lake authorization", detail: "Actionable: sign in to Fabric and authorize the semantic model connection." },
    { match: /Bicep release/i, label: "Bicep update available", detail: "Non-blocking CLI version notice." },
    { match: /pipeline did not complete|did not complete in time/i, label: "HDS pipeline timeout", detail: "Often non-blocking; rerun validation after Fabric finishes background processing." },
  ].filter((item) => (backendLogs ?? []).some((log) => item.match.test(log.message)));

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
    if (!instanceId || resumingHds) return;
    setResumingHds(true);
    setError("");
    try {
      if (isMock) {
        resumeMockHds(instanceId);
      } else {
        await resumeAfterHds(instanceId);
        await poll();
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to resume");
    } finally {
      setResumingHds(false);
    }
  };

  const handleContinueFailed = async () => {
    if (!instanceId || continuingFailed) return;
    const deployConfig = (status?.customStatus as Record<string, unknown>)?.deployConfig as DeploymentConfig | undefined;
    if (!deployConfig) {
      setError("Original deployment config not available. Please start a new deployment from the Deploy tab.");
      return;
    }
    setContinuingFailed(true);
    setError("");
    try {
      if (isMock) {
        const newId = startMockDeployment(deployConfig);
        navigate(`/monitor/${newId}`);
      } else {
        const { instanceId: newId } = await continueFailedDeployment(instanceId);
        navigate(`/monitor/${newId}`);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to continue deployment");
    } finally {
      setContinuingFailed(false);
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
    // Milestone 1 — Foundation: infra and patient-linked source data.
    { patterns: ["Fabric Workspace"], weight: 1 },
    { patterns: ["Base Azure Infrastructure", "Azure Infrastructure", "Shared HDS Infrastructure"], weight: 10 },
    { patterns: ["FHIR Service", "Synthea"], weight: 15 },
    { patterns: ["DICOM Loader", "ImagingStudy"], weight: 8 },
    // Milestone 2 — Active telemetry: Event Hub/Eventstream/Eventhouse/KQL.
    { patterns: ["Fabric RTI Ingest", "Fabric RTI Phase 1", "Phase 1: Fabric RTI", "Eventhouse", "Eventstream"], weight: 7 },
    { patterns: ["Fabric RTI Enrichment", "RTI Phase 2", "Phase 2: Fabric RTI", "Enrichment"], weight: 5 },
    // Milestone 3 — HDS bridge and row gates.
    { patterns: ["HDS Deployment", "HDS Detection", "HDS Guidance", "Healthcare Data Solutions"], weight: 1 },
    { patterns: ["DICOM Shortcut", "HDS Pipeline", "Pipeline Triggers", "Row Gates"], weight: 10 },
    // Milestone 4+ — optional enrichments, semantic layer, and UX.
    { patterns: ["Imaging", "Cohorting", "DICOM Viewer", "Direct Lake"], weight: 10 },
    { patterns: ["Ontology", "DeviceAssociation"], weight: 4 },
    { patterns: ["Data Agent", "Conversational"], weight: 3 },
    { patterns: ["Activator", "Reflex"], weight: 3 },
    { patterns: ["Quality", "Claims", "CMS", "Scorecard", "PDC"], weight: 12 },
    { patterns: ["Payer", "claim-stream", "HealthcareOpsAgent", "Graph Agent"], weight: 8 },
  ];

  function getStepWeight(phaseName: string): number {
    for (const sw of STEP_WEIGHTS) {
      if (sw.patterns.some((pat) => phaseName.toUpperCase().includes(pat.toUpperCase()))) {
        return sw.weight;
      }
    }

    // Legacy Durable Functions phases may still emit only a broad phase number.
    const phaseMatch = phaseName.match(/PHASE\s*(\d+)/i);
    if (phaseMatch) {
      const n = Number(phaseMatch[1]);
      if (n === 1) return 8;
      if (n === 2) return 6;
      if (n === 3) return 5;
      if (n === 4) return 5;
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
  type MilestoneDef = { label: string; shortLabel?: string; phaseIndices: number[]; namePatterns: string[]; position: number; endWeight: number; phaseNumber?: number };

  // Teardown-specific milestones: reverse order of deployment phases.
  // Teardown-specific milestones: describe actual teardown operations.
  const TEARDOWN_MILESTONES: MilestoneDef[] = [
    { label: "Workspace Items", phaseIndices: [0], namePatterns: ["Fabric Workspace Items"], position: 8, endWeight: 20 },
    { label: "Workspace Identity", phaseIndices: [1], namePatterns: ["Workspace Identity"], position: 36, endWeight: 40 },
    { label: "Workspace Deletion", phaseIndices: [2], namePatterns: ["Delete Workspace"], position: 64, endWeight: 60 },
    { label: "Azure Resources", phaseIndices: [3], namePatterns: ["Azure Resource Group"], position: 90, endWeight: 80 },
  ];

  const MILESTONES: MilestoneDef[] = [
    { label: "1. Data Fabric Foundation", shortLabel: "Foundation", phaseIndices: [0, 1, 2, 3], namePatterns: ["Fabric Workspace", "Base Azure Infrastructure", "FHIR", "Shared HDS Infrastructure", "DICOM Loader", "ImagingStudy"], position: 8, endWeight: 34, phaseNumber: 1 },
    { label: "2. Active Patient Telemetry", shortLabel: "Telemetry", phaseIndices: [4, 5], namePatterns: ["Fabric RTI", "Fabric RTI (auto)", "Telemetry", "Eventhouse", "Eventstream", "RTI Phase 2", "Enrichment"], position: 24, endWeight: 46, phaseNumber: 2 },
    { label: "3. HDS Bridge + Row Gates", shortLabel: "HDS Bridge", phaseIndices: [6, 7], namePatterns: ["HDS Deployment", "HDS Detection", "HDS Guidance", "Healthcare Data Solutions", "DICOM Shortcut", "HDS Pipelines", "Pipeline Triggers", "Row Gates"], position: 40, endWeight: 57, phaseNumber: 3 },
    { label: "4. Semantic Intelligence & UX", shortLabel: "Semantic UX", phaseIndices: [8, 9, 10], namePatterns: ["Imaging", "Cohorting", "DICOM Viewer", "Direct Lake", "Ontology", "DeviceAssociation", "Data Agent", "Conversational"], position: 56, endWeight: 74, phaseNumber: 4 },
    { label: "5. Bedside Alerting & Action", shortLabel: "Alerts", phaseIndices: [11], namePatterns: ["Activator", "Reflex"], position: 75, endWeight: 77, phaseNumber: 5 },
    { label: "6. Population Health & Quality", shortLabel: "Quality", phaseIndices: [12], namePatterns: ["Quality", "Claims", "CMS", "Scorecard", "PDC", "Adherence", "HCC", "RAF", "Readmission", "Utilization", "PMPM", "Star Rating"], position: 92, endWeight: 89, phaseNumber: 6 },
    { label: "7. Payer RTI & Ops", shortLabel: "Payer Ops", phaseIndices: [13], namePatterns: ["Payer", "Fraud", "HighCost", "CareGap", "claim-stream", "PayerOps", "HealthcareOpsAgent", "Graph Agent"], position: 98, endWeight: 97, phaseNumber: 7 },
  ];

  // ── Adaptive milestones: determine active milestones from instance ID ──
  // Instance ID format: P<milestone-digits>-<timestamp> (e.g. P12345-20260406-195906)
  // Legacy formats: ALLPHASES-*, PHASE2+-*, FABRIC-*, teardown*
  function getActiveMilestoneNumbers(): Set<number> {
    const deployConfig = (status?.customStatus as Record<string, unknown>)?.deployConfig as DeploymentConfig | undefined;
    const id = instanceId ?? "";
    const pMatch = id.match(/^P(\d+)-/i);
    const idMilestones = pMatch
      ? new Set(pMatch[1].split("").map(Number).filter((n) => n >= 1 && n <= 7))
      : null;

    // Overrule legacy IDs if we have the rich deployment config saved, but do
    // not delete milestone digits already encoded in a P123... instance id.
    // Resumed runs legitimately set skip_* flags for already-completed work;
    // hiding those milestones makes the frontend look like phases were missed.
    if (deployConfig && !isTeardown) {
      const set = new Set<number>(idMilestones ?? [1]);
      // Keep milestone 2 visible when Phase 2 telemetry work was part of the
      // original journey, even if this resumed run skipped already-completed
      // Fabric/RTI steps. Hiding it makes the journey line jump from 1 to 3.
      if (!deployConfig.skip_fabric || !deployConfig.skip_rti_phase2 || instanceId?.startsWith("P")) set.add(2);
      if (!deployConfig.skip_hds_pipelines) set.add(3);
      if (!(deployConfig.skip_imaging && deployConfig.skip_data_agents && deployConfig.skip_ontology)) set.add(4);
      if (!deployConfig.skip_activator) set.add(5);
      if (!deployConfig.skip_quality_measures) set.add(6);
      if (!deployConfig.skip_phase7) set.add(7);
      if (set.has(5) && !set.has(6)) set.add(6);
      return set;
    }

    // New format: P followed by milestone digits (P12345, P2345, P3, etc.)
    if (idMilestones) {
      if (idMilestones.has(5) && !idMilestones.has(6)) {
        // If it had the 5-digit full deploy, map to all 6 milestones under the new model
        idMilestones.add(6);
      }
      return idMilestones;
    }

    // Legacy formats
    if (id.startsWith("ALLPHASES")) return new Set([1, 2, 3, 4, 5, 6, 7]);
    if (id.startsWith("PHASE2+")) return new Set([1, 2, 3, 4, 5, 6, 7]);
    if (id.startsWith("FABRIC")) return new Set([1, 2, 3, 4, 5, 6, 7]);

    // Default: show all current deployment milestones.
    return new Set([1, 2, 3, 4, 5, 6, 7]);
  }


  const activeMilestoneNumbers = getActiveMilestoneNumbers();

  // The backend only emits phases that actually logged a Deploy-All step.
  // Keep Phase 7 visible when it was selected in the deployment config but did
  // not produce an output phase, so operators can see the gap instead of
  // mistaking the deployment screen for a six-phase plan.
  if (!isTeardown && activeMilestoneNumbers.has(7) && !phases.some((p) => /payer|claim-stream|healthcareopsagent|graph agent/i.test(p.phase))) {
    phases = [...phases, { phase: "7. Payer RTI & Ops: Claim stream, scoring, activator, and agents", status: "pending", milestone: 7 }];
  }

  const cleanPhaseName = (name: string) => name.toLowerCase()
    .replace(/^(phase\s*\d+:|\d+[a-z]?\.\s*[^:]+:)/i, "")
    .replace(/\s*\(auto\)\s*/i, "")
    .replace(/\s*\(manual\)\s*/i, "")
    .trim();

  const phaseMatchesTemplate = (phase: PhaseInfo, template: PhaseInfo): boolean => {
    if (phase.phase === template.phase) return true;
    if (template.id === "phase_5_alerts" && /phase\s*5|data activator|clinicalalertactivator|reflex/i.test(phase.phase)) return true;
    if (template.id === "phase_7_payer_ops" && /payer|claim-stream|healthcareopsagent|graph agent/i.test(phase.phase)) return true;

    const pClean = cleanPhaseName(phase.phase);
    const templateClean = cleanPhaseName(template.phase);

    if (pClean === templateClean) return true;
    if (pClean.includes("workspace") && templateClean.includes("workspace")) return true;
    if (((pClean.includes("base azure") || pClean.includes("shared hds infrastructure")) && templateClean.includes("base azure"))) return true;
    if (pClean.includes("fhir service") && templateClean.includes("fhir service")) return true;
    if ((pClean.includes("dicom loader") || pClean.includes("dicom service") || pClean.includes("imagingstudy")) && (templateClean.includes("dicom loader") || templateClean.includes("imagingstudy"))) return true;
    if ((pClean.includes("healthcare data solutions") || pClean.includes("hds guidance") || pClean.includes("hds deployment")) && (templateClean.includes("hds deployment") || templateClean.includes("hds detection"))) return true;
    if (pClean.includes("dicom shortcut") && templateClean.includes("dicom shortcut")) return true;
    if ((pClean.includes("swa viewer") || pClean.includes("imaging & reporting") || pClean.includes("imaging and reporting")) && (templateClean.includes("swa viewer") || templateClean.includes("direct lake"))) return true;
    if ((pClean.includes("conversational") || pClean.includes("data agents")) && templateClean.includes("conversational")) return true;
    if (pClean.includes("ontology") && templateClean.includes("ontology")) return true;
    if ((pClean.includes("reflex alerts") || pClean.includes("data activator")) && templateClean.includes("reflex alerts")) return true;
    if ((pClean.includes("analytics pipeline") || pClean.includes("cms quality") || pClean.includes("quality")) && templateClean.includes("analytics pipeline")) return true;
    if ((pClean.includes("payer") || pClean.includes("claim")) && templateClean.includes("payer rti")) return true;

    if (templateClean === "fabric rti ingest") {
      const lower = phase.phase.toLowerCase();
      const isEnrichment = lower.includes("enrichment") || lower.includes("phase 2: fabric rti (") || lower.includes("rti phase 2");
      return pClean.includes("fabric rti") && !isEnrichment;
    }

    if (templateClean === "fabric rti enrichment") {
      const lower = phase.phase.toLowerCase();
      const isEnrichment = lower.includes("enrichment") || lower.includes("phase 2: fabric rti (") || lower.includes("rti phase 2");
      return pClean.includes("fabric rti") && isEnrichment;
    }

    return pClean.includes(templateClean) || templateClean.includes(pClean);
  };

  const skippedByConfig = (template: PhaseInfo): boolean => {
    const deployConfig = (status?.customStatus as Record<string, unknown> | null)?.deployConfig as DeploymentConfig | undefined;
    if (!deployConfig) return false;
    switch (template.id) {
      case "phase_1_base_infra":
        return deployConfig.skip_base_infra;
      case "phase_1_fhir":
        return deployConfig.skip_fhir || deployConfig.skip_synthea;
      case "phase_2_rti_ingest":
        return deployConfig.skip_fabric;
      case "phase_2_rti_enrichment":
        return deployConfig.skip_rti_phase2;
      case "phase_1_dicom":
        return deployConfig.skip_dicom;
      case "phase_3_hds_detection":
      case "phase_3_hds_pipelines":
        return deployConfig.skip_hds_pipelines;
      case "phase_4_imaging":
        return deployConfig.skip_imaging;
      case "phase_4_agents":
        return deployConfig.skip_data_agents;
      case "phase_4_ontology":
        return deployConfig.skip_ontology;
      case "phase_5_alerts":
        return deployConfig.skip_activator;
      case "phase_6_quality":
        return deployConfig.skip_quality_measures;
      case "phase_7_payer_ops":
        return deployConfig.skip_phase7;
      default:
        return false;
    }
  };

  const displayPhaseTemplate = isTeardown
    ? phases
    : ALL_PHASES.filter((phase) => activeMilestoneNumbers.has(phase.milestone ?? 0));
  const displayPhases = isTeardown
    ? phases
    : displayPhaseTemplate.map((template) => {
        const matches = phases.filter((phase) => phaseMatchesTemplate(phase, template));
        const activeMatch = matches.find((phase) => phase.status === "running" || phase.status === "waiting_for_input");
        const resolved = activeMatch ?? matches[matches.length - 1];
        const currentMatchesTemplate = currentPhase
          ? phaseMatchesTemplate({ phase: currentPhase, status: "running" }, template)
          : false;
        if (!resolved && !currentMatchesTemplate) return { ...template, status: skippedByConfig(template) ? "skipped" : "pending" as const };
        const statusForCurrentPhase = isRunning && currentMatchesTemplate
          ? (isWaitingForHds ? "waiting_for_input" : "running")
          : resolved?.status ?? "pending";
        return { ...template, ...resolved, phase: template.phase, status: statusForCurrentPhase };
      });

  const isProblemSubStep = (subStep: PhaseSubStep) => subStep.status === "failed" || subStep.status === "warning";
  const phaseSubSteps = (phase: PhaseInfo) => phase.subSteps ?? [];
  const actionRequiredSubSteps = displayPhases.flatMap((phase) =>
    phaseSubSteps(phase)
      .filter(isProblemSubStep)
      .map((subStep) => ({ phase: phase.phase, subStep }))
  );
  const hasDegradedSubSteps = (phase: PhaseInfo) => phaseSubSteps(phase).some(isProblemSubStep);
  const hasFailedSubSteps = (phase: PhaseInfo) => phaseSubSteps(phase).some((subStep) => subStep.status === "failed");

  const allMilestonesTemplate = isTeardown ? TEARDOWN_MILESTONES : MILESTONES;
  // Filter milestones to those whose phaseNumber is in the active set.
  // For teardown: hide Fabric milestones when only an Azure RG is being torn down.
  const teardownHasFabric = !!(status?.customStatus as Record<string, unknown>)?.workspaceName;
  const teardownHasAzure = !!(status?.customStatus as Record<string, unknown>)?.resourceGroupName;
  const FABRIC_TEARDOWN_PATTERNS = new Set(["Fabric Workspace Items", "Workspace Identity", "Delete Workspace"]);
  const AZURE_TEARDOWN_PATTERNS = new Set(["Azure Resource Group"]);
  const baseFilteredMilestones = isTeardown
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

  // Keep canonical phase numbers in labels. The monitor may hide skipped
  // milestones (for example Phase 5 Data Activator), but renumbering Phase 7
  // to "6" makes the deployment screen look like Payer RTI & Ops is missing.
  // Only positions are redistributed for the active set.
  const numMilestones = baseFilteredMilestones.length;
  const filteredMilestones = baseFilteredMilestones.map((ms, idx) => {
    const position = numMilestones > 1 ? 8 + (idx * (84 / (numMilestones - 1))) : 50;
    return { ...ms, position };
  });

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

    if (isValidationReconciled && status?.runtimeStatus === "Completed") return "done";

    const milestoneIndex = activeMilestones.findIndex((m) => m.label === ms.label);
    const msPhaseNumber = ms.phaseNumber ?? 0;
    const relevantDisplayPhases = displayPhases.filter((p) =>
      msPhaseNumber > 0
        ? p.milestone === msPhaseNumber
        : ms.namePatterns.some((pat) => p.phase.toUpperCase().includes(pat.toUpperCase()))
    );
    const matchedPhases = relevantDisplayPhases.length > 0
      ? relevantDisplayPhases
      : phases.filter((p) => ms.namePatterns.some((pat) => p.phase.toUpperCase().includes(pat.toUpperCase())));
    const allDone = matchedPhases.length > 0 && matchedPhases.every((p) => p.status === "succeeded" || p.status === "skipped");
    const hasActive = matchedPhases.some((p) => p.status === "running");
    const hasWaiting = matchedPhases.some((p) => p.status === "waiting_for_input");
    const hasStarted = matchedPhases.some((p) => p.status !== "pending");


    if (currentPhaseNumber > 0 && msPhaseNumber > 0 && currentPhaseNumber > msPhaseNumber) return "done";
    if (hasWaiting) return "waiting";
    if (hasActive || (currentPhaseNumber > 0 && msPhaseNumber === currentPhaseNumber && isRunning)) return "active";
    if (allDone) return "done";

    // Backend phase numbers can move ahead of resumed/skipped phases, but a
    // milestone is only complete when every visible phase in that milestone is
    // complete. This keeps Intelligence pending until both Data Agents and
    // Ontology have deployed.
    if (currentPhaseNumber > 0 && msPhaseNumber > 0 && currentPhaseNumber > msPhaseNumber && allDone) return "done";

    // Weight-based fallback only promotes milestones whose visible phases are
    // already complete; otherwise partial milestones like Intelligence would
    // appear done after Data Agents alone.
    if (milestoneIndex >= 0 && milestoneIndex < dynamicSegments.length && allDone) {
      if (weightedCompleted >= dynamicSegments[milestoneIndex].wEnd) return "done";
    }

    // If cancelled/failed, check if this milestone had any activity
    if (isCancelled || isFailed) {
      return hasStarted ? "cancelled" : "pending";
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
            const milestoneTitle = ms.shortLabel ?? ms.label.replace(/^\d+\.\s*/, "");
            const milestoneNumber = ms.phaseNumber ? String(ms.phaseNumber) : "";
            const milestoneLabelStyle = isTeardown && msStatus === "done"
              ? { backgroundColor: tokens.colorPaletteYellowForeground1, color: "#000000", borderColor: tokens.colorPaletteYellowForeground1 }
              : isTeardown && msStatus === "active"
                ? { color: tokens.colorPaletteYellowForeground1, borderColor: tokens.colorPaletteYellowForeground1, backgroundColor: tokens.colorPaletteYellowBackground1 }
                : undefined;
            const milestoneNumberStyle = msStatus === "done"
              ? { backgroundColor: "rgba(255,255,255,0.22)", color: "inherit" }
              : msStatus === "waiting" || msStatus === "active"
                ? { backgroundColor: tokens.colorNeutralBackground1, color: "inherit" }
                : undefined;
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
                  title={ms.label}
                  className={`${styles.milestoneLabel} ${
                    msStatus === "done"
                      ? styles.milestoneLabelDone
                      : msStatus === "waiting"
                      ? styles.milestoneLabelActive
                      : ""
                  }`}
                  style={milestoneLabelStyle}
                >
                  {milestoneNumber && (
                    <span className={styles.milestoneLabelNumber} style={milestoneNumberStyle}>
                      {milestoneNumber}
                    </span>
                  )}
                  <span className={styles.milestoneLabelText}>{milestoneTitle}</span>
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

          // Full card/timeline order includes pending future stages for active deployments.
          const allPhasesNormalized = displayPhases;

          // Separate phases by status
          const completedPhases = allPhasesNormalized.filter(
            (p) => p.status === "succeeded" || p.status === "skipped" || (isComplete && p.status === "pending")
          );
          const activePhases = allPhasesNormalized.filter(
            (p) => !isComplete && (p.status === "running" || p.status === "waiting_for_input")
          );
          const futurePhases = allPhasesNormalized.filter(
            (p) => !isComplete && p.status === "pending"
          );
          const hasSucceededPhases = allPhasesNormalized.some((p) => p.status === "succeeded");
          const hasSkippedPhases = allPhasesNormalized.some((p) => p.status === "skipped");
          const hasDegradedPhases = allPhasesNormalized.some(hasDegradedSubSteps);
          const hasSlowPhases = allPhasesNormalized.some((p) => parseDurationMinutes(p.duration) > 6.0);

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
                    const degradedCompleted = completedPhases.filter(hasDegradedSubSteps);
                    const summaryHasFailed = degradedCompleted.some(hasFailedSubSteps);
                    const summaryDegradedColor = summaryHasFailed ? tokens.colorPaletteRedBorderActive : tokens.colorPaletteYellowBorderActive;
                    
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
                        {degradedCompleted.length > 0 && (
                          <div style={{ color: summaryDegradedColor, marginTop: "4px", fontSize: "11px", fontWeight: tokens.fontWeightSemibold }}>
                            {degradedCompleted.length} completed phase{degradedCompleted.length === 1 ? "" : "s"} need attention
                          </div>
                        )}
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
                            boxShadow: degradedCompleted.length > 0 ? `inset 0 0 0 2px ${summaryDegradedColor}, inset 0 0 6px rgba(0, 0, 0, 0.15)` : "inset 0 0 6px rgba(0, 0, 0, 0.15)",
                            height: "100%",
                            flexShrink: 0
                          }}
                        >
                          {degradedCompleted.length > 0 && (
                            <span
                              aria-hidden="true"
                              style={{ position: "absolute", top: 0, right: 0, bottom: 0, width: "4px", backgroundColor: summaryDegradedColor }}
                            />
                          )}
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
                    const isComp = p.status === "succeeded" || p.status === "skipped" || (isComplete && p.status === "pending");
                    const isActive = !isComplete && (p.status === "running" || p.status === "waiting_for_input");
                    const isFut = !isComplete && p.status === "pending";

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
                    const degradedSubSteps = phaseSubSteps(p).filter(isProblemSubStep);
                    const isDegraded = degradedSubSteps.length > 0;
                    const degradedColor = hasFailedSubSteps(p) ? tokens.colorPaletteRedBorderActive : tokens.colorPaletteYellowBorderActive;

                    // Styles and color coding
                    let bgColor = tokens.colorPaletteGreenBackground2;
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
                      bgColor = "#CC5500";
                      textColor = "#ffffff";
                    }

                    const baseTooltip = isFut
                      ? `${p.phase}: Pending / Not started yet`
                      : `${p.phase}: ${mins > 0.1 ? `${mins.toFixed(1)} min` : isActive ? "active / in progress" : p.status === "skipped" ? "skipped" : "completed"}`;
                    const tooltipContent = `${baseTooltip}${isDegraded ? ` · ${degradedSubSteps.length} sub-step${degradedSubSteps.length === 1 ? "" : "s"} need attention` : ""} (Click to scroll)`;
                    const ganttBoxShadow = [
                      mins > 6.0 && !isActive && !isFut ? "inset 0 0 8px rgba(204, 85, 0, 0.45)" : "",
                      isDegraded ? `inset 0 0 0 2px ${degradedColor}` : "",
                    ].filter(Boolean).join(", ");

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
                            ...(ganttBoxShadow ? { boxShadow: ganttBoxShadow } : {}),
                            ...(isDegraded ? { outline: `1px solid ${degradedColor}`, outlineOffset: "-1px" } : {}),
                            ...(isFut ? { border: `1px dashed ${tokens.colorNeutralStroke1}`, boxSizing: "border-box" } : {})
                          }}
                        >
                          {isDegraded && (
                            <span
                              aria-hidden="true"
                              style={{ position: "absolute", top: 0, right: 0, bottom: 0, width: "4px", backgroundColor: degradedColor }}
                            />
                          )}
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
                  {hasSucceededPhases && (
                    <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS }}>
                      <div style={{ width: "12px", height: "12px", borderRadius: "3px", backgroundColor: tokens.colorPaletteGreenBackground2 }} />
                      <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>Succeeded</Text>
                    </div>
                  )}
                  {activePhases.length > 0 && (
                    <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS }}>
                      <div className="gantt-running-striped" style={{
                        width: "12px",
                        height: "12px",
                        borderRadius: "3px",
                        backgroundColor: tokens.colorPaletteYellowBackground2
                      }} />
                      <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>In Progress (Live Growth)</Text>
                    </div>
                  )}
                  {hasSkippedPhases && (
                    <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS }}>
                      <div style={{ width: "12px", height: "12px", borderRadius: "3px", backgroundColor: tokens.colorNeutralBackground3, border: `1px solid ${tokens.colorNeutralStroke2}` }} />
                      <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>Skipped</Text>
                    </div>
                  )}
                  {compressCompleted && futurePhases.length > 0 && (
                    <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS }}>
                      <div style={{ width: "12px", height: "12px", borderRadius: "3px", backgroundColor: tokens.colorNeutralBackground2, border: `1px dashed ${tokens.colorNeutralStroke1}` }} />
                      <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>Pending</Text>
                    </div>
                  )}
                  {hasDegradedPhases && (
                    <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS }}>
                      <div style={{ width: "12px", height: "12px", borderRadius: "3px", backgroundColor: tokens.colorPaletteGreenBackground2, boxShadow: `inset 0 0 0 2px ${tokens.colorPaletteYellowBorderActive}` }} />
                      <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>Degraded Sub-step</Text>
                    </div>
                  )}
                  {hasSlowPhases && (
                    <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS }}>
                      <div style={{ width: "12px", height: "12px", borderRadius: "3px", backgroundColor: "#CC5500", boxShadow: "0 0 4px rgba(204, 85, 0, 0.45)" }} />
                      <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>Slow Phase ({">"}6m)</Text>
                    </div>
                  )}
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

      {actionRequiredSubSteps.length > 0 && (
        <div className={styles.actionRequired} role="alert" aria-live="polite">
          <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalS }}>
            <ErrorCircleRegular style={{ color: tokens.colorPaletteRedBorderActive, fontSize: "20px" }} />
            <Subtitle1>Action Required</Subtitle1>
            <Badge color="warning" size="small">{actionRequiredSubSteps.length}</Badge>
          </div>
          <div className={styles.actionRequiredList}>
            {actionRequiredSubSteps.map(({ phase, subStep }) => (
              <div key={`${phase}-${subStep.name}`} className={styles.actionRequiredItem}>
                <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS, flexWrap: "wrap" }}>
                  <Badge color={subStep.status === "failed" ? "danger" : "warning"} size="small">{subStep.status}</Badge>
                  <Text weight="semibold" size={200}>{phase}</Text>
                  <Text size={200}>· {subStep.name}</Text>
                  {subStep.runId && <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>run {subStep.runId}</Text>}
                  {subStep.url && (
                    <Button
                      size="small"
                      appearance="subtle"
                      icon={<OpenRegular />}
                      onClick={() => window.open(subStep.url, "_blank", "noreferrer")}
                    >
                      Open
                    </Button>
                  )}
                </div>
                {subStep.detail && <Text size={200} style={{ color: tokens.colorNeutralForeground2 }}>{subStep.detail}</Text>}
              </div>
            ))}
          </div>
        </div>
      )}

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
                {wsName && <span className={styles.configItem}><FabricBadge /> {wsName}</span>}
                {rgName && <span className={styles.configItem}><AzureBadge /> {rgName}</span>}
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
          { key: "skip_device_assoc", label: "Device and/or DICOM Association", phase: 1 },
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
          { key: "skip_phase7", label: "Payer RTI & Ops", phase: 7 },
          { key: "skip_payer_rti", label: "Payer RTI", phase: 7 },
          { key: "skip_payer_activator", label: "Payer Activator", phase: 7 },
          { key: "skip_ops_agent", label: "Ops Agents", phase: 7 },
          { key: "skip_graph_agent", label: "Healthcare Graph Agent", phase: 7 },
        ];
        const enabled = COMPONENTS.filter((c) => !cfg[c.key]);
        const skipped = COMPONENTS.filter((c) => cfg[c.key]);
        return (
          <Card className={styles.configCard} size="small">
            <CardHeader header={<Text weight="semibold" size={300}>Deployment Configuration</Text>} />
            <div className={styles.configGrid}>
              {(cfg.fabric_workspace_name as string) && (
                <span className={styles.configItem}><FabricBadge /> {cfg.fabric_workspace_name as string}</span>
              )}
              {(cfg.resource_group_name as string) && (
                <span className={styles.configItem}><AzureBadge /> {cfg.resource_group_name as string}</span>
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
        if (isTeardown) return null;
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

      {!isMock && status?.customStatus && (
        <Card style={{ marginBottom: tokens.spacingVerticalM }}>
          <CardHeader
            header={<Subtitle1>{isTeardown ? "Teardown Resource State" : "Cloud Resource State"}</Subtitle1>}
            description={status.customStatus.detail || status.runtimeStatus}
          />
          <div style={{ display: "grid", gap: tokens.spacingVerticalS, padding: `0 ${tokens.spacingHorizontalL} ${tokens.spacingVerticalM}` }}>
            <div style={{ display: "flex", flexWrap: "wrap", gap: tokens.spacingHorizontalS }}>
              {status.customStatus.workspaceName && (
                <Badge color={cloudState?.workspace.status === "deleted" ? "subtle" : "brand"}>Fabric: {status.customStatus.workspaceName} · {cloudState?.workspace.status ?? "checking"}</Badge>
              )}
              {status.customStatus.resourceGroupName && (
                <Badge color={cloudState?.resourceGroup.status === "deleted" || status.runtimeStatus === "Completed" ? "success" : cloudState?.resourceGroup.status === "deleting" ? "warning" : "informative"}>Azure RG: {status.customStatus.resourceGroupName} · {cloudState?.resourceGroup.provisioningState ?? "checking"}</Badge>
              )}
              <Badge color={status.runtimeStatus === "Completed" ? "success" : status.runtimeStatus === "Running" ? "informative" : "warning"}>Local: {status.runtimeStatus}</Badge>
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: tokens.spacingHorizontalS }}>
              <Button size="small" appearance="secondary" onClick={runValidation} disabled={validating}>{validating ? "Validating…" : isTeardown ? "Validate teardown" : "Validate deployment"}</Button>
              {!isTeardown && status.runtimeStatus === "Completed" && !(status.output?.phases ?? []).some((p) => /PHASE 7|PAYER RTI/i.test(p.phase)) && (
                <Button size="small" appearance="primary" onClick={runPhase7Continuation} disabled={continuingPhase7}>{continuingPhase7 ? "Starting Phase 7…" : "Run missing Phase 7"}</Button>
              )}
            </div>
            {validation && (
              <div style={{ display: "grid", gap: 4 }}>

                {validation.checks.map((check) => (
                  <Text key={check.name} size={200}><Badge size="small" color={check.status === "pass" ? "success" : check.status === "warning" ? "warning" : "danger"}>{check.status}</Badge> {check.name}: {check.detail}</Text>
                ))}
              </div>
            )}
          </div>
        </Card>
      )}

      {logExplainers.length > 0 && (
        <MessageBar intent="info" style={{ marginBottom: tokens.spacingVerticalM }}>
          <MessageBarBody>
            <Text weight="semibold">Log explainers:</Text>{" "}
            {logExplainers.map((item) => `${item.label} — ${item.detail}`).join(" · ")}
          </MessageBarBody>
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
            disabled={resumingHds}
            style={{ marginTop: tokens.spacingVerticalM }}
          >
            {resumingHds ? "Resuming…" : "Continue — HDS is deployed"}
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
          // Build log-to-phase mapping from the backend phase tag first.
          // Falling back to message-boundary scanning is only for older logs that
          // predate phase tags; never dump a rotated buffer into the last card.
          const logPhaseMap = new Map<number, number[]>(); // phaseIndex → log indices
          const pushLog = (phaseIdx: number, logIdx: number) => {
            if (!logPhaseMap.has(phaseIdx)) logPhaseMap.set(phaseIdx, []);
            logPhaseMap.get(phaseIdx)!.push(logIdx);
          };
          const findPhaseIndex = (rawPhase: string | number | undefined) => {
            if (rawPhase === undefined || rawPhase === null) return -1;
            const raw = String(rawPhase).trim();
            if (!raw) return -1;
            const numeric = Number(raw);
            if (Number.isInteger(numeric) && numeric >= 0 && numeric < displayPhases.length) {
              return numeric;
            }
            return displayPhases.findIndex((displayPhase) => {
              const candidates = [
                displayPhase.phase,
                ...phases
                  .filter((phase) => phaseMatchesTemplate(phase, displayPhase))
                  .map((phase) => phase.phase),
              ];
              return candidates.some((candidate) =>
                raw.toUpperCase() === candidate.toUpperCase() ||
                phaseMatchesTemplate({ phase: raw, status: "running" }, { ...displayPhase, phase: candidate })
              );
            });
          };
          if (!isMock && backendLogs && backendLogs.length > 0) {
            let currentPhaseIdx = -1;
            for (let logIdx = 0; logIdx < backendLogs.length; logIdx++) {
              const explicitPhaseIdx = findPhaseIndex(backendLogs[logIdx].phase);
              if (explicitPhaseIdx >= 0) {
                pushLog(explicitPhaseIdx, logIdx);
                currentPhaseIdx = explicitPhaseIdx;
                continue;
              }

              const msg = backendLogs[logIdx].message.toUpperCase();
              // Check if this log starts a new phase by matching canonical or backend phase names.
              for (let pIdx = 0; pIdx < displayPhases.length; pIdx++) {
                const phaseCandidates = [
                  displayPhases[pIdx].phase,
                  ...phases.filter((phase) => phaseMatchesTemplate(phase, displayPhases[pIdx])).map((phase) => phase.phase),
                ];
                if (phaseCandidates.some((name) => msg.includes(name.toUpperCase())) && (msg.includes("STEP") || msg.includes("╔") || msg.includes("───"))) {
                  currentPhaseIdx = pIdx;
                  break;
                }
              }
              if (currentPhaseIdx >= 0) {
                pushLog(currentPhaseIdx, logIdx);
              }
            }
          }

          return displayPhases.map((phase, phaseIdx) => {
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
              <div style={{ display: "flex", gap: "10px", justifyContent: "flex-end" }}>
                <Button
                  appearance="outline"
                  icon={<DocumentTableRegular style={{ color: "#107c41" }} />}
                  onClick={exportToXLSX}
                  size="small"
                >
                  Export to .XLSX
                </Button>
                <Button
                  appearance="outline"
                  icon={<DocumentTextRegular />}
                  onClick={exportToCSV}
                  size="small"
                >
                  Export to .CSV
                </Button>
              </div>

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

      {/* Floating continue button - above redeploy (shown on failure only) */}
      {isFailed && !isTeardown && !isRunning && (
        <Button
          className={styles.floatingContinueFailedBtn}
          appearance="primary"
          icon={<PlayRegular />}
          onClick={handleContinueFailed}
          disabled={continuingFailed || redeploying}
          size="medium"
        >
          {continuingFailed ? "Continuing…" : "Continue deployment from last failed steps"}
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

      {/* Floating HDS resume button - visible while the manual gate is pending */}
      {isWaitingForHds && isRunning && (
        <Tooltip content="Click after the manual Fabric HDS deployment is complete." relationship="description">
          <Button
            className={styles.floatingResumeBtn}
            appearance="primary"
            icon={<PlayRegular />}
            onClick={handleResume}
            disabled={resumingHds}
            size="medium"
          >
            {resumingHds ? "Resuming…" : "Resume after HDS"}
          </Button>
        </Tooltip>
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
