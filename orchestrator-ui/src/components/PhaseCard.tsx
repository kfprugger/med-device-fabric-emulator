import { useState, useEffect, useRef, useCallback } from "react";
import {
  Badge,
  Card,
  CardHeader,
  makeStyles,
  ProgressBar,
  Text,
  Tooltip,
  tokens,
} from "@fluentui/react-components";
import {
  CheckmarkCircleFilled,
  DismissCircleFilled,
  ArrowSyncCircleRegular,
  ClockRegular,
  PauseCircleRegular,
  ChevronDownRegular,
  ChevronUpRegular,
  WarningFilled,
} from "@fluentui/react-icons";
import type { PhaseInfo } from "../api";
import { getPhaseLogs } from "../api";
import type { PhaseLog } from "../mockDeployment";
import { useReducedMotion } from "../hooks/useReducedMotion";

const useStyles = makeStyles({
  card: {
    marginBottom: tokens.spacingVerticalS,
    transition: "box-shadow 0.2s ease, transform 0.15s ease",
    cursor: "pointer",
    ":hover": {
      boxShadow: tokens.shadow4,
      transform: "translateY(-1px)",
    },
    ":focus-visible": {
      outline: `2px solid ${tokens.colorBrandStroke1}`,
      outlineOffset: "2px",
    },
  },
  cardActive: {
    border: `1px solid ${tokens.colorBrandForeground1}`,
    boxShadow: tokens.shadow8,
  },
  row: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalM,
  },
  duration: {
    marginLeft: "auto",
    color: tokens.colorNeutralForeground3,
    fontSize: tokens.fontSizeBase200,
  },
  chevron: {
    marginLeft: tokens.spacingHorizontalS,
    color: tokens.colorNeutralForeground3,
    fontSize: tokens.fontSizeBase300,
  },
  logPanel: {
    maxHeight: "240px",
    overflowY: "auto",
    padding: `${tokens.spacingVerticalS} ${tokens.spacingHorizontalL}`,
    backgroundColor: tokens.colorNeutralBackground3,
    borderTop: `1px solid ${tokens.colorNeutralStroke2}`,
    fontFamily: "'Cascadia Code', 'Consolas', 'Courier New', monospace",
    fontSize: tokens.fontSizeBase200,
    lineHeight: "1.6",
  },
  logLine: {
    display: "flex",
    gap: tokens.spacingHorizontalS,
    alignItems: "baseline",
  },
  logTime: {
    color: tokens.colorNeutralForeground4,
    flexShrink: 0,
    minWidth: "85px",
  },
  logInfo: { color: tokens.colorNeutralForeground2 },
  logSuccess: { color: tokens.colorPaletteGreenForeground1 },
  logWarn: { color: tokens.colorPaletteYellowForeground1 },
  logError: { color: tokens.colorPaletteRedForeground1 },
  emptyLog: {
    color: tokens.colorNeutralForeground4,
    fontStyle: "italic",
  },
  warningBanner: {
    backgroundColor: tokens.colorStatusWarningBackground1,
    borderLeft: `3px solid ${tokens.colorPaletteYellowForeground1}`,
    padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalM}`,
    fontSize: tokens.fontSizeBase200,
    color: tokens.colorNeutralForeground1,
  },
});

function statusIcon(status: string, hasWarnings?: boolean) {
  if (status === "succeeded" && hasWarnings) {
    return <WarningFilled color={tokens.colorPaletteYellowForeground1} />;
  }
  switch (status) {
    case "succeeded":
      return <CheckmarkCircleFilled color={tokens.colorPaletteGreenForeground1} />;
    case "failed":
      return <DismissCircleFilled color={tokens.colorPaletteRedForeground1} />;
    case "running":
      return <ArrowSyncCircleRegular color={tokens.colorPaletteBlueForeground2} />;
    case "waiting_for_input":
      return <PauseCircleRegular color={tokens.colorPaletteYellowForeground1} />;
    case "skipped":
      return <ClockRegular color={tokens.colorNeutralForeground3} />;
    default:
      return <ClockRegular />;
  }
}

function statusBadge(status: string, hasWarnings?: boolean) {
  if (status === "succeeded" && hasWarnings) {
    return <Badge color="warning">warnings</Badge>;
  }
  const colorMap: Record<string, "success" | "danger" | "informative" | "warning" | "subtle"> = {
    succeeded: "success",
    failed: "danger",
    running: "informative",
    waiting_for_input: "warning",
    skipped: "subtle",
    pending: "subtle",
  };
  return <Badge color={colorMap[status] || "subtle"}>{status}</Badge>;
}

function formatDuration(duration?: number | string): string {
  if (duration === undefined || duration === null || duration === "") return "";
  // If it's already a formatted string from the backend (e.g. "10.2 min", "0 min")
  if (typeof duration === "string") {
    // Try to parse "X.X min" format → convert to seconds
    const minMatch = duration.match(/([\d.]+)\s*min/i);
    if (minMatch) {
      const mins = parseFloat(minMatch[1]);
      if (!isNaN(mins)) {
        const totalSec = mins * 60;
        if (totalSec < 60) return `${Math.round(totalSec)}s`;
        return `${Math.floor(mins)}m ${Math.round((mins % 1) * 60)}s`;
      }
    }
    return duration; // Return as-is if we can't parse
  }
  // Numeric seconds
  if (isNaN(duration)) return "";
  if (duration < 60) return `${duration.toFixed(0)}s`;
  const mins = Math.floor(duration / 60);
  const secs = duration % 60;
  return `${mins}m ${secs.toFixed(0)}s`;
}

interface PhaseCardProps {
  phase: PhaseInfo;
  logs?: PhaseLog[];
  defaultExpanded?: boolean;
  autoScroll?: boolean;
  instanceId?: string;
}

const PHASE_TOOLTIPS: Record<string, string> = {
  "Step 1: Fabric Workspace": "Workspace validation and managed identity provisioning",
  "Step 1b: Base Azure Infrastructure": "Event Hub, ACR, Storage, Key Vault, Masimo emulator ACI",
  "Step 2: FHIR Service & Data Loading": "FHIR Service, Synthea patients, FHIR Loader upload",
  "Step 2b: DICOM Infrastructure & Loading": "DICOM Service, TCIA download, re-tag, ADLS upload",
  "Step 3: Fabric RTI Phase 1": "Eventhouse, KQL DB, Eventstream, dashboard, FHIR $export",
  "Step 4: HDS Detection": "Auto-detect Healthcare Data Solutions + scipy environment",
  "Step 5: Fabric RTI Phase 2": "Bronze shortcut, KQL shortcuts, enriched alerts, Clinical Alerts Map",
  "Step 5b: HDS Pipelines": "DICOM shortcut, clinical/imaging/OMOP pipeline triggers",
  "Step 6: Data Agents": "Patient 360 + Clinical Triage Data Agents",
  "Step 7: Imaging Toolkit": "Cohorting Agent, OHIF DICOM Viewer, Power BI Imaging Report",
  "Step 8: Ontology": "DeviceAssociation table, ClinicalDeviceOntology, agent binding",
  "Step 9: Data Activator": "ClinicalAlertActivator Reflex + email notification rules",
};

function formatLogTime(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString("en-US", { hour12: false });
  } catch {
    return "";
  }
}

export function PhaseCard({ phase, logs = [], defaultExpanded, autoScroll = true, instanceId }: PhaseCardProps) {
  const styles = useStyles();
  const reducedMotion = useReducedMotion();
  const tooltip = PHASE_TOOLTIPS[phase.phase] || phase.phase;
  const isActive = phase.status === "running" || phase.status === "waiting_for_input";
  const hasWarnings = (phase.warnings?.length ?? 0) > 0;
  const [expanded, setExpanded] = useState(defaultExpanded ?? isActive);
  const logEndRef = useRef<HTMLDivElement>(null);
  const [fetchedLogs, setFetchedLogs] = useState<PhaseLog[] | null>(null);
  const [fetchingLogs, setFetchingLogs] = useState(false);
  const hasFetched = useRef(false);

  // Fetch per-phase logs from backend when card is expanded and we have no logs
  const fetchPhaseLogs = useCallback(async () => {
    if (!instanceId || hasFetched.current || fetchingLogs) return;
    if (phase.status === "pending") return;
    hasFetched.current = true;
    setFetchingLogs(true);
    try {
      const result = await getPhaseLogs(instanceId, phase.phase);
      if (result.length > 0) {
        setFetchedLogs(result as PhaseLog[]);
      }
    } catch {
      // non-fatal
    } finally {
      setFetchingLogs(false);
    }
  }, [instanceId, phase.phase, phase.status, fetchingLogs]);

  useEffect(() => {
    if (expanded && logs.length === 0 && !fetchedLogs && instanceId) {
      fetchPhaseLogs();
    }
  }, [expanded, logs.length, fetchedLogs, instanceId, fetchPhaseLogs]);

  const displayLogs = logs.length > 0 ? logs : (fetchedLogs ?? []);

  // Auto-expand when phase becomes active
  useEffect(() => {
    if (isActive) setExpanded(true);
  }, [isActive]);

  // Auto-scroll logs to bottom (respects autoScroll prop)
  useEffect(() => {
    if (autoScroll && expanded && logEndRef.current) {
      logEndRef.current.scrollIntoView({ behavior: reducedMotion ? "auto" : "smooth" });
    }
  }, [displayLogs.length, expanded, autoScroll, reducedMotion]);

  const logLevelStyle = (level: PhaseLog["level"]) => {
    switch (level) {
      case "success": return styles.logSuccess;
      case "warn": return styles.logWarn;
      case "error": return styles.logError;
      default: return styles.logInfo;
    }
  };

  const logPrefix = (level: PhaseLog["level"]) => {
    switch (level) {
      case "success": return "✓";
      case "warn": return "⚠";
      case "error": return "✗";
      default: return "›";
    }
  };

  return (
    <Tooltip content={tooltip} relationship="description" positioning="after">
      <Card
        className={`${styles.card} ${isActive ? styles.cardActive : ""}`}
        size="small"
        onClick={() => setExpanded((v) => !v)}
        role="button"
        tabIndex={0}
        aria-expanded={expanded}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            setExpanded((v) => !v);
          }
        }}
      >
        <CardHeader
          image={statusIcon(phase.status, hasWarnings)}
          header={
            <div className={styles.row}>
              <Text weight="semibold">{phase.phase}</Text>
              {statusBadge(phase.status, hasWarnings)}
              <Text className={styles.duration} size={200}>
                {formatDuration(phase.duration)}
              </Text>
              <span className={styles.chevron}>
                {expanded ? <ChevronUpRegular /> : <ChevronDownRegular />}
              </span>
            </div>
          }
        />
        {phase.status === "running" && <ProgressBar />}
        {expanded && hasWarnings && (
          <div className={styles.warningBanner}>
            {phase.warnings!.map((w, i) => (
              <div key={i}>⚠ {w}</div>
            ))}
          </div>
        )}
        {expanded && (
          <div className={styles.logPanel}>
            {displayLogs.length === 0 && (
              <div className={styles.emptyLog}>
                {fetchingLogs
                  ? "Loading logs…"
                  : phase.status === "pending"
                  ? "Waiting to start…"
                  : phase.status === "succeeded" || phase.status === "skipped"
                  ? "Completed — no logs available for this phase."
                  : phase.status === "running"
                  ? "Waiting for output…"
                  : "No logs available"}
              </div>
            )}
            {displayLogs.map((log, i) => (
              <div key={i} className={styles.logLine}>
                <span className={styles.logTime}>{formatLogTime(log.timestamp)}</span>
                <span className={logLevelStyle(log.level)}>
                  {logPrefix(log.level)} {log.message}
                </span>
              </div>
            ))}
            <div ref={logEndRef} />
          </div>
        )}
      </Card>
    </Tooltip>
  );
}
