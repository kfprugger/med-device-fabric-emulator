import { useState, useEffect, useRef, useCallback } from "react";
import {
  Badge,
  Button,
  Card,
  CardHeader,
  makeStyles,
  Popover,
  PopoverSurface,
  PopoverTrigger,
  ProgressBar,
  Text,
  tokens,
} from "@fluentui/react-components";
import {
  CheckmarkCircleFilled,
  DismissCircleFilled,
  ArrowSyncCircleRegular,
  ClockRegular,
  PauseCircleRegular,
  InfoRegular,
  ChevronDownRegular,
  ChevronUpRegular,
  WarningFilled,
} from "@fluentui/react-icons";
import type { PhaseInfo, PhaseSubStep, PhaseSubStepStatus } from "../api";
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
  subStepPills: {
    display: "flex",
    flexWrap: "wrap" as const,
    gap: tokens.spacingHorizontalXS,
    padding: `0 ${tokens.spacingHorizontalL} ${tokens.spacingVerticalXS}`,
  },
  subStepPill: {
    display: "inline-flex",
    alignItems: "center",
    gap: "4px",
    maxWidth: "220px",
    padding: `2px ${tokens.spacingHorizontalXS}`,
    borderRadius: tokens.borderRadiusCircular,
    border: `1px solid ${tokens.colorNeutralStroke2}`,
    backgroundColor: tokens.colorNeutralBackground2,
    fontSize: tokens.fontSizeBase100,
    color: tokens.colorNeutralForeground2,
  },
  subStepPillWarning: {
    border: `1px solid ${tokens.colorPaletteYellowBorderActive}`,
    backgroundColor: tokens.colorStatusWarningBackground1,
    color: tokens.colorNeutralForeground1,
  },
  subStepPillFailed: {
    border: `1px solid ${tokens.colorPaletteRedBorderActive}`,
    backgroundColor: tokens.colorPaletteRedBackground1,
    color: tokens.colorPaletteRedForeground1,
  },
  subStepDetailPanel: {
    display: "flex",
    flexDirection: "column" as const,
    gap: tokens.spacingVerticalXS,
    padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalL}`,
    backgroundColor: tokens.colorNeutralBackground2,
    borderTop: `1px solid ${tokens.colorNeutralStroke2}`,
  },
  subStepDetail: {
    padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalM}`,
    borderRadius: tokens.borderRadiusMedium,
    borderLeft: `3px solid ${tokens.colorPaletteYellowBorderActive}`,
    backgroundColor: tokens.colorStatusWarningBackground1,
  },
  subStepDetailFailed: {
    borderLeftColor: tokens.colorPaletteRedBorderActive,
    backgroundColor: tokens.colorPaletteRedBackground1,
  },
  infoButton: {
    minWidth: "24px",
    width: "24px",
    height: "24px",
    color: tokens.colorNeutralForeground3,
  },
  infoPopoverContent: {
    maxWidth: "320px",
    whiteSpace: "normal" as const,
    lineHeight: "1.4",
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

function subStepBadgeColor(status: PhaseSubStepStatus): "success" | "danger" | "informative" | "warning" | "subtle" {
  if (status === "succeeded") return "success";
  if (status === "failed") return "danger";
  if (status === "warning") return "warning";
  if (status === "running") return "informative";
  return "subtle";
}

function subStepLabel(subStep: PhaseSubStep): string {
  const duration = formatDuration(subStep.duration);
  return duration ? `${subStep.name} · ${duration}` : subStep.name;
}

function isActionSubStep(subStep: PhaseSubStep): boolean {
  return subStep.status === "failed" || subStep.status === "warning";
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
  "Phase 1: Fabric Workspace": "Workspace validation, capacity assignment, and managed identity provisioning",
  "Phase 1: Base Azure Infrastructure": "Event Hub, ACR, Storage, Key Vault, and Masimo emulator ACI",
  "Phase 1: FHIR Service + Synthea + Loader": "FHIR infrastructure, Synthea patients, FHIR Loader upload, and device associations",
  "Phase 1: Shared HDS Infrastructure": "Shared HDS workspace and storage prerequisites when FHIR is bypassed",
  "Phase 1: DICOM Loader": "TCIA download, patient-preserving re-tagging, ADLS upload, and FHIR ImagingStudy creation",
  "Phase 2: Fabric RTI": "Masimo Eventhouse, KQL database/functions, Eventstream topology, dashboard, and FHIR $export",
  "Phase 2: Fabric RTI (auto)": "Post-HDS bronze shortcuts, KQL shortcuts, enriched alerts, and Clinical Alerts Map",
  "Phase 3: HDS Deployment Detection": "Manual HDS deployment detection, notebook cleanup, and resume gate",
  "Phase 3: DICOM Shortcut + HDS Pipelines": "DICOM shortcut, clinical/imaging/OMOP pipeline triggers, and row-count gates",
  "Phase 4: Imaging & Reporting": "Cohorting Agent, OHIF DICOM Viewer, Direct Lake imaging report, and proxy/index validation",
  "Phase 4: Ontology": "DeviceAssociation table, ClinicalDeviceOntology, DevicePayerOntology, and agent binding",
  "Phase 4: Ontology-Aware Data Agents": "Patient 360 + Clinical Triage agents bound to ClinicalDeviceOntology",
  "Phase 5: Data Activator": "ClinicalAlertActivator Reflex + email notification rules",
  "Phase 6: CMS Quality & Claims": "Claims star schema, quality measures, Star Ratings, HCC risk, and Power BI report",
  "Phase 7: Payer RTI & Ops": "Claim stream, payer scoring, activator, HealthcareOpsAgent, and graph agent",
};

function formatLogTime(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString("en-US", { hour12: false });
  } catch {
    return "";
  }
}
function canonicalPhaseName(name: string): string {
  return name
    .toLowerCase()
    .replace(/^(phase\s*\d+:|\d+[a-z]?\.\s*[^:]+:)/i, "")
    .replace(/\s*\(auto\)\s*/i, "")
    .trim();
}

function getPhaseTooltip(phaseName: string): string {
  const exact = PHASE_TOOLTIPS[phaseName];
  if (exact) return exact;

  const canonical = canonicalPhaseName(phaseName);
  const match = Object.entries(PHASE_TOOLTIPS)
    .map(([key, value]) => ({ keyCanonical: canonicalPhaseName(key), value }))
    .sort((a, b) => b.keyCanonical.length - a.keyCanonical.length)
    .find(({ keyCanonical }) => canonical.includes(keyCanonical) || keyCanonical.includes(canonical));
  return match?.value ?? phaseName;
}


export function PhaseCard({ phase, logs = [], defaultExpanded, autoScroll = true, instanceId }: PhaseCardProps) {
  const styles = useStyles();
  const reducedMotion = useReducedMotion();
  const tooltip = getPhaseTooltip(phase.phase);
  const isActive = phase.status === "running" || phase.status === "waiting_for_input";
  const subSteps = phase.subSteps ?? [];
  const actionSubSteps = subSteps.filter(isActionSubStep);
  const hasWarnings = (phase.warnings?.length ?? 0) > 0 || actionSubSteps.length > 0;
  const [expanded, setExpanded] = useState(defaultExpanded ?? isActive);
  const previousStatusRef = useRef(phase.status);
  const logPanelRef = useRef<HTMLDivElement>(null);
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

  // Auto-expand when phase becomes active, then auto-collapse once that active work succeeds.
  useEffect(() => {
    if (isActive) {
      setExpanded(true);
    }
  }, [isActive]);

  useEffect(() => {
    const previousStatus = previousStatusRef.current;
    if (previousStatus !== phase.status) {
      if (phase.status === "succeeded") {
        setExpanded(false);
      }
      previousStatusRef.current = phase.status;
    }
  }, [phase.status]);

  // Auto-scroll only the log panel, never the whole page.
  useEffect(() => {
    if (!autoScroll || !expanded || !logPanelRef.current) return;

    const panel = logPanelRef.current;
    panel.scrollTo({
      top: panel.scrollHeight,
      behavior: reducedMotion ? "auto" : "smooth",
    });
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
    <Card
        id={`phase-card-${phase.phase.replace(/\s+/g, "-")}`}
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
              <Popover positioning={{ position: "below", align: "end" }} withArrow>
                <PopoverTrigger disableButtonEnhancement>
                  <Button
                    appearance="transparent"
                    size="small"
                    className={styles.infoButton}
                    icon={<InfoRegular />}
                    aria-label={`About ${phase.phase}`}
                    onClick={(event) => event.stopPropagation()}
                    onKeyDown={(event) => event.stopPropagation()}
                  />
                </PopoverTrigger>
                <PopoverSurface
                  className={styles.infoPopoverContent}
                  onClick={(event) => event.stopPropagation()}
                  onKeyDown={(event) => event.stopPropagation()}
                >
                  <Text weight="semibold" block>{phase.phase}</Text>
                  <Text size={200}>{tooltip}</Text>
                </PopoverSurface>
              </Popover>
              <span className={styles.chevron}>
                {expanded ? <ChevronUpRegular /> : <ChevronDownRegular />}
              </span>
            </div>
          }
        />
        {phase.status === "running" && <ProgressBar />}
        {subSteps.length > 0 && (
          <div className={styles.subStepPills} aria-label="Pipeline sub-steps">
            {subSteps.map((subStep) => (
              <span
                key={subStep.name}
                className={`${styles.subStepPill} ${subStep.status === "failed" ? styles.subStepPillFailed : subStep.status === "warning" ? styles.subStepPillWarning : ""}`}
                title={subStep.detail || subStep.name}
              >
                <Badge color={subStepBadgeColor(subStep.status)} size="small">{subStep.status}</Badge>
                <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{subStepLabel(subStep)}</span>
              </span>
            ))}
          </div>
        )}
        {expanded && (phase.warnings?.length ?? 0) > 0 && (
          <div className={styles.warningBanner}>
            {(phase.warnings ?? []).map((w, i) => (
              <div key={i}>⚠ {w}</div>
            ))}
          </div>
        )}
        {expanded && actionSubSteps.length > 0 && (
          <div className={styles.subStepDetailPanel}>
            {actionSubSteps.map((subStep) => (
              <div
                key={subStep.name}
                className={`${styles.subStepDetail} ${subStep.status === "failed" ? styles.subStepDetailFailed : ""}`}
              >
                <div style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalXS, flexWrap: "wrap" }}>
                  <Badge color={subStepBadgeColor(subStep.status)} size="small">{subStep.status}</Badge>
                  <Text weight="semibold" size={200}>{subStep.name}</Text>
                  {subStep.duration && <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>{formatDuration(subStep.duration)}</Text>}
                  {subStep.runId && <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>run {subStep.runId}</Text>}
                  {subStep.url && (
                    <a
                      href={subStep.url}
                      target="_blank"
                      rel="noreferrer"
                      onClick={(event) => event.stopPropagation()}
                      style={{ color: tokens.colorBrandForeground1, fontSize: tokens.fontSizeBase100 }}
                    >
                      Open
                    </a>
                  )}
                </div>
                {subStep.detail && (
                  <Text size={100} block style={{ marginTop: tokens.spacingVerticalXXS, color: tokens.colorNeutralForeground2 }}>
                    {subStep.detail}
                  </Text>
                )}
              </div>
            ))}
          </div>
        )}
        {expanded && (
          <div className={styles.logPanel} ref={logPanelRef}>
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
          </div>
        )}
    </Card>
  );
}
