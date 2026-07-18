import { useState, useEffect, useMemo, useRef, type PointerEvent as ReactPointerEvent } from "react";
import { useNavigate } from "react-router-dom";
import {
  Badge,
  Button,
  Card,
  CardHeader,
  Checkbox,
  Dialog,
  DialogActions,
  DialogBody,
  DialogContent,
  DialogSurface,
  DialogTitle,
  Dropdown,
  Field,
  InfoLabel,
  Input,
  Option,
  SpinButton,
  Spinner,
  Subtitle1,
  Text,
  Title2,
  Tooltip,
  makeStyles,
  tokens,
} from "@fluentui/react-components";
import { RocketRegular, BeakerRegular, AddRegular, DismissRegular, ArrowSyncRegular, PlayRegular, ChevronDownRegular, ChevronUpRegular, CheckmarkCircleRegular, CircleRegular, SettingsRegular, ClipboardRegular, FlashRegular } from "@fluentui/react-icons";
import { startDeployment, listCapacities, checkExistingDeployment, resumeCapacity, pauseCapacity, listFhirRegions, listSubscriptions, type DeploymentConfig, type FabricCapacity, type ExistingDeploymentInfo } from "../api";
import { startMockDeployment, getMockSubscriptions, getMockCapacities } from "../mockDeployment";
import { useAppState } from "../AppState";
import { MockDataBanner } from "../components/MockDataBanner";
import { HistoryInput } from "../components/HistoryInput";
import { getTagHistory, addTagToHistory } from "../formHistory";
import { useReducedMotion } from "../hooks/useReducedMotion";
import { AzureIcon, FabricIcon } from "../components/BrandIcons";

const useStyles = makeStyles({
  form: {
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalL,
    marginTop: tokens.spacingVerticalL,
  },
  section: {
    marginBottom: "0",
    transition: "box-shadow 0.2s ease",
    overflow: "visible",
    ":hover": {
      boxShadow: tokens.shadow8,
    },
  },
  sectionFullWidth: {
    gridColumn: "1 / -1",
  },
  sectionHeader: {
    cursor: "default",
  },
  fieldGroup: {
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalM,
    padding: `0 ${tokens.spacingHorizontalL} ${tokens.spacingVerticalM}`,
    overflow: "visible",
  },
  subscriptionRow: {
    display: "grid",
    gridTemplateColumns: "1fr 1fr",
    gap: tokens.spacingHorizontalM,
    overflow: "visible",
  },
  capacityFieldRow: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalXS,
  },
  fieldLabelWithIcon: {
    display: "inline-flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalS,
  },
  labelSeparator: {
    width: "1px",
    height: "14px",
    backgroundColor: tokens.colorNeutralStroke2,
    flexShrink: 0,
  },
  actions: {
    display: "flex",
    gap: tokens.spacingHorizontalM,
    marginTop: tokens.spacingVerticalXXL,
  },
  error: {
    color: tokens.colorStatusDangerForeground1,
    fontSize: tokens.fontSizeBase200,
    marginTop: tokens.spacingVerticalS,
  },
  checkboxGroup: {
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalXS,
    padding: `0 ${tokens.spacingHorizontalL} ${tokens.spacingVerticalM}`,
  },
  stickyHeader: {
    position: "sticky",
    top: 0,
    zIndex: 10,
    backgroundColor: tokens.colorNeutralBackground1,
    paddingBottom: tokens.spacingVerticalXS,
  },
  summarySidebar: {
    position: "sticky",
    top: tokens.spacingVerticalXXL,
    height: "fit-content",
    maxHeight: "calc(100vh - 100px)",
    overflowY: "auto",
  },
  compactField: {
    "@media (min-width: 1200px)": {
      padding: `0 ${tokens.spacingHorizontalM} ${tokens.spacingVerticalS}`,
    },
  },
  cardRequired: {
    borderLeft: `3px solid ${tokens.colorBrandStroke1}`,
  },
  cardOptional: {
    borderLeft: `3px solid ${tokens.colorNeutralStroke2}`,
  },
});

function TagHistoryPanel({ onSelect }: { onSelect: (tags: Record<string, string>) => void }) {
  const [tagHistory, setTagHistory] = useState<Array<Record<string, string>>>([]);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    getTagHistory()
      .then((h) => {
        setTagHistory(h.filter((t) => Object.keys(t).length > 0));
        setLoaded(true);
      })
      .catch(() => setLoaded(true));
  }, []);

  if (!loaded || tagHistory.length === 0) return null;

  return (
    <div style={{
      marginBottom: tokens.spacingVerticalS,
      padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalM}`,
      backgroundColor: tokens.colorNeutralBackground3,
      borderRadius: tokens.borderRadiusMedium,
      fontSize: tokens.fontSizeBase200,
    }}>
      <Text size={200} weight="semibold" style={{ marginBottom: tokens.spacingVerticalXXS, display: "block" }}>
        Previously used tags:
      </Text>
      <div style={{ display: "flex", flexWrap: "wrap", gap: tokens.spacingHorizontalXS }}>
        {tagHistory.map((tags, i) => {
          const label = Object.entries(tags).map(([k, v]) => `${k}:${v}`).join(", ");
          return (
            <Button
              key={i}
              appearance="subtle"
              size="small"
              onClick={() => onSelect(tags)}
              style={{
                fontSize: tokens.fontSizeBase200,
                padding: `2px ${tokens.spacingHorizontalS}`,
                border: `1px solid ${tokens.colorNeutralStroke2}`,
                borderRadius: tokens.borderRadiusMedium,
              }}
            >
              {label}
            </Button>
          );
        })}
      </div>
    </div>
  );
}

function getRectIntersection(rx: number, ry: number, rw: number, rh: number, tx: number, ty: number) {
  const cx = rx + rw / 2;
  const cy = ry + rh / 2;
  const dx = tx - cx;
  const dy = ty - cy;
  if (dx === 0 && dy === 0) return { x: cx, y: cy };

  const absDx = Math.abs(dx);
  const absDy = Math.abs(dy);
  const txLimit = rw / (2 * absDx);
  const tyLimit = rh / (2 * absDy);
  const t = Math.min(txLimit, tyLimit);

  return {
    x: cx + t * dx,
    y: cy + t * dy,
  };
}

const AZURE_REGION_LABELS: Record<string, string> = {
  australiaeast: "Australia East",
  canadacentral: "Canada Central",
  centralindia: "Central India",
  eastus: "East US",
  eastus2: "East US 2",
  francecentral: "France Central",
  germanywestcentral: "Germany West Central",
  japaneast: "Japan East",
  koreacentral: "Korea Central",
  northcentralus: "North Central US",
  northeurope: "North Europe",
  qatarcentral: "Qatar Central",
  southcentralus: "South Central US",
  southeastasia: "Southeast Asia",
  swedencentral: "Sweden Central",
  switzerlandnorth: "Switzerland North",
  uksouth: "UK South",
  westcentralus: "West Central US",
  westeurope: "West Europe",
  westus2: "West US 2",
  westus3: "West US 3",
};

function normalizeAzureLocation(location: string): string {
  return location.replace(/\s/g, "").toLowerCase();
}

function formatAzureLocationLabel(location: string): string {
  const normalized = normalizeAzureLocation(location);
  return AZURE_REGION_LABELS[normalized] ?? location;
}

function normalizeFhirRegionList(regions: string[]): string[] {
  return Array.from(new Set(regions.map(normalizeAzureLocation).filter(Boolean))).sort();
}

export function DeployWizard() {
  const styles = useStyles();
  const reducedMotion = useReducedMotion();
  const navigate = useNavigate();
  const { selectedSubscription, setSelectedSubscription, subscriptions: ctxSubscriptions, capacities: ctxCapacities } = useAppState();
  const [subscriptions, setSubscriptions] = useState(getMockSubscriptions());
  const [loading, setLoading] = useState(false);
  const [deploymentStartMessage, setDeploymentStartMessage] = useState("");
  const [error, setError] = useState("");
  const [usingMock, setUsingMock] = useState(true);
  const [capacities, setCapacities] = useState<FabricCapacity[]>([]);
  const [selectedCapacity, setSelectedCapacity] = useState<string>("");
  const [pauseAfterDeploy, setPauseAfterDeploy] = useState(false);
  const [capacityRefreshing, setCapacityRefreshing] = useState(false);
  const [resumingCapacity, setResumingCapacity] = useState(false);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [existingDeployCollapsed, setExistingDeployCollapsed] = useState(false);
  const [showSummary] = useState(true);
  const [initializing, setInitializing] = useState(true);
  const [loadWarning, setLoadWarning] = useState("");
  const [showResourcePreview, setShowResourcePreview] = useState(false);
  const [resourcePreviewMode, setResourcePreviewMode] = useState<"cards" | "graph">("cards");
  const [resourceGraphZoom, setResourceGraphZoom] = useState(1);
  const [graphNodeOffsets, setGraphNodeOffsets] = useState<Record<string, { x: number; y: number }>>({});
  const [graphLabelOffsets, setGraphLabelOffsets] = useState<Record<string, { x: number; y: number }>>({});
  const [graphDrag, setGraphDrag] = useState<{
    kind: "node" | "label";
    id: string;
    startX: number;
    startY: number;
    baseX: number;
    baseY: number;
  } | null>(null);
  const [deepCheckingExisting, setDeepCheckingExisting] = useState(false);
  const [fhirRegions, setFhirRegions] = useState<string[] | null>(null); // null = not loaded yet
  const [skipGroup1Collapsed, setSkipGroup1Collapsed] = useState(false);
  const [skipGroup2Collapsed, setSkipGroup2Collapsed] = useState(false);
  const [skipGroup3Collapsed, setSkipGroup3Collapsed] = useState(false);
  const [skipGroup4Collapsed, setSkipGroup4Collapsed] = useState(false);

  const [autoExportXlsx, setAutoExportXlsx] = useState(() => localStorage.getItem("autoExportXlsx") === "true");
  const [autoExportCsv, setAutoExportCsv] = useState(() => localStorage.getItem("autoExportCsv") === "true");

  useEffect(() => {
    localStorage.setItem("autoExportXlsx", String(autoExportXlsx));
  }, [autoExportXlsx]);

  useEffect(() => {
    localStorage.setItem("autoExportCsv", String(autoExportCsv));
  }, [autoExportCsv]);

  const graphContainerRef = useRef<HTMLDivElement>(null);
  const [scrollState, setScrollState] = useState({
    scrollLeft: 0,
    scrollTop: 0,
    clientWidth: 0,
    clientHeight: 0,
  });
  const [miniMapDragging, setMiniMapDragging] = useState(false);
  const [miniMapCollapsed, setMiniMapCollapsed] = useState(false);

  const handleGraphScroll = () => {
    const el = graphContainerRef.current;
    if (el) {
      setScrollState({
        scrollLeft: el.scrollLeft,
        scrollTop: el.scrollTop,
        clientWidth: el.clientWidth,
        clientHeight: el.clientHeight,
      });
    }
  };

  useEffect(() => {
    if (showResourcePreview) {
      const timer = setTimeout(() => {
        handleGraphScroll();
      }, 100);
      return () => clearTimeout(timer);
    }
  }, [showResourcePreview, resourceGraphZoom]);

  const handleMiniMapPointer = (event: React.PointerEvent<SVGSVGElement> | ReactPointerEvent) => {
    const el = graphContainerRef.current;
    if (!el) return;
    const rect = event.currentTarget.getBoundingClientRect();
    const clickX = ((event.clientX - rect.left) / rect.width) * GRAPH_WIDTH;
    const clickY = ((event.clientY - rect.top) / rect.height) * GRAPH_HEIGHT;

    const nextLeft = clickX * resourceGraphZoom - el.clientWidth / 2;
    const nextTop = clickY * resourceGraphZoom - el.clientHeight / 2;

    el.scrollTo({
      left: Math.max(0, nextLeft),
      top: Math.max(0, nextTop),
      behavior: "auto",
    });
    setScrollState({
      scrollLeft: el.scrollLeft,
      scrollTop: el.scrollTop,
      clientWidth: el.clientWidth,
      clientHeight: el.clientHeight,
    });
  };

  const handleMiniMapPointerDown = (event: ReactPointerEvent) => {
    setMiniMapDragging(true);
    event.currentTarget.setPointerCapture(event.pointerId);
    handleMiniMapPointer(event);
  };

  const handleMiniMapPointerMove = (event: ReactPointerEvent) => {
    if (miniMapDragging) {
      handleMiniMapPointer(event);
    }
  };

  const handleMiniMapPointerUp = (event: ReactPointerEvent) => {
    setMiniMapDragging(false);
    event.currentTarget.releasePointerCapture(event.pointerId);
  };

  const getCapacitySelectionValue = (capacity: FabricCapacity) => {
    return capacity.id || `${capacity.subscription}:${capacity.resourceGroup}:${capacity.name}`;
  };

  const getShortSubscriptionId = (subscriptionId?: string) => {
    if (!subscriptionId) return "";
    return subscriptionId.slice(0, 8);
  };

  const getCapacityFallbackParts = (value: string) => {
    if (!value) return null;
    if (value.startsWith("/subscriptions/")) {
      const segments = value.split("/").filter(Boolean);
      const subscriptionId = segments[1] ?? "";
      const capacityName = segments[segments.length - 1] ?? value;
      return { capacityName, subscriptionId, subscriptionName: "" };
    }
    const parts = value.split(":");
    if (parts.length >= 3) {
      return {
        subscriptionId: parts[0],
        subscriptionName: "",
        capacityName: parts[2],
      };
    }
    return {
      subscriptionId: "",
      subscriptionName: "",
      capacityName: value,
    };
  };

  const formatSubscriptionReference = (subscriptionName?: string, subscriptionId?: string) => {
    const shortId = getShortSubscriptionId(subscriptionId);
    if (subscriptionName && shortId) return `${subscriptionName} (${shortId})`;
    if (subscriptionName) return subscriptionName;
    if (shortId) return `Sub ${shortId}`;
    return "";
  };

  const isTrialCapacity = (capacity: FabricCapacity) => (capacity.sku ?? "").toUpperCase().startsWith("FT");

  const isUsableCapacity = (capacity: FabricCapacity) => {
    const sku = (capacity.sku ?? "").toUpperCase();
    return sku.startsWith("F") && !sku.startsWith("FT") && sku !== "PP3";
  };

  const usableCapacities = capacities.filter(isUsableCapacity);
  const onlyTrialCapacitiesDetected = capacities.length > 0 && capacities.every(isTrialCapacity);

  const formatCapacityRegionLabel = (capacity: FabricCapacity) => {
    const region = capacity.location ? formatAzureLocationLabel(capacity.location) : "Unknown region";
    return region;
  };

  const formatCapacityMenuLabel = (capacity: FabricCapacity) => {
    const subscriptionLabel = formatSubscriptionReference(capacity.subscriptionName, capacity.subscription);
    const regionLabel = formatCapacityRegionLabel(capacity);
    const suffix = subscriptionLabel ? ` • ${subscriptionLabel}` : "";
    return `${capacity.name} — ${capacity.sku} (${capacity.state ?? "Unknown"}) • ${regionLabel}${suffix}`;
  };

  const formatSelectedCapacityLabel = (value: string) => {
    const capacity = findCapacity(value);
    if (capacity) {
      const subscriptionLabel = formatSubscriptionReference(capacity.subscriptionName, capacity.subscription);
      const regionLabel = formatCapacityRegionLabel(capacity);
      return subscriptionLabel ? `${capacity.name} — ${regionLabel} (${subscriptionLabel})` : `${capacity.name} — ${regionLabel}`;
    }

    const fallback = getCapacityFallbackParts(value);
    if (!fallback) return "";
    const subscriptionLabel = formatSubscriptionReference(fallback.subscriptionName, fallback.subscriptionId);
    return subscriptionLabel ? `${fallback.capacityName} (${subscriptionLabel})` : fallback.capacityName;
  };

  const findCapacity = (value: string) => {
    return capacities.find((capacity) => {
      const selectionValue = getCapacitySelectionValue(capacity);
      return selectionValue === value || capacity.name === value;
    });
  };

  const refreshCapacities = () => {
    if (subscriptions.length === 0) return;
    if (usingMock) {
      // In mock mode, simulate resume completing after a few refreshes
      setCapacities((prev) => prev.map((c) =>
        c.state === "Resuming" ? { ...c, state: "Active" } : c
      ));
      setLoadWarning("");
      return;
    }
    setCapacityRefreshing(true);
    listCapacities()
      .then((allCaps) => {
        setCapacities(allCaps);
        // Update selected capacity state if it still exists
        if (selectedCapacity) {
          const updated = allCaps.find((capacity) => {
            const selectionValue = getCapacitySelectionValue(capacity);
            return selectionValue === selectedCapacity || capacity.name === selectedCapacity;
          });
          if (updated && selectedCapacity !== getCapacitySelectionValue(updated)) {
            setSelectedCapacity(getCapacitySelectionValue(updated));
          } else if (!updated) {
            setSelectedCapacity("");
          }
        }
        if (allCaps.length === 0) {
          setError("Unable to load Fabric capacities right now.");
          setLoadWarning("Unable to load Fabric capacities right now.");
        } else if (allCaps.every(isTrialCapacity)) {
          setError("Only Fabric trial capacities (FT*) were detected. Healthcare Data Solutions requires a paid Fabric F-SKU capacity.");
          setLoadWarning("Only Fabric trial capacities (FT*) were detected. Select or create a paid Fabric F-SKU capacity before deploying.");
        } else {
          setError("");
          setLoadWarning("");
        }
      })
      .catch(() => {
        setError("Failed to refresh capacity state. Try again.");
        setLoadWarning("Unable to load Fabric capacities right now.");
      })
      .finally(() => setCapacityRefreshing(false));
  };

  // Fetch real subscriptions on mount — prefer context prefetch if available
  useEffect(() => {
    if (ctxSubscriptions.length > 0) {
      setSubscriptions(ctxSubscriptions);
      setUsingMock(false);
      if (!selectedSubscription) setSelectedSubscription(ctxSubscriptions[0].id);
      return;
    }
    listSubscriptions()
      .then((subs: Array<{ id: string; name: string }>) => {
        if (subs.length > 0) {
          setSubscriptions(subs);
          setUsingMock(false);
          setLoadWarning("");
          if (!selectedSubscription) {
            setSelectedSubscription(subs[0].id);
          }
        }
      })
      .catch(() => {
        setUsingMock(true);
        setLoadWarning("Live Azure subscription scan unavailable. Using mock data.");
      });
  }, [ctxSubscriptions]); // eslint-disable-line react-hooks/exhaustive-deps

  // Fetch Fabric capacities across all subscriptions
  useEffect(() => {
    if (subscriptions.length === 0) return;
    if (usingMock) {
      // Use mock capacities in mock mode — leave selection blank so the user picks explicitly.
      const mockCaps = getMockCapacities() as FabricCapacity[];
      setCapacities(mockCaps);
      setLoadWarning("");
      setInitializing(false);
      return;
    }
    // Seed from the app-wide prefetch if available, then refresh in background.
    // Do NOT auto-select a capacity — the user must choose one explicitly so this
    // UI is safe to use across multiple users / tenants without leaking a default.
    if (ctxCapacities.length > 0 && capacities.length === 0) {
      setCapacities(ctxCapacities);
      setLoadWarning("");
      setInitializing(false);
      return;
    }
    setCapacityRefreshing(true);
    // Scan all accessible subscriptions since the capacity may live outside the currently selected Azure context.
    listCapacities()
      .then((allCaps) => {
        setCapacities(allCaps);
        if (allCaps.length === 0) {
          setLoadWarning("Unable to load Fabric capacities right now.");
        } else if (allCaps.every(isTrialCapacity)) {
          setLoadWarning("Only Fabric trial capacities (FT*) were detected. Healthcare Data Solutions requires a paid Fabric F-SKU capacity.");
        } else {
          setLoadWarning("");
        }
      })
      .catch(() => {
        setCapacities([]);
        setLoadWarning("Unable to load Fabric capacities right now.");
      })
      .finally(() => {
        setCapacityRefreshing(false);
        setInitializing(false);
      });
  }, [subscriptions, usingMock]); // eslint-disable-line react-hooks/exhaustive-deps

  // Fetch AHDS FHIR-supported regions once on mount (independent of mock mode).
  // Falls back to the currently published FHIR service region list when the
  // backend is unreachable so validation still works in mock / offline mode.
  const FHIR_FALLBACK_REGIONS = [
    "australiaeast", "canadacentral", "centralindia", "eastus", "eastus2",
    "francecentral", "germanywestcentral", "japaneast", "koreacentral",
    "northcentralus", "northeurope", "qatarcentral", "southcentralus",
    "southeastasia", "swedencentral", "switzerlandnorth", "uksouth",
    "westcentralus", "westeurope", "westus2", "westus3",
  ];
  useEffect(() => {
    listFhirRegions().then((regions) => {
      setFhirRegions(normalizeFhirRegionList(regions.length > 0 ? regions : FHIR_FALLBACK_REGIONS));
    });
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const supportedFhirRegionIds = useMemo(
    () => new Set((fhirRegions ?? []).map(normalizeAzureLocation)),
    [fhirRegions]
  );
  const fhirRegionLabels = useMemo(
    () => (fhirRegions ?? []).map(formatAzureLocationLabel),
    [fhirRegions]
  );

  const [showJsonEditor, setShowJsonEditor] = useState(false);
  const [config, setConfig] = useState<DeploymentConfig>({
    resource_group_name: "",
    location: "eastus",
    admin_security_group: "",
    fabric_workspace_name: "",
    patient_count: 100,
    tags: { SecurityControl: "Ignore" },
    skip_base_infra: false,
    skip_fhir: false,
    skip_dicom: false,
    skip_fabric: false,
    alert_email: "",
    capacity_subscription_id: "",
    capacity_resource_group: "",
    capacity_name: "",
    pause_capacity_after_deploy: false,
    reuse_patients: false,
    use_cached_synthea: false,
    // Granular component toggles
    skip_synthea: false,
    skip_device_assoc: false,
    skip_fhir_export: false,
    skip_rti_phase2: false,
    skip_hds_pipelines: false,
    skip_data_agents: false,
    skip_imaging: false,
    skip_ontology: false,
    skip_activator: false,
    skip_quality_measures: false,
    require_bronze_clinical_fhir: false,
    require_bronze_imaging_dicom: false,
    skip_phase7: false,
    skip_payer_rti: false,
    skip_payer_activator: false,
    skip_ops_agent: false,
    skip_graph_agent: false,
    payer_ops_email: "",
    claim_event_rate_per_minute: 60,
  });

  const [useNamingConvention, setUseNamingConvention] = useState(true);
  const [useTags, setUseTags] = useState(true);
  const [tagRows, setTagRows] = useState<Array<{ name: string; value: string }>>([
    { name: "SecurityControl", value: "Ignore" },
  ]);
  const [namingPrefix, setNamingPrefix] = useState("");
  const [existingDeploy, setExistingDeploy] = useState<ExistingDeploymentInfo | null>(null);
  const [checkingExisting, setCheckingExisting] = useState(false);
  const [overridePriorSettings, setOverridePriorSettings] = useState(false);

  // Determine which card needs attention next
  const activeCardIndex = useNamingConvention && !namingPrefix ? 0
    : !useNamingConvention && (!config.resource_group_name || !config.fabric_workspace_name) ? 1
      : !selectedCapacity || !config.admin_security_group ? 1
        : !config.fabric_workspace_name && !useNamingConvention ? 2
          : !config.patient_count || (!config.skip_activator && !config.alert_email?.trim()) ? 3
            : -1; // all filled — no glow

  // Calculate completion status for cards
  const getCardCompletion = (cardIndex: number): { complete: number; total: number } => {
    switch (cardIndex) {
      case 0: // Naming
        return { complete: (!useNamingConvention || !!namingPrefix) ? 1 : 0, total: 1 };
      case 1: // Azure Config
        const azureFields = [selectedSubscription, selectedCapacity, config.admin_security_group];
        if (!useNamingConvention) azureFields.push(config.resource_group_name);
        return { complete: azureFields.filter(Boolean).length, total: azureFields.length };
      case 2: // Fabric Config
        return { complete: config.fabric_workspace_name ? 1 : 0, total: 1 };
      case 3: // Data Config
        const requiredDataFields: (string | number | null)[] = [config.patient_count];
        if (!config.skip_activator) {
          requiredDataFields.push(config.alert_email?.trim() || null);
        }
        if (!config.skip_phase7 && !config.skip_payer_activator) {
          requiredDataFields.push(config.payer_ops_email?.trim() || config.alert_email?.trim() || null);
        }
        return { complete: requiredDataFields.filter(Boolean).length, total: requiredDataFields.length };
      default:
        return { complete: 0, total: 0 };
    }
  };

  // Calculate estimated duration
  const getEstimatedDurationMinutes = (): number => {
    let minutes = 5; // Base infrastructure
    if (!config.skip_fhir && !config.skip_synthea) minutes += Math.ceil(config.patient_count / 10);
    if (!config.skip_dicom) minutes += 20;
    if (!config.skip_fabric) minutes += 15;
    if (!config.skip_hds_pipelines) minutes += 10;
    if (!config.skip_data_agents) minutes += 5;
    if (!config.skip_imaging) minutes += 5;
    if (!config.skip_phase7) minutes += 8;
    if (!config.skip_phase7 && !config.skip_payer_rti) minutes += 5;
    return minutes;
  };

  const getEstimatedDuration = (): string => {
    const minutes = getEstimatedDurationMinutes();
    return minutes >= 60 ? `${Math.floor(minutes / 60)}h ${minutes % 60}m` : `${minutes}m`;
  };

  const applyPreset = (preset: "demo" | "full" | "infra" | "repair" | "data") => {
    setShowAdvanced(preset !== "demo");
    setConfig((prev) => {
      const base: DeploymentConfig = {
        ...prev,
        // Presets must never override a patient count the user explicitly entered.
        // Only supply a preset default when no value is set yet.
        patient_count: prev.patient_count || (preset === "demo" ? 25 : 100),
        skip_base_infra: false,
        skip_fhir: false,
        skip_dicom: false,
        skip_fabric: false,
        skip_synthea: false,
        skip_device_assoc: false,
        skip_fhir_export: false,
        skip_rti_phase2: false,
        skip_hds_pipelines: false,
        skip_data_agents: false,
        skip_imaging: false,
        skip_ontology: false,
        skip_activator: !prev.alert_email,
        skip_quality_measures: false,
        skip_phase7: false,
        skip_payer_rti: false,
        skip_payer_activator: !prev.alert_email && !prev.payer_ops_email,
        skip_ops_agent: false,
        skip_graph_agent: false,
        payer_ops_email: prev.payer_ops_email || "",
        claim_event_rate_per_minute: prev.claim_event_rate_per_minute || 60,
      };
      if (preset === "demo") {
        return { ...base, skip_imaging: true, skip_quality_measures: true, skip_payer_activator: !prev.alert_email && !prev.payer_ops_email };
      }
      if (preset === "infra") {
        return {
          ...base,
          skip_fhir: true,
          skip_dicom: true,
          skip_fabric: true,
          skip_synthea: true,
          skip_device_assoc: true,
          skip_fhir_export: true,
          skip_rti_phase2: true,
          skip_hds_pipelines: true,
          skip_data_agents: true,
          skip_imaging: true,
          skip_ontology: true,
          skip_activator: true,
          skip_quality_measures: true,
          skip_phase7: true,
          skip_payer_rti: true,
          skip_payer_activator: true,
          skip_ops_agent: true,
          skip_graph_agent: true,
        };
      }
      if (preset === "repair") {
        return { ...base, reuse_patients: true, skip_synthea: true, skip_device_assoc: true, skip_phase7: true, skip_payer_rti: true, skip_payer_activator: true, skip_ops_agent: true, skip_graph_agent: true };
      }
      if (preset === "data") {
        return { ...base, skip_base_infra: true, skip_fhir: true, skip_dicom: true, skip_synthea: true, skip_device_assoc: true, skip_phase7: true, skip_payer_rti: true, skip_payer_activator: true, skip_ops_agent: true, skip_graph_agent: true };
      }
      return base;
    });
  };

  const enabledComponents = [
    [!config.skip_base_infra, "Infrastructure"],
    [!config.skip_fhir, "FHIR"],
    [!config.skip_synthea, "Synthea"],
    [!config.skip_dicom, "DICOM"],
    [!config.skip_fabric, "Fabric RTI"],
    [!config.skip_hds_pipelines, "HDS Pipelines"],
    [!config.skip_data_agents, "Data Agents"],
    [!config.skip_imaging, "Imaging"],
    [!config.skip_ontology, "Ontology"],
    [!config.skip_activator && !!config.alert_email, "Alerts"],
    [!config.skip_quality_measures, "Quality"],
    [!config.skip_phase7 && !config.skip_payer_rti, "Payer RTI"],
    [!config.skip_phase7 && !config.skip_payer_activator, "Payer Activator"],
    [!config.skip_phase7 && !config.skip_ops_agent, "Ops Agents"],
    [!config.skip_phase7 && !config.skip_graph_agent, "Graph Agent"],
  ].filter(([enabled]) => enabled).map(([, label]) => label as string);

  const uniqueSuffix = selectedSubscription && config.resource_group_name
    ? "{uniqueString(resourceGroup().id)}"
    : "{uniqueString(rg)}";
  const appName = `masimo${uniqueSuffix}`;
  const hdsWorkspaceName = `hdws${uniqueSuffix}`;
  const fhirServiceName = `fhir${uniqueSuffix}`;
  const storageAccountName = `stfhir${uniqueSuffix}`;

  const prospectiveAzureAssets = [
    { enabled: true, type: "Resource group", name: config.resource_group_name || "rg-<deployment>" },
    { enabled: !config.skip_base_infra, type: "Event Hubs namespace", name: `${appName}-eh-ns` },
    { enabled: !config.skip_base_infra, type: "Event Hub", name: "telemetry-stream" },
    { enabled: !config.skip_phase7 && !config.skip_payer_rti && !config.skip_base_infra, type: "Event Hub", name: "claim-stream" },
    { enabled: !config.skip_phase7 && !config.skip_payer_rti && !config.skip_base_infra, type: "ACI container group", name: "claim-emulator-grp" },
    { enabled: !config.skip_base_infra, type: "Authorization rule", name: "emulator-access" },
    { enabled: !config.skip_base_infra, type: "Container Registry", name: `${appName}acr` },
    { enabled: !config.skip_base_infra, type: "Key Vault", name: `${appName}-kv` },
    { enabled: !config.skip_base_infra, type: "ACI container group", name: "masimo-emulator-grp" },
    { enabled: !config.skip_fhir, type: "Health Data Services workspace", name: hdsWorkspaceName },
    { enabled: !config.skip_fhir, type: "FHIR service", name: fhirServiceName },
    { enabled: !config.skip_fhir, type: "Storage account / ADLS Gen2", name: storageAccountName },
    { enabled: !config.skip_fhir, type: "Blob container", name: "synthea-output" },
    { enabled: !config.skip_fhir, type: "Blob container", name: "fhir-export" },
    { enabled: !config.skip_dicom, type: "Blob container", name: "dicom-output" },
    { enabled: !config.skip_fhir, type: "User-assigned managed identity", name: "id-aci-fhir-jobs" },
    { enabled: !config.skip_synthea, type: "ACI job", name: "synthea-generator-job" },
    { enabled: !config.skip_fhir && !config.skip_synthea, type: "ACI job", name: "fhir-loader-job" },
    { enabled: !config.skip_dicom, type: "ACI job", name: "dicom-loader-job" },
    { enabled: !config.skip_imaging, type: "Container App", name: "hds-dicom-proxy" },
    { enabled: !config.skip_imaging, type: "Static Web App", name: "OHIF DICOM viewer" },
  ].filter((asset) => asset.enabled);

  const prospectiveFabricAssets = [
    { enabled: true, type: "Fabric workspace", name: config.fabric_workspace_name || "<workspace>" },
    { enabled: true, type: "Workspace managed identity", name: `${config.fabric_workspace_name || "<workspace>"} identity` },
    { enabled: !config.skip_fabric, type: "Eventhouse", name: "MasimoEventhouse" },
    { enabled: !config.skip_fabric, type: "KQL Database", name: "MasimoKQLDB" },
    { enabled: !config.skip_fabric, type: "KQL table", name: "TelemetryRaw" },
    { enabled: !config.skip_fabric, type: "KQL table", name: "AlertHistory" },
    { enabled: !config.skip_phase7 && !config.skip_payer_rti, type: "KQL table", name: "claims_events" },
    { enabled: !config.skip_phase7 && !config.skip_payer_rti, type: "KQL table", name: "fraud_scores" },
    { enabled: !config.skip_phase7 && !config.skip_payer_rti, type: "KQL table", name: "highcost_alerts" },
    { enabled: !config.skip_phase7 && !config.skip_payer_rti, type: "KQL table", name: "care_gap_alerts" },
    { enabled: !config.skip_phase7 && !config.skip_payer_activator, type: "Reflex", name: "PayerOpsActivator" },
    { enabled: !config.skip_phase7 && !config.skip_ops_agent, type: "Operations Agent", name: "HealthcareOpsAgent" },
    { enabled: !config.skip_phase7 && !config.skip_ops_agent, type: "Data Agent", name: "Payer Ops Triage" },
    { enabled: !config.skip_phase7 && !config.skip_graph_agent, type: "Data Agent", name: "Healthcare Graph Agent" },
    { enabled: !config.skip_fabric, type: "Eventstream", name: "MasimoTelemetryStream" },
    { enabled: !config.skip_fhir_export, type: "OneLake shortcut", name: `FHIR export → ${storageAccountName}/fhir-export` },
    { enabled: !config.skip_hds_pipelines, type: "Lakehouse", name: "Healthcare Bronze Lakehouse (HDS)" },
    { enabled: !config.skip_hds_pipelines, type: "Lakehouse", name: "Healthcare Silver Lakehouse (HDS)" },
    { enabled: !config.skip_hds_pipelines, type: "Pipeline", name: "Clinical pipeline" },
    { enabled: !config.skip_hds_pipelines, type: "Pipeline", name: "Imaging pipeline" },
    { enabled: !config.skip_hds_pipelines, type: "Pipeline", name: "OMOP pipeline" },
    { enabled: !config.skip_hds_pipelines, type: "Shortcut", name: "DICOM-HDS" },
    { enabled: !config.skip_data_agents, type: "Data Agent", name: "Patient 360 / Clinical Triage agents" },
    { enabled: !config.skip_imaging, type: "Data Agent", name: "DICOM Cohorting Agent" },
    { enabled: !config.skip_imaging, type: "Lakehouse", name: "healthcare1_reporting_gold" },
    { enabled: !config.skip_ontology, type: "Notebook", name: "create_device_association_table" },
    { enabled: !config.skip_ontology, type: "Ontology", name: "ClinicalDeviceOntology" },
    { enabled: !config.skip_activator && !!config.alert_email, type: "Reflex", name: "ClinicalAlertActivator" },
    { enabled: !config.skip_quality_measures, type: "Notebook / report", name: "Population Health & Quality Dashboard" },
  ].filter((asset) => asset.enabled);

  const graphNodes = [
    { id: "synthea", label: "Synthea\nPatient generator", group: "External", x: 30, y: 60, enabled: !config.skip_synthea },
    { id: "tcia", label: "TCIA\nDICOM studies", group: "External", x: 30, y: 210, enabled: !config.skip_dicom },
    { id: "emulator", label: "Masimo Emulator\nACI", group: "Azure", x: 30, y: 520, enabled: !config.skip_base_infra },
    { id: "fhir", label: `FHIR Service\n${fhirServiceName}`, group: "Azure", x: 320, y: 60, enabled: !config.skip_fhir },
    { id: "dicom-loader", label: "DICOM Loader\nTCIA → ADLS", group: "Azure", x: 320, y: 210, enabled: !config.skip_dicom },
    { id: "eventhub", label: "Event Hub\ntelemetry-stream", group: "Azure", x: 320, y: 520, enabled: !config.skip_base_infra },
    { id: "adls", label: `ADLS Gen2\n${storageAccountName}`, group: "Azure", x: 610, y: 210, enabled: !config.skip_fhir || !config.skip_dicom },
    { id: "claimemulator", label: "Claim Emulator\nACI", group: "Azure", x: 30, y: 650, enabled: !config.skip_phase7 && !config.skip_payer_rti && !config.skip_base_infra },
    { id: "claimhub", label: "Event Hub\nclaim-stream", group: "Azure", x: 320, y: 650, enabled: !config.skip_phase7 && !config.skip_payer_rti && !config.skip_base_infra },
    { id: "eventstream", label: "Eventstream\nMasimoTelemetryStream", group: "Fabric", x: 610, y: 520, enabled: !config.skip_fabric },
    { id: "bronze", label: "Bronze Lakehouse\nHDS", group: "Fabric", x: 900, y: 60, enabled: !config.skip_hds_pipelines },
    { id: "eventhouse", label: "Eventhouse / KQL\nMasimoKQLDB", group: "Fabric", x: 900, y: 365, enabled: !config.skip_fabric },
    { id: "silver", label: "Silver Lakehouse\nHDS", group: "Fabric", x: 1190, y: 60, enabled: !config.skip_hds_pipelines },
    { id: "gold", label: "Gold OMOP Lakehouse", group: "Fabric", x: 1480, y: 60, enabled: !config.skip_hds_pipelines },
    { id: "agents", label: "Data Agents\nPatient 360 / Triage", group: "Fabric", x: 1480, y: 365, enabled: !config.skip_data_agents },
    { id: "reporting", label: "Reporting LH\nPower BI / OHIF", group: "Fabric+Azure", x: 1480, y: 520, enabled: !config.skip_imaging },
    { id: "quality", label: "Population Health\n& Quality", group: "Fabric", x: 1770, y: 60, enabled: !config.skip_quality_measures },
    { id: "ontology", label: "ClinicalDeviceOntology", group: "Fabric", x: 1770, y: 365, enabled: !config.skip_ontology },
    { id: "activator", label: "Data Activator\nClinicalAlertActivator", group: "Fabric", x: 1770, y: 520, enabled: !config.skip_activator && !!config.alert_email },
    { id: "payerkql", label: "Payer RTI KQL\nclaims + scores", group: "Fabric", x: 1190, y: 650, enabled: !config.skip_phase7 && !config.skip_payer_rti },
    { id: "payerops", label: "Payer Ops\nAgents + Activator", group: "Fabric", x: 1480, y: 650, enabled: !config.skip_phase7 && (!config.skip_ops_agent || !config.skip_payer_activator) },
    { id: "graphagent", label: "Healthcare Graph Agent", group: "Fabric", x: 1770, y: 650, enabled: !config.skip_phase7 && !config.skip_graph_agent },
  ].filter((node) => node.enabled);

  const positionedGraphNodes = graphNodes.map((node) => {
    const offset = graphNodeOffsets[node.id] ?? { x: 0, y: 0 };
    return { ...node, x: node.x + offset.x, y: node.y + offset.y };
  });
  const graphNodeIds = new Set(positionedGraphNodes.map((node) => node.id));
  const graphEdges = [
    { id: "synthea-fhir", from: "synthea", to: "fhir", label: "FHIR bundles", lx: 0, ly: -36 },
    { id: "fhir-adls", from: "fhir", to: "adls", label: "$export NDJSON", lx: -15, ly: -30 },
    { id: "tcia-adls", from: "tcia", to: "adls", label: "re-tag + upload", lx: 0, ly: -36 },
    { id: "emulator-eventhub", from: "emulator", to: "eventhub", label: "telemetry", lx: 0, ly: -36 },
    { id: "eventhub-eventstream", from: "eventhub", to: "eventstream", label: "source", lx: 0, ly: -36 },
    { id: "eventstream-eventhouse", from: "eventstream", to: "eventhouse", label: "TelemetryRaw", lx: -18, ly: -35 },
    { id: "claimemulator-claimhub", from: "claimemulator", to: "claimhub", label: "claims", lx: 0, ly: -36 },
    { id: "claimhub-eventstream", from: "claimhub", to: "eventstream", label: "claim source", lx: 0, ly: -36 },
    { id: "eventstream-payerkql", from: "eventstream", to: "payerkql", label: "claims_events", lx: 0, ly: -36 },
    { id: "payerkql-payerops", from: "payerkql", to: "payerops", label: "worklist", lx: 0, ly: -36 },
    { id: "ontology-graphagent", from: "ontology", to: "graphagent", label: "manual attach", lx: 0, ly: -36 },
    { id: "adls-bronze", from: "adls", to: "bronze", label: "OneLake shortcut", lx: -18, ly: -35 },
    { id: "bronze-silver", from: "bronze", to: "silver", label: "HDS pipelines", lx: 0, ly: -36 },
    { id: "silver-gold", from: "silver", to: "gold", label: "OMOP", lx: 0, ly: -36 },
    { id: "silver-eventhouse", from: "silver", to: "eventhouse", label: "KQL shortcuts", lx: -50, ly: -10, curvature: -45 },
    { id: "eventhouse-agents", from: "eventhouse", to: "agents", label: "alerts", lx: 0, ly: -36 },
    { id: "silver-agents", from: "silver", to: "agents", label: "clinical data", lx: -30, ly: -30, curvature: -45 },
    { id: "gold-reporting", from: "gold", to: "reporting", label: "cohorts", lx: 80, ly: 0, curvature: -120 },
    { id: "silver-reporting", from: "silver", to: "reporting", label: "Direct Lake", lx: -60, ly: 20, curvature: -80 },
    { id: "ontology-agents", from: "ontology", to: "agents", label: "semantic binding", lx: 0, ly: -36 },
    { id: "ontology-reporting", from: "ontology", to: "reporting", label: "semantic binding", lx: 20, ly: -20 },
    { id: "eventhouse-activator", from: "eventhouse", to: "activator", label: "fn_ClinicalAlerts", lx: 20, ly: 30, curvature: -50 },
    { id: "silver-quality", from: "silver", to: "quality", label: "FHIR/claims", lx: 0, ly: -45, curvature: -60 },
    { id: "gold-quality", from: "gold", to: "quality", label: "quality measures", lx: 0, ly: -36 },
  ].filter((edge) => graphNodeIds.has(edge.from) && graphNodeIds.has(edge.to));

  const graphNodeById = new Map(positionedGraphNodes.map((node) => [node.id, node]));
  const GRAPH_WIDTH = 1960;
  const GRAPH_HEIGHT = 780;
  const NODE_WIDTH = 165;
  const NODE_HEIGHT = 74;

  const getEdgeGeom = (edge: typeof graphEdges[0]) => {
    const from = graphNodeById.get(edge.from)!;
    const to = graphNodeById.get(edge.to)!;

    const fromCenterX = from.x + NODE_WIDTH / 2;
    const fromCenterY = from.y + NODE_HEIGHT / 2;
    const toCenterX = to.x + NODE_WIDTH / 2;
    const toCenterY = to.y + NODE_HEIGHT / 2;

    const startPt = getRectIntersection(from.x, from.y, NODE_WIDTH, NODE_HEIGHT, toCenterX, toCenterY);
    const endPt = getRectIntersection(to.x, to.y, NODE_WIDTH, NODE_HEIGHT, fromCenterX, fromCenterY);

    const dx = endPt.x - startPt.x;
    const dy = endPt.y - startPt.y;
    const len = Math.hypot(dx, dy) || 1;

    const startGap = 6;
    const endGap = 10;

    const x1 = startPt.x + (dx / len) * startGap;
    const y1 = startPt.y + (dy / len) * startGap;
    const x2 = endPt.x - (dx / len) * endGap;
    const y2 = endPt.y - (dy / len) * endGap;

    const curvature = (edge as any).curvature ?? 0;
    let cx = (x1 + x2) / 2;
    let cy = (y1 + y2) / 2;

    if (curvature !== 0) {
      const lineDx = x2 - x1;
      const lineDy = y2 - y1;
      const lineLen = Math.hypot(lineDx, lineDy) || 1;
      const nx = -lineDy / lineLen;
      const ny = lineDx / lineLen;
      cx = cx + nx * curvature;
      cy = cy + ny * curvature;
    }

    const labelOffset = graphLabelOffsets[edge.id] ?? { x: 0, y: 0 };
    const midX = 0.25 * x1 + 0.5 * cx + 0.25 * x2 + (edge.lx ?? 0) + labelOffset.x;
    const midY = 0.25 * y1 + 0.5 * cy + 0.25 * y2 + (edge.ly ?? 0) + labelOffset.y;
    const labelWidth = Math.max(132, edge.label.length * 7.5 + 28);

    return {
      pathD: `M ${x1} ${y1} Q ${cx} ${cy} ${x2} ${y2}`,
      midX,
      midY,
      labelWidth,
    };
  };
  const graphLabelVisible = resourceGraphZoom >= 0.85;
  const graphColor = (group: string) => group === "Azure"
    ? tokens.colorPaletteBlueBackground2
    : group === "External"
      ? tokens.colorNeutralBackground3
      : group === "Fabric+Azure"
        ? tokens.colorPalettePurpleBackground2
        : tokens.colorBrandBackground2;

  const getGraphPointer = (event: ReactPointerEvent, svg: SVGSVGElement) => {
    const rect = svg.getBoundingClientRect();
    return {
      x: ((event.clientX - rect.left) / rect.width) * GRAPH_WIDTH,
      y: ((event.clientY - rect.top) / rect.height) * GRAPH_HEIGHT,
    };
  };

  const startGraphNodeDrag = (event: ReactPointerEvent<SVGGElement>, nodeId: string) => {
    const svg = event.currentTarget.ownerSVGElement;
    if (!svg) return;
    event.preventDefault();
    const point = getGraphPointer(event, svg);
    const base = graphNodeOffsets[nodeId] ?? { x: 0, y: 0 };
    setGraphDrag({ kind: "node", id: nodeId, startX: point.x, startY: point.y, baseX: base.x, baseY: base.y });
  };

  const startGraphLabelDrag = (event: ReactPointerEvent<SVGGElement>, edgeId: string) => {
    const svg = event.currentTarget.ownerSVGElement;
    if (!svg) return;
    event.preventDefault();
    event.stopPropagation();
    const point = getGraphPointer(event, svg);
    const base = graphLabelOffsets[edgeId] ?? { x: 0, y: 0 };
    setGraphDrag({ kind: "label", id: edgeId, startX: point.x, startY: point.y, baseX: base.x, baseY: base.y });
  };

  const updateGraphDrag = (event: ReactPointerEvent<SVGSVGElement>) => {
    if (!graphDrag) return;
    const point = getGraphPointer(event, event.currentTarget);
    const next = { x: graphDrag.baseX + point.x - graphDrag.startX, y: graphDrag.baseY + point.y - graphDrag.startY };
    if (graphDrag.kind === "node") {
      setGraphNodeOffsets((prev) => ({ ...prev, [graphDrag.id]: next }));
    } else {
      setGraphLabelOffsets((prev) => ({ ...prev, [graphDrag.id]: next }));
    }
  };

  const resetGraphLayout = () => {
    setGraphNodeOffsets({});
    setGraphLabelOffsets({});
    setGraphDrag(null);
  };

  const copyDeploymentPlan = () => {
    const plan = {
      resourceGroup: config.resource_group_name,
      workspace: config.fabric_workspace_name,
      subscription: selectedSubscription,
      location: config.location,
      capacity: selectedCapacity ? formatSelectedCapacityLabel(selectedCapacity) : "",
      patientCount: config.patient_count,
      estimatedDuration: getEstimatedDuration(),
      components: enabledComponents,
      azureAssets: prospectiveAzureAssets,
      fabricAssets: prospectiveFabricAssets,
      tags: config.tags,
    };
    navigator.clipboard?.writeText(JSON.stringify(plan, null, 2)).catch(() => undefined);
  };

  const update = (field: keyof DeploymentConfig, value: unknown) => {
    setConfig((prev) => {
      const next = { ...prev, [field]: value };

      // ── Dependency auto-toggle rules ──
      // When a component is disabled, auto-disable its dependents.
      // When re-enabled, dependents stay as-is (user re-enables manually).

      // skip_fhir → forces skip_synthea, skip_device_assoc, skip_fhir_export
      if (field === "skip_fhir" && value) {
        next.skip_synthea = true;
        next.skip_device_assoc = true;
        next.skip_fhir_export = true;
      }
      // skip_synthea → forces skip_device_assoc (no patients = no devices)
      if (field === "skip_synthea" && value) {
        next.skip_device_assoc = true;
      }
      if (field === "skip_phase7" && value) {
        next.skip_payer_rti = true;
        next.skip_payer_activator = true;
        next.skip_ops_agent = true;
        next.skip_graph_agent = true;
      }
      // skip_dicom → forces skip_hds_pipelines, skip_imaging
      if (field === "skip_dicom" && value) {
        next.skip_hds_pipelines = true;
        next.skip_imaging = true;
      }
      // skip_fabric (RTI) → forces skip_fhir_export, skip_rti_phase2, skip_activator
      if (field === "skip_fabric" && value) {
        next.skip_fhir_export = true;
        next.skip_rti_phase2 = true;
        next.skip_activator = true;
        next.skip_phase7 = true;
        next.skip_payer_rti = true;
        next.skip_payer_activator = true;
        next.skip_ops_agent = true;
        next.skip_graph_agent = true;
      }
      // skip_base_infra → forces skip_synthea, skip_fhir, skip_dicom, skip_device_assoc
      if (field === "skip_base_infra" && value) {
        next.skip_fhir = true;
        next.skip_synthea = true;
        next.skip_device_assoc = true;
        next.skip_dicom = true;
        next.skip_fhir_export = true;
        next.skip_hds_pipelines = true;
        next.skip_imaging = true;
      }

      // Re-enabling a parent → unblock children (restore to not-skipped)
      if (field === "skip_fhir" && !value) {
        next.skip_synthea = false;
        next.skip_device_assoc = false;
        next.skip_fhir_export = false;
      }
      if (field === "skip_synthea" && !value) {
        next.skip_device_assoc = false;
      }
      if (field === "skip_dicom" && !value) {
        next.skip_hds_pipelines = false;
        next.skip_imaging = false;
      }
      if (field === "skip_fabric" && !value) {
        next.skip_fhir_export = false;
        next.skip_rti_phase2 = false;
        next.skip_activator = false;
      }
      if (field === "skip_base_infra" && !value) {
        next.skip_fhir = false;
        next.skip_synthea = false;
        next.skip_device_assoc = false;
        next.skip_dicom = false;
        next.skip_fhir_export = false;
        next.skip_hds_pipelines = false;
        next.skip_imaging = false;
      }
      if (field === "skip_phase7" && !value) {
        next.skip_payer_rti = false;
        next.skip_payer_activator = !next.alert_email && !next.payer_ops_email;
        next.skip_ops_agent = false;
        next.skip_graph_agent = false;
      }

      return next;
    });
  };

  // Check for existing deployment when workspace/RG names are set
  useEffect(() => {
    const ws = config.fabric_workspace_name;
    const rg = config.resource_group_name;
    if (!ws && !rg) {
      setExistingDeploy(null);
      return;
    }
    const abortController = new AbortController();
    const timer = setTimeout(() => {
      setCheckingExisting(true);
      checkExistingDeployment(ws, rg, abortController.signal)
        .then((info) => {
          if (abortController.signal.aborted) return;
          setExistingDeploy(info);
          if (info) {
            update("reuse_patients", true);
            setOverridePriorSettings(false);
            // Auto-populate config fields from prior deployment
            const pc = info.priorConfig;
            if (pc) {
              setConfig((prev) => ({
                ...prev,
                location: pc.location || prev.location,
                admin_security_group: pc.admin_security_group || prev.admin_security_group,
                alert_email: pc.alert_email || prev.alert_email,
                patient_count: pc.patient_count || prev.patient_count,
                reuse_patients: true,
                use_cached_synthea: pc.use_cached_synthea ?? prev.use_cached_synthea,
              }));
              // Auto-select capacity if it was used before
              if (pc.capacity_name) {
                setSelectedCapacity(pc.capacity_name);
              }
              // Restore tags
              if (pc.tags && Object.keys(pc.tags).length > 0) {
                setUseTags(true);
                setTagRows(
                  Object.entries(pc.tags).map(([name, value]) => ({ name, value }))
                );
              }
            }
          }
        })
        .catch(() => {
          if (!abortController.signal.aborted) setExistingDeploy(null);
        })
        .finally(() => {
          if (!abortController.signal.aborted) setCheckingExisting(false);
        });
    }, 500); // debounce
    return () => {
      clearTimeout(timer);
      abortController.abort();
    };
  }, [config.fabric_workspace_name, config.resource_group_name]); // eslint-disable-line react-hooks/exhaustive-deps

  // When naming prefix changes, auto-derive RG and workspace names
  const handleNamingChange = (prefix: string) => {
    // Azure resource names: max 90 chars, alphanumeric + dashes
    const sanitized = prefix.replace(/[^a-zA-Z0-9-]/g, "").substring(0, 40);
    setNamingPrefix(sanitized);
    if (useNamingConvention && sanitized) {
      setConfig((prev) => ({
        ...prev,
        resource_group_name: `rg-${sanitized}`,
        fabric_workspace_name: sanitized,
      }));
    }
  };

  const handleNamingToggle = (checked: boolean) => {
    setUseNamingConvention(checked);
    if (checked && namingPrefix) {
      setConfig((prev) => ({
        ...prev,
        resource_group_name: `rg-${namingPrefix}`,
        fabric_workspace_name: namingPrefix,
      }));
    }
  };

  // Sync tagRows → config.tags
  const syncTags = (rows: Array<{ name: string; value: string }>) => {
    const parsed: Record<string, string> = {};
    for (const row of rows) {
      if (row.name.trim()) {
        parsed[row.name.trim()] = row.value.trim();
      }
    }
    update("tags", parsed);
  };

  const updateTagRow = (index: number, field: "name" | "value", val: string) => {
    setTagRows((prev) => {
      const next = [...prev];
      next[index] = { ...next[index], [field]: val };
      syncTags(next);
      return next;
    });
  };

  const addTagRow = () => {
    setTagRows((prev) => [...prev, { name: "", value: "" }]);
  };

  const removeTagRow = (index: number) => {
    setTagRows((prev) => {
      const next = prev.filter((_, i) => i !== index);
      if (next.length === 0) next.push({ name: "", value: "" });
      syncTags(next);
      return next;
    });
  };

  const startActualDeployment = async () => {
    setLoading(true);
    setDeploymentStartMessage("Validating local auth and starting the backend deployment run…");
    setError("");

    try {
      // Save tags to history before deploying
      if (Object.keys(config.tags).length > 0) {
        addTagToHistory(config.tags);
      }
      // Inject capacity fields from state
      const cap = findCapacity(selectedCapacity);
      const fallbackCapacity = getCapacityFallbackParts(selectedCapacity);
      const deployConfig: DeploymentConfig = {
        ...config,
        location: normalizeAzureLocation(config.location),
        capacity_name: cap?.name ?? fallbackCapacity?.capacityName ?? selectedCapacity,
        capacity_resource_group: cap?.resourceGroup ?? config.capacity_resource_group ?? "",
        capacity_subscription_id: cap?.subscription ?? fallbackCapacity?.subscriptionId ?? selectedSubscription,
        pause_capacity_after_deploy: pauseAfterDeploy,
      };
      const { instanceId } = await startDeployment(deployConfig);
      navigate(`/monitor/${instanceId}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Unknown error");
      setDeploymentStartMessage("");
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = async () => {
    if (validationErrors.length > 0) {
      setError(validationErrors[0]);
      return;
    }
    setShowResourcePreview(true);
  };

  const runLiveExistingValidation = async () => {
    if (!config.fabric_workspace_name && !config.resource_group_name) return;
    setDeepCheckingExisting(true);
    try {
      const info = await checkExistingDeployment(config.fabric_workspace_name, config.resource_group_name, undefined, true);
      if (info) setExistingDeploy(info);
    } finally {
      setDeepCheckingExisting(false);
    }
  };

  const handleMockDeploy = () => {
    // For mock mode, auto-fill workspace name if empty
    const mockConfig = {
      ...config,
      location: normalizeAzureLocation(config.location),
      fabric_workspace_name: config.fabric_workspace_name || "med-device-rti-hds-demo",
    };
    const instanceId = startMockDeployment(mockConfig);
    navigate(`/monitor/${instanceId}`);
  };

  const locationUnsupported = fhirRegions !== null &&
    !config.skip_fhir &&
    !supportedFhirRegionIds.has(normalizeAzureLocation(config.location));

  const validationErrors = useMemo(() => {
    const issues: string[] = [];
    if (!selectedSubscription) issues.push("Select an Azure subscription.");
    if (!selectedCapacity) issues.push("Select a Fabric capacity.");
    if (onlyTrialCapacitiesDetected) issues.push("Only Fabric trial capacities (FT*) were detected. Healthcare Data Solutions requires a paid Fabric F-SKU capacity.");
    const selectedCap = findCapacity(selectedCapacity);
    if (selectedCap && !isUsableCapacity(selectedCap)) issues.push(`Selected Fabric capacity ${selectedCap.name} uses unsupported SKU ${selectedCap.sku || "unknown"}. Select a paid Fabric F-SKU capacity.`);
    if (!config.admin_security_group?.trim()) issues.push("Admin Security Group is required.");
    if (useNamingConvention && !namingPrefix.trim()) issues.push("Deployment Name is required when naming convention is enabled.");
    if (!useNamingConvention && !config.resource_group_name?.trim()) issues.push("Resource Group Name is required.");
    if (!config.fabric_workspace_name?.trim()) issues.push("Fabric workspace name is required.");
    if (!config.skip_activator && !config.alert_email?.trim()) issues.push("Alert email is required.");
    if (!config.patient_count || config.patient_count < 1) issues.push("Patient count must be at least 1.");
    if (locationUnsupported) issues.push(`Not an acceptable region. Please select a supported AHDS FHIR region.`);
    return issues;
  }, [
    selectedSubscription,
    selectedCapacity,
    capacities,
    onlyTrialCapacitiesDetected,
    config.admin_security_group,
    config.resource_group_name,
    config.fabric_workspace_name,
    config.alert_email,
    config.patient_count,
    config.location,
    config.skip_fhir,
    useNamingConvention,
    namingPrefix,
    locationUnsupported,
  ]);

  const canStartDeployment = validationErrors.length === 0 && !loading;

  useEffect(() => {
    if (!error) return;
    setError("");
  }, [
    selectedSubscription,
    selectedCapacity,
    config.admin_security_group,
    config.resource_group_name,
    config.fabric_workspace_name,
    config.alert_email,
    config.patient_count,
    config.location,
    useNamingConvention,
    namingPrefix,
  ]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: tokens.spacingHorizontalXXL, position: "relative" }}>
      {/* Main content area */}
      <div style={{ flex: 1, minWidth: 0 }}>
        {usingMock && <MockDataBanner />}
        {loadWarning && (
          <div style={{
            marginBottom: tokens.spacingVerticalS,
            padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalM}`,
            backgroundColor: tokens.colorStatusWarningBackground1,
            borderLeft: `4px solid ${tokens.colorStatusWarningBorderActive}`,
            borderRadius: tokens.borderRadiusMedium,
          }}>
            <Text size={200}>{loadWarning}</Text>
          </div>
        )}
        <div className={styles.stickyHeader}>
          <Title2>Deployment Settings</Title2>
        </div>

        {/* Responsive grid: auto-flow dense packing eliminates wasted space */}
        <style>{`
        .deploy-form-grid {
          display: flex;
          flex-direction: column;
          gap: 16px;
          margin-top: 16px;
        }
        .deploy-form-grid > * {
          animation: deploy-card-in 0.5s ease both;
        }
        .deploy-columns-row {
          display: flex;
          flex-direction: column;
          gap: 16px;
        }
        .deploy-column {
          display: flex;
          flex-direction: column;
          gap: 16px;
          min-width: 0;
        }
        .deploy-column > * {
          animation: deploy-card-in 0.5s ease both;
        }
        .deploy-column > *:nth-child(1) { animation-delay: 0.05s; }
        .deploy-column > *:nth-child(2) { animation-delay: 0.12s; }
        .deploy-column > *:nth-child(3) { animation-delay: 0.19s; }
        @keyframes deploy-card-in {
          from { opacity: 0; transform: translateY(16px); }
          to   { opacity: 1; transform: translateY(0); }
        }
        .deploy-card-active {
          outline: 3px solid ${tokens.colorBrandStroke1} !important;
          outline-offset: 4px;
          animation: deploy-card-in 0.5s ease both, deploy-card-pulse 2s ease-in-out 0.6s infinite !important;
        }
        @keyframes deploy-card-pulse {
          0%, 100% { box-shadow: 0 0 16px ${tokens.colorBrandBackground2}; outline-color: ${tokens.colorBrandStroke1}; }
          50%      { box-shadow: 0 0 36px ${tokens.colorBrandBackground2Hover}; outline-color: ${tokens.colorBrandStroke2}; }
        }
        @media (min-width: 1200px) {
          .deploy-columns-row {
            flex-direction: row;
            align-items: flex-start;
          }
          .deploy-column {
            flex: 1;
          }
        }
        /* Compact padding on wide screens */
        @media (min-width: 1400px) {
          .deploy-compact-padding .fui-CardHeader {
            padding: 12px 16px;
          }
          .deploy-compact-padding .deploy-field-group {
            padding: 0 16px 12px;
          }
        }
        /* Tall cards span 2 rows for better visual hierarchy */
        .deploy-card-tall {
          grid-row: span 2;
        }
        /* Sticky card headers */
        .deploy-card-sticky-header .fui-CardHeader {
          position: sticky;
          top: 0;
          z-index: 5;
          background-color: inherit;
          border-bottom: 1px solid ${tokens.colorNeutralStroke2};
        }
        /* Collapsible section animation */
        .deploy-collapsible-content {
          overflow: hidden;
          transition: max-height 0.3s ease, opacity 0.3s ease;
        }
        .deploy-collapsible-collapsed {
          max-height: 0 !important;
          opacity: 0;
        }
        /* Advanced options toggle */
        .deploy-advanced-toggle {
          margin: 12px 0;
          padding: 10px 16px;
          background: ${tokens.colorNeutralBackground3};
          border: 1px solid ${tokens.colorNeutralStroke2};
          border-radius: 6px;
          cursor: pointer;
          display: flex;
          align-items: center;
          justify-content: space-between;
          transition: all 0.2s;
          color: ${tokens.colorNeutralForeground1};
        }
        .deploy-advanced-toggle:hover {
          background: ${tokens.colorNeutralBackground4};
          border-color: ${tokens.colorBrandStroke1};
        }
        .deploy-advanced-toggle svg {
          color: ${tokens.colorNeutralForeground2};
        }
        ${reducedMotion ? `
        .deploy-form-grid > *,
        .deploy-card-active,
        .deploy-collapsible-content,
        .deploy-advanced-toggle {
          animation: none !important;
          transition: none !important;
        }
        ` : ""}
        @media (prefers-reduced-motion: reduce) {
          .deploy-form-grid > *,
          .deploy-card-active,
          .deploy-collapsible-content {
            animation: none !important;
            transition: none !important;
          }
        }
      `}</style>

        {initializing ? (
          <div className={`${styles.form} deploy-form-grid`}>
            <Card className={styles.section}><CardHeader header={<Subtitle1>Loading deployment configuration...</Subtitle1>} /></Card>
            <Card className={styles.section}><CardHeader header={<Subtitle1>Loading capacities and subscriptions...</Subtitle1>} /></Card>
            <Card className={styles.section}><CardHeader header={<Subtitle1>Preparing defaults...</Subtitle1>} /></Card>
          </div>
        ) : (

          <div className={`${styles.form} deploy-form-grid deploy-compact-padding`}>
            {/* Deployment Presets */}
            <Card className={`${styles.section} ${styles.sectionFullWidth}`} style={{ overflow: "visible" }}>
              <CardHeader
                header={
                  <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <Subtitle1>Deployment Presets</Subtitle1>
                    <Badge color="brand" size="small" icon={<FlashRegular />}>Fast start</Badge>
                  </div>
                }
                description="Choose a safe default profile, then fine-tune individual components below."
              />
              <div className={styles.fieldGroup}>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: tokens.spacingHorizontalS }}>
                  <Button appearance="outline" onClick={() => applyPreset("demo")}>Demo / fastest</Button>
                  <Button appearance="outline" onClick={() => applyPreset("full")}>Full platform</Button>
                  <Button appearance="outline" onClick={() => applyPreset("infra")}>Infra only</Button>
                  <Button appearance="outline" onClick={() => applyPreset("repair")}>Resume / repair</Button>
                  <Button appearance="outline" onClick={() => applyPreset("data")}>Data pipeline only</Button>
                </div>
                <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>
                  Current plan enables {enabledComponents.length} component(s): {enabledComponents.slice(0, 8).join(", ")}{enabledComponents.length > 8 ? ` +${enabledComponents.length - 8} more` : ""}.
                </Text>
              </div>
            </Card>

            <div className="deploy-columns-row">
              <div className="deploy-column">
                {/* Naming Convention */}
                <Card className={`${styles.section} ${styles.cardRequired}${activeCardIndex === 0 ? " deploy-card-active" : ""}`} style={{ overflow: "visible" }}>
                  <CardHeader
                    className={styles.sectionHeader}
                    header={
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <Subtitle1>Naming Convention</Subtitle1>
                        {(() => {
                          const { complete, total } = getCardCompletion(0);
                          return complete === total ? (
                            <Badge color="success" size="small" icon={<CheckmarkCircleRegular />}>Complete</Badge>
                          ) : (
                            <Badge color="informative" size="small" icon={<CircleRegular />}>{complete}/{total}</Badge>
                          );
                        })()}
                      </div>
                    }
                    description="Auto-generate consistent names for Azure and Fabric resources"
                  />
                  <div className={styles.fieldGroup}>
                    <Checkbox
                      checked={useNamingConvention}
                      onChange={(_, d) => handleNamingToggle(!!d.checked)}
                      label="Use naming convention (recommended)"
                    />
                    {useNamingConvention && (
                      <>
                        <Field
                          label={
                            <InfoLabel info="Enter a short prefix like 'rojo-0404'. The Resource Group will be 'rg-rojo-0404' and the Fabric Workspace will be 'rojo-0404'." infoButton={{ popover: { positioning: "after" } }}>
                              <span className={styles.fieldLabelWithIcon}>
                                <img src="/icon-deployment.svg" alt="" width={16} height={16} />
                                <span className={styles.labelSeparator} />
                                Deployment Name
                              </span>
                            </InfoLabel>
                          }
                          required
                        >
                          <HistoryInput
                            field="naming-prefix"
                            value={namingPrefix}
                            onChange={(v) => handleNamingChange(v)}
                            placeholder="e.g. rojo-0404"
                          />
                        </Field>
                        {namingPrefix && (
                          <div style={{
                            display: "flex",
                            flexDirection: "column",
                            gap: tokens.spacingVerticalXXS,
                            padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalM}`,
                            backgroundColor: tokens.colorNeutralBackground3,
                            borderRadius: tokens.borderRadiusMedium,
                            fontSize: tokens.fontSizeBase200,
                          }}>
                            <Text size={200}>
                              <Text weight="semibold" size={200}>Resource Group:</Text> rg-{namingPrefix}
                            </Text>
                            <Text size={200}>
                              <Text weight="semibold" size={200}>Fabric Workspace:</Text> {namingPrefix}
                            </Text>
                          </div>
                        )}
                      </>
                    )}
                  </div>
                </Card>

                {/* Azure Configuration */}
                <Card className={`${styles.section} ${styles.cardRequired}${activeCardIndex === 1 ? " deploy-card-active" : ""}`} style={{ overflow: "visible" }}>
                  <CardHeader
                    className={styles.sectionHeader}
                    header={
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <Subtitle1>Azure Configuration</Subtitle1>
                        {(() => {
                          const { complete, total } = getCardCompletion(1);
                          return complete === total ? (
                            <Badge color="success" size="small" icon={<CheckmarkCircleRegular />}>Complete</Badge>
                          ) : (
                            <Badge color="informative" size="small" icon={<CircleRegular />}>{complete}/{total}</Badge>
                          );
                        })()}
                      </div>
                    }
                    description="Target Azure subscription and resource group settings"
                  />
                  <div className={styles.fieldGroup}>

                    {existingDeploy?.priorConfig && (
                      <div style={{
                        padding: tokens.spacingHorizontalM,
                        backgroundColor: tokens.colorNeutralBackground4,
                        borderLeft: `4px solid ${tokens.colorBrandStroke1}`,
                        borderRadius: tokens.borderRadiusMedium,
                        marginBottom: tokens.spacingVerticalS,
                      }}>
                        <div
                          onClick={() => setExistingDeployCollapsed(!existingDeployCollapsed)}
                          style={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer" }}
                        >
                          <Text size={200} weight="semibold">
                            <Badge color="informative" size="small" style={{ marginRight: 6 }}>Auto-populated</Badge>
                            Settings from prior deployment
                          </Text>
                          {existingDeployCollapsed ? <ChevronDownRegular /> : <ChevronUpRegular />}
                        </div>
                        <div className={`deploy-collapsible-content${existingDeployCollapsed ? " deploy-collapsible-collapsed" : ""}`}
                          style={{ maxHeight: existingDeployCollapsed ? "0" : "200px" }}
                        >
                          <Text size={200} block style={{ marginTop: 8 }}>
                            Restored from deployment <strong>{existingDeploy.instanceId}</strong>
                          </Text>
                          <Checkbox
                            checked={overridePriorSettings}
                            onChange={(_, d) => setOverridePriorSettings(!!d.checked)}
                            label="Override previous settings"
                            style={{ marginTop: tokens.spacingVerticalXS }}
                          />
                        </div>
                      </div>
                    )}
                    <Field
                      label={
                        <InfoLabel info="Azure subscription where infrastructure resources will be deployed. This selection also applies to the Teardown tab." infoButton={{ popover: { positioning: "after" } }}>
                          <span className={styles.fieldLabelWithIcon}>
                            <img src="/azure_logo.svg" alt="" width={16} height={16} />
                            <span className={styles.labelSeparator} />
                            Azure Subscription
                          </span>
                        </InfoLabel>
                      }
                    >
                      <Dropdown
                        value={subscriptions.find((s) => s.id === selectedSubscription)?.name ?? "Select…"}
                        selectedOptions={[selectedSubscription]}
                        onOptionSelect={(_, data) => setSelectedSubscription(data.optionValue as string)}
                      >
                        {subscriptions.map((s) => (
                          <Option key={s.id} value={s.id}>{s.name}</Option>
                        ))}
                      </Dropdown>
                    </Field>
                    <Field
                      label={
                        <InfoLabel info="The Fabric capacity backing the workspace. Used to pause billing after deployment. Capacities are scanned across all subscriptions." infoButton={{ popover: { positioning: "after" } }}>
                          <span className={styles.fieldLabelWithIcon}>
                            <img src="/fabric_16_color.svg" alt="" width={16} height={16} />
                            <span className={styles.labelSeparator} />
                            Fabric Capacity
                          </span>
                        </InfoLabel>
                      }
                    >
                      <div className={styles.capacityFieldRow}>
                        <Dropdown
                          style={{ flex: 1 }}
                          value={
                            selectedCapacity
                              ? (() => {
                                return formatSelectedCapacityLabel(selectedCapacity);
                              })()
                              : capacityRefreshing
                                ? "Refreshing capacity status..."
                                : capacities.length === 0 ? "No capacities found" : onlyTrialCapacitiesDetected ? "Only trial capacities detected" : "Select…"
                          }
                          selectedOptions={selectedCapacity ? [selectedCapacity] : []}
                          onOptionSelect={(_, data) => setSelectedCapacity(data.optionValue as string)}
                          disabled={(usableCapacities.length === 0 && capacityRefreshing) || onlyTrialCapacitiesDetected}
                        >
                          {usableCapacities.map((c) => (
                            <Option key={getCapacitySelectionValue(c)} value={getCapacitySelectionValue(c)} text={formatCapacityMenuLabel(c)}>
                              {formatCapacityMenuLabel(c)}
                            </Option>
                          ))}
                        </Dropdown>
                        <Tooltip content="Refresh capacity status" relationship="label">
                          <Button
                            appearance="subtle"
                            icon={<ArrowSyncRegular />}
                            size="small"
                            onClick={refreshCapacities}
                            disabled={capacityRefreshing}
                            style={capacityRefreshing ? { animation: "spin 1s linear infinite" } : undefined}
                          />
                        </Tooltip>
                        {(() => {
                          const cap = findCapacity(selectedCapacity);
                          if (!cap) return null;
                          const isActive = cap.state === "Active";
                          const isPaused = cap.state === "Paused" || cap.state === "Suspended";
                          const isResuming = cap.state === "Resuming" || resumingCapacity;
                          return (
                            <>
                              {/* Status badge — always visible */}
                              {isActive && !resumingCapacity && (
                                <Badge color="success" size="small">Active</Badge>
                              )}
                              {isResuming && (
                                <Badge color="warning" size="small" style={{ animation: "pulse 1.5s ease-in-out infinite" }}>
                                  {cap.state === "Active" ? "Active ✓" : "Resuming…"}
                                </Badge>
                              )}
                              {isPaused && !resumingCapacity && (
                                <Badge color="danger" size="small">{cap.state}</Badge>
                              )}
                              {/* Resume button — only when not active and not already resuming */}
                              {!isActive && (
                                <Tooltip content={`Resume capacity "${cap.name}" (currently ${cap.state})`} relationship="label">
                                  <Button
                                    appearance="primary"
                                    icon={<PlayRegular />}
                                    size="small"
                                    disabled={resumingCapacity}
                                    onClick={async () => {
                                      setResumingCapacity(true);
                                      setError("");
                                      try {
                                        if (usingMock) {
                                          // Mock: set state to Resuming, then Active after a delay
                                          setCapacities((prev) => prev.map((c) =>
                                            c.name === cap.name ? { ...c, state: "Resuming" } : c
                                          ));
                                          setTimeout(() => {
                                            setCapacities((prev) => prev.map((c) =>
                                              c.name === cap.name ? { ...c, state: "Active" } : c
                                            ));
                                            setResumingCapacity(false);
                                          }, 5000);
                                          return;
                                        }
                                        await resumeCapacity(cap.subscription, cap.resourceGroup, cap.name);
                                        // Poll capacity status until Active (max 3 min)
                                        let elapsed = 0;
                                        const capName = cap.name;
                                        const poll = setInterval(() => {
                                          elapsed += 5;
                                          refreshCapacities();
                                          if (elapsed >= 180) {
                                            clearInterval(poll);
                                            setResumingCapacity(false);
                                          }
                                        }, 5000);
                                        const checkActive = setInterval(() => {
                                          setCapacities((current) => {
                                            const fresh = current.find((c) => c.name === capName);
                                            if (fresh?.state === "Active") {
                                              clearInterval(poll);
                                              clearInterval(checkActive);
                                              setResumingCapacity(false);
                                            }
                                            return current;
                                          });
                                        }, 3000);
                                        setTimeout(() => clearInterval(checkActive), 180000);
                                      } catch (e) {
                                        setError(e instanceof Error ? e.message : "Failed to resume capacity");
                                        setResumingCapacity(false);
                                      }
                                    }}
                                  >
                                    {resumingCapacity ? "Resuming…" : "Resume"}
                                  </Button>
                                </Tooltip>
                              )}
                              {isActive && showAdvanced && (
                                <Tooltip content={`Pause capacity "${cap.name}" now. Confirm because this can interrupt Fabric workloads.`} relationship="label">
                                  <Button
                                    appearance="secondary"
                                    size="small"
                                    onClick={async () => {
                                      if (!window.confirm(`Pause Fabric capacity ${cap.name}? This may interrupt active Fabric workloads.`)) return;
                                      setError("");
                                      try {
                                        await pauseCapacity(cap.subscription, cap.resourceGroup, cap.name);
                                        await refreshCapacities();
                                      } catch (e) {
                                        setError(e instanceof Error ? e.message : "Failed to pause capacity");
                                      }
                                    }}
                                  >
                                    Pause now
                                  </Button>
                                </Tooltip>
                              )}
                            </>
                          );
                        })()}
                      </div>
                    </Field>
                    <Field
                      label={
                        <InfoLabel info="Azure resource group where Event Hub, ACR, FHIR Service, and ACI containers are deployed." infoButton={{ popover: { positioning: "after" } }}>
                          <span className={styles.fieldLabelWithIcon}>
                            <img src="/icon-resource-group.svg" alt="" width={16} height={16} />
                            <span className={styles.labelSeparator} />
                            Resource Group Name
                          </span>
                        </InfoLabel>
                      }
                    >
                      <HistoryInput
                        field="resource-group"
                        value={config.resource_group_name}
                        onChange={(v) => update("resource_group_name", v)}
                        disabled={useNamingConvention}
                        placeholder={useNamingConvention ? "Set via naming convention above" : "e.g. rg-medtech-rti-fhir"}
                      />
                    </Field>
                    <Field
                      label={
                        <InfoLabel info="Azure region for all resources. Must support FHIR Service and Event Hubs." infoButton={{ popover: { positioning: "after" } }}>
                          <span className={styles.fieldLabelWithIcon}>
                            <img src="/icon-location.svg" alt="" width={16} height={16} />
                            <span className={styles.labelSeparator} />
                            Location
                          </span>
                        </InfoLabel>
                      }
                      validationState={locationUnsupported ? "error" : undefined}
                      validationMessage={locationUnsupported
                        ? `Not an acceptable region. Please select from the following: ${fhirRegionLabels.join(", ")}`
                        : undefined}
                    >
                      <HistoryInput
                        field="location"
                        value={config.location}
                        onChange={(v) => update("location", v)}
                        disabled={!!existingDeploy?.priorConfig && !overridePriorSettings}
                        suggestions={fhirRegionLabels}
                        suggestionsLabel="Supported AHDS FHIR regions"
                      />
                    </Field>
                    <Field
                      label={
                        <InfoLabel info="Entra ID security group granted admin access to FHIR Service and Key Vault." infoButton={{ popover: { positioning: "after" } }}>
                          <span className={styles.fieldLabelWithIcon}>
                            <img src="/icon-groups.svg" alt="" width={16} height={16} />
                            <span className={styles.labelSeparator} />
                            Admin Security Group
                          </span>
                        </InfoLabel>
                      }
                    >
                      <HistoryInput
                        field="admin-security-group"
                        value={config.admin_security_group}
                        onChange={(v) => update("admin_security_group", v)}
                        disabled={!!existingDeploy?.priorConfig && !overridePriorSettings}
                      />
                    </Field>
                    {/* Advanced Options Toggle */}
                    <div className="deploy-advanced-toggle" onClick={() => setShowAdvanced(!showAdvanced)}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <SettingsRegular style={{ fontSize: 16, color: tokens.colorBrandForeground1 }} />
                        <Text weight="semibold" size={300}>Advanced Options</Text>
                      </div>
                      {showAdvanced ? <ChevronUpRegular /> : <ChevronDownRegular />}
                    </div>
                    <div className={`deploy-collapsible-content${!showAdvanced ? " deploy-collapsible-collapsed" : ""}`}
                      style={{ maxHeight: showAdvanced ? "800px" : "0" }}
                    >
                      <Checkbox
                        checked={useTags}
                        onChange={(_, d) => {
                          setUseTags(!!d.checked);
                          if (!d.checked) {
                            update("tags", {});
                            setTagRows([{ name: "", value: "" }]);
                          }
                        }}
                        label="Add resource tags"
                      />
                      {useTags && (
                        <div>
                          <TagHistoryPanel
                            onSelect={(tags) => {
                              const rows = Object.entries(tags).map(([name, value]) => ({ name, value }));
                              if (rows.length === 0) rows.push({ name: "", value: "" });
                              setTagRows(rows);
                              syncTags(rows);
                            }}
                          />
                          <div style={{
                            display: "grid",
                            gridTemplateColumns: "1fr auto 1fr auto",
                            gap: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalS}`,
                            alignItems: "center",
                            marginBottom: tokens.spacingVerticalXS,
                          }}>
                            <Text weight="semibold" size={200}>Name</Text>
                            <span />
                            <Text weight="semibold" size={200}>Value</Text>
                            <span />
                          </div>
                          {tagRows.map((row, i) => (
                            <div
                              key={i}
                              style={{
                                display: "grid",
                                gridTemplateColumns: "1fr auto 1fr auto",
                                gap: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalS}`,
                                alignItems: "center",
                                marginBottom: tokens.spacingVerticalXS,
                              }}
                            >
                              <Input
                                value={row.name}
                                onChange={(_, d) => updateTagRow(i, "name", d.value)}
                                placeholder="e.g. SecurityControl"
                                size="small"
                              />
                              <Text size={300} style={{ color: tokens.colorNeutralForeground3 }}>:</Text>
                              <Input
                                value={row.value}
                                onChange={(_, d) => updateTagRow(i, "value", d.value)}
                                placeholder="e.g. Ignore"
                                size="small"
                              />
                              <Button
                                appearance="subtle"
                                icon={<DismissRegular />}
                                size="small"
                                onClick={() => removeTagRow(i)}
                                disabled={tagRows.length === 1 && !row.name && !row.value}
                              />
                            </div>
                          ))}
                          <Button
                            appearance="subtle"
                            icon={<AddRegular />}
                            size="small"
                            onClick={addTagRow}
                            style={{ marginTop: tokens.spacingVerticalXS }}
                          >
                            Add tag
                          </Button>
                        </div>
                      )}
                    </div>
                  </div>
                </Card>
              </div>

              <div className="deploy-column">
                {/* Fabric Configuration */}
                <Card className={`${styles.section} ${styles.cardRequired}${activeCardIndex === 2 ? " deploy-card-active" : ""}`}>
                  <CardHeader
                    className={styles.sectionHeader}
                    header={
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <Subtitle1>Fabric Configuration</Subtitle1>
                        {(() => {
                          const { complete, total } = getCardCompletion(2);
                          return complete === total ? (
                            <Badge color="success" size="small" icon={<CheckmarkCircleRegular />}>Complete</Badge>
                          ) : (
                            <Badge color="informative" size="small" icon={<CircleRegular />}>{complete}/{total}</Badge>
                          );
                        })()}
                      </div>
                    }
                    description="Microsoft Fabric workspace where RTI, Lakehouses, and Data Agents are deployed"
                  />
                  <div className={styles.fieldGroup}>
                    <Field
                      label={
                        <InfoLabel info="The Fabric workspace must already exist. Eventhouse, KQL databases, Eventstream, Lakehouses, and Data Agents will be created here." infoButton={{ popover: { positioning: "after" } }}>
                          <span className={styles.fieldLabelWithIcon}>
                            <img src="/fabric_16_color.svg" alt="" width={16} height={16} />
                            <span className={styles.labelSeparator} />
                            Fabric Workspace Name
                          </span>
                        </InfoLabel>
                      }
                      required={!useNamingConvention}
                    >
                      <HistoryInput
                        field="fabric-workspace"
                        value={config.fabric_workspace_name}
                        onChange={(v) => update("fabric_workspace_name", v)}
                        disabled={useNamingConvention}
                        placeholder={useNamingConvention ? "Set via naming convention above" : "e.g. med-device-rti-hds"}
                      />
                    </Field>
                    {selectedCapacity && showAdvanced && (
                      <Checkbox
                        checked={pauseAfterDeploy}
                        onChange={(_, d) => setPauseAfterDeploy(!!d.checked)}
                        label={`Pause capacity "${formatSelectedCapacityLabel(selectedCapacity)}" after successful deployment`}
                      />
                    )}
                  </div>
                </Card>

                {/* Data Configuration */}
                <Card className={`${styles.section} ${styles.cardRequired}${activeCardIndex === 3 ? " deploy-card-active" : ""}`} style={{ overflow: "visible" }}>
                  <CardHeader
                    className={styles.sectionHeader}
                    header={
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <Subtitle1>Data Configuration</Subtitle1>
                        {(() => {
                          const { complete, total } = getCardCompletion(3);
                          return complete === total ? (
                            <Badge color="success" size="small" icon={<CheckmarkCircleRegular />}>Complete</Badge>
                          ) : (
                            <Badge color="danger" size="small">Required</Badge>
                          );
                        })()}
                      </div>
                    }
                    description="Synthetic patient data generation and alerting"
                  />
                  <div className={styles.fieldGroup}>

                    {/* Existing deployment detection banner */}
                    {checkingExisting && (
                      <div style={{
                        padding: tokens.spacingHorizontalS,
                        backgroundColor: tokens.colorNeutralBackground4,
                        borderLeft: `4px solid ${tokens.colorBrandStroke1}`,
                        borderRadius: tokens.borderRadiusMedium,
                        marginBottom: tokens.spacingVerticalS,
                        display: "flex",
                        alignItems: "center",
                        gap: tokens.spacingHorizontalS,
                      }}>
                        <Badge color="informative" size="small">Checking</Badge>
                        <Text size={200}>Checking local deployment history...</Text>
                      </div>
                    )}

                    {existingDeploy && (
                      <div style={{
                        padding: tokens.spacingHorizontalM,
                        backgroundColor: tokens.colorStatusWarningBackground1,
                        borderLeft: `4px solid ${tokens.colorStatusWarningBorderActive}`,
                        borderRadius: tokens.borderRadiusMedium,
                        marginBottom: tokens.spacingVerticalS,
                      }}>
                        <div
                          onClick={() => setExistingDeployCollapsed(!existingDeployCollapsed)}
                          style={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer" }}
                        >
                          <Text weight="semibold">
                            Previous deployment detected
                          </Text>
                          {existingDeployCollapsed ? <ChevronDownRegular /> : <ChevronUpRegular />}
                        </div>
                        <div className={`deploy-collapsible-content${existingDeployCollapsed ? " deploy-collapsible-collapsed" : ""}`}
                          style={{ maxHeight: existingDeployCollapsed ? "0" : "400px" }}
                        >
                          <Text size={200} block style={{ marginTop: 4, color: tokens.colorNeutralForeground2 }}>
                            Workspace <strong>{existingDeploy.workspaceName}</strong> was deployed on{" "}
                            {new Date(existingDeploy.createdTime).toLocaleString()}
                          </Text>
                          <Button size="small" appearance="outline" onClick={runLiveExistingValidation} disabled={deepCheckingExisting} style={{ marginTop: tokens.spacingVerticalS }}>
                            {deepCheckingExisting ? "Validating live Azure/FHIR state…" : "Run live Azure/FHIR validation"}
                          </Button>
                          {existingDeploy.azureRgExists && (
                            <>
                              <Text size={200} block style={{ marginTop: 4 }}>
                                FHIR: <strong>{existingDeploy.fhirPatientCount}</strong> patients,{" "}
                                <strong>{existingDeploy.fhirDeviceCount}</strong> Masimo devices
                              </Text>
                              <Tooltip
                                content="FHIR $export writes NDJSON files to ADLS Gen2. HDS pipelines, Bronze Lakehouse shortcuts, and Silver/Gold tables all depend on this data. If 0, the $export has not run yet — it will be triggered automatically on deploy."
                                relationship="description"
                                positioning="above"
                              >
                                <Text size={200} block style={{
                                  marginTop: 4,
                                  padding: `${tokens.spacingVerticalXXS} ${tokens.spacingHorizontalS}`,
                                  borderRadius: tokens.borderRadiusMedium,
                                  backgroundColor: (existingDeploy.exportedFiles ?? 0) === 0
                                    ? tokens.colorStatusDangerBackground1
                                    : "transparent",
                                  cursor: "help",
                                }}>
                                  {(existingDeploy.exportedFiles ?? 0) === 0 && (
                                    <Badge color="danger" size="small" style={{ marginRight: 6 }}>Critical</Badge>
                                  )}
                                  Storage: <strong>{existingDeploy.exportedFiles ?? 0}</strong> exported FHIR files,{" "}
                                  <strong>{existingDeploy.dicomStudies ?? 0}</strong> DICOM imaging blobs
                                  {(existingDeploy.exportedFiles ?? 0) === 0 && (
                                    <span style={{ color: tokens.colorStatusDangerForeground1, marginLeft: 6 }}>
                                      — $export required for HDS pipelines
                                    </span>
                                  )}
                                </Text>
                              </Tooltip>
                              <Text size={200} block style={{ marginTop: 2 }}>
                                {existingDeploy.emulatorRunning ? (
                                  <>Emulator: <strong style={{ color: tokens.colorPaletteGreenForeground1 }}>running</strong> ({existingDeploy.emulatorDeviceCount ?? 100} devices streaming telemetry)</>
                                ) : (
                                  <>Emulator: <strong style={{ color: tokens.colorStatusDangerForeground1 }}>stopped</strong></>
                                )}
                              </Text>
                            </>
                          )}
                          <div style={{ marginTop: tokens.spacingVerticalS, display: "flex", flexDirection: "column", gap: tokens.spacingVerticalXS }}>
                            <Checkbox
                              checked={config.reuse_patients}
                              onChange={(_, d) => update("reuse_patients", !!d.checked)}
                              label={`Reuse existing ${existingDeploy.fhirPatientCount} patients and ${existingDeploy.fhirDeviceCount} devices`}
                            />
                            <Text size={200} style={{ color: tokens.colorNeutralForeground3, paddingLeft: 28 }}>
                              {config.reuse_patients
                                ? "Synthea generation, FHIR Loader, and DICOM Loader will be skipped. Emulator stays running."
                                : `New batch of ${config.patient_count} patients will be generated with ${config.patient_count} new device associations. Existing data will be cleared and replaced.`}
                            </Text>
                          </div>
                        </div>
                      </div>
                    )}

                    <Field
                      label={
                        <InfoLabel info="Number of synthetic patients generated by Synthea. More patients = longer FHIR load time. 100 patients ≈ 15 min." infoButton={{ popover: { positioning: "after" } }}>
                          <span className={styles.fieldLabelWithIcon}>
                            <img src="/icon-patient.svg" alt="" width={14} height={14} />
                            <span className={styles.labelSeparator} />
                            Patient Count{config.reuse_patients ? " (ignored — reusing existing)" : existingDeploy ? " (new batch)" : " (to be generated)"}
                          </span>
                        </InfoLabel>
                      }
                    >
                      <SpinButton
                        value={config.patient_count}
                        min={10}
                        max={10000}
                        step={10}
                        onChange={(_, d) => {
                          // Respect what the user enters: prefer the numeric value, else parse the
                          // typed display string; never silently snap back to a default.
                          const parsed = d.value ?? (d.displayValue != null ? parseInt(d.displayValue, 10) : NaN);
                          if (!Number.isNaN(parsed)) update("patient_count", parsed);
                        }}
                        disabled={config.reuse_patients}
                      />
                    </Field>
                    <div style={{ marginTop: tokens.spacingVerticalS, display: "flex", flexDirection: "column", gap: tokens.spacingVerticalXS, marginBottom: tokens.spacingVerticalS }}>
                      <Checkbox
                        checked={config.use_cached_synthea}
                        onChange={(_, d) => update("use_cached_synthea", !!d.checked)}
                        disabled={config.reuse_patients || config.skip_synthea}
                        label="Use Prepackaged Patient Bundles (Skip generation container)"
                      />
                      <Text size={200} style={{ color: tokens.colorNeutralForeground3, paddingLeft: 28 }}>
                        {config.use_cached_synthea
                          ? "Loads pre-generated high-fidelity clinical patient JSON data from cache, saving 15-30 minutes of ACI container generation time."
                          : "Generates new randomized patient and medical device telemetry data on-the-fly using an Azure Synthea container."}
                      </Text>
                    </div>
                    <Field
                      label={
                        <InfoLabel info="Email address for clinical alert notifications via Data Activator (Reflex)." infoButton={{ popover: { positioning: "after" } }}>
                          Alert Email
                        </InfoLabel>
                      }
                      required
                    >
                      <HistoryInput
                        field="alert-email"
                        value={config.alert_email}
                        onChange={(v) => update("alert_email", v)}
                        placeholder="operator@example.com"
                        type="email"
                      />
                    </Field>
                  </div>
                </Card>

                {/* Phase & Component Control */}
                <Card className={`${styles.section} ${styles.cardOptional}`}>
                  <CardHeader
                    className={styles.sectionHeader}
                    header={
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <Subtitle1>Phase &amp; Component Control</Subtitle1>
                        <Badge color="informative" size="small">Optional</Badge>
                      </div>
                    }
                    description={
                      <span className={styles.fieldLabelWithIcon}>
                        <img src="/icon-phases.svg" alt="" width={14} height={14} />
                        <span className={styles.labelSeparator} />
                        Toggle individual components — dependencies auto-adjust
                      </span>
                    }
                  />
                  <div className={styles.checkboxGroup}>
                    {/* ── Group 1: Infrastructure & Data Ingestion (Phase 1) ── */}
                    <div
                      onClick={() => setSkipGroup1Collapsed(!skipGroup1Collapsed)}
                      style={{
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                        padding: "8px 12px",
                        backgroundColor: tokens.colorNeutralBackground3,
                        borderRadius: tokens.borderRadiusMedium,
                        cursor: "pointer",
                        userSelect: "none",
                        marginTop: tokens.spacingVerticalXS,
                        borderLeft: `4px solid ${tokens.colorBrandStroke1}`
                      }}
                    >
                      <Text weight="semibold" size={300} style={{ color: tokens.colorBrandForeground1 }}>
                        1. Data Fabric Foundation
                      </Text>
                      {skipGroup1Collapsed ? <ChevronDownRegular /> : <ChevronUpRegular />}
                    </div>
                    <div className={`deploy-collapsible-content ${skipGroup1Collapsed ? "deploy-collapsible-collapsed" : ""}`} style={{ display: "flex", flexDirection: "column", gap: tokens.spacingVerticalXS, padding: "8px 12px 12px", transition: "all 0.3s ease" }}>
                      <Tooltip content="Skip Event Hub, ACR, emulator ACI, Storage, and Bicep infra" relationship="description" positioning="after">
                        <Checkbox
                          label={`Azure Synthetic Data Foundation${existingDeploy ? " (already deployed)" : ""}`}
                          checked={!config.skip_base_infra}
                          onChange={(_, d) => update("skip_base_infra", !d.checked)}
                        />
                      </Tooltip>
                      <Tooltip content="Skip FHIR R4 service deployment and data loading" relationship="description" positioning="after">
                        <Checkbox
                          label={`FHIR Service + Data Loading${existingDeploy ? " (already deployed)" : ""}`}
                          checked={!config.skip_fhir}
                          onChange={(_, d) => update("skip_fhir", !d.checked)}
                          disabled={config.skip_base_infra}
                        />
                      </Tooltip>
                      <div style={{ paddingLeft: 24, display: "flex", flexDirection: "column", gap: tokens.spacingVerticalXS }}>
                        <Tooltip content="Skip Synthea patient generation — use existing patients" relationship="description" positioning="after">
                          <Checkbox
                            label="Synthea Patient Generation"
                            checked={!config.skip_synthea}
                            onChange={(_, d) => update("skip_synthea", !d.checked)}
                            disabled={config.skip_fhir}
                          />
                        </Tooltip>
                        {!config.skip_synthea && (
                          <Tooltip content="Skip starting ACI patient generator and upload prepackaged patient bundles instead." relationship="description" positioning="after">
                            <Checkbox
                              label="Use Prepackaged Patient Bundles"
                              checked={config.use_cached_synthea}
                              onChange={(_, d) => update("use_cached_synthea", !!d.checked)}
                              disabled={config.skip_fhir}
                              style={{ marginLeft: 24 }}
                            />
                          </Tooltip>
                        )}
                        <Tooltip content="Skip Device and/or DICOM patient cohort association mapping" relationship="description" positioning="after">
                          <Checkbox
                            label="Device and/or DICOM Association"
                            checked={!config.skip_device_assoc}
                            onChange={(_, d) => update("skip_device_assoc", !d.checked)}
                            disabled={config.skip_synthea || config.skip_fhir}
                          />
                        </Tooltip>
                      </div>
                      <Tooltip content="Skip TCIA DICOM download, re-tagging, ADLS upload, and FHIR ImagingStudy creation" relationship="description" positioning="after">
                        <Checkbox
                          label="DICOM Download + Upload"
                          checked={!config.skip_dicom}
                          onChange={(_, d) => update("skip_dicom", !d.checked)}
                          disabled={config.skip_base_infra}
                        />
                      </Tooltip>
                    </div>

                    {/* ── Group 2: Active Patient Telemetry (Phase 2) ── */}
                    <div
                      onClick={() => setSkipGroup2Collapsed(!skipGroup2Collapsed)}
                      style={{
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                        padding: "8px 12px",
                        backgroundColor: tokens.colorNeutralBackground3,
                        borderRadius: tokens.borderRadiusMedium,
                        cursor: "pointer",
                        userSelect: "none",
                        marginTop: tokens.spacingVerticalS,
                        borderLeft: `4px solid ${tokens.colorPalettePurpleBorderActive}`
                      }}
                    >
                      <Text weight="semibold" size={300} style={{ color: tokens.colorBrandForeground1 }}>
                        2. Active Patient Telemetry
                      </Text>
                      {skipGroup2Collapsed ? <ChevronDownRegular /> : <ChevronUpRegular />}
                    </div>
                    <div className={`deploy-collapsible-content ${skipGroup2Collapsed ? "deploy-collapsible-collapsed" : ""}`} style={{ display: "flex", flexDirection: "column", gap: tokens.spacingVerticalXS, padding: "8px 12px 12px", transition: "all 0.3s ease" }}>
                      <Tooltip content="Skip Masimo Eventhouse, KQL database/functions, Eventstream topology, core dashboard, and FHIR $export" relationship="description" positioning="after">
                        <Checkbox
                          label="Fabric RTI ingest (Eventhouse + Eventstream)"
                          checked={!config.skip_fabric}
                          onChange={(_, d) => update("skip_fabric", !d.checked)}
                        />
                      </Tooltip>
                      <div style={{ paddingLeft: 24 }}>
                        <Tooltip content="Skip FHIR $export to ADLS Gen2 — HDS pipelines need this data" relationship="description" positioning="after">
                          <Checkbox
                            label="FHIR $export to ADLS"
                            checked={!config.skip_fhir_export}
                            onChange={(_, d) => update("skip_fhir_export", !d.checked)}
                            disabled={config.skip_fabric || config.skip_fhir}
                          />
                        </Tooltip>
                      </div>
                      <Tooltip content="Skip RTI Phase 2 — KQL→Silver shortcuts and enriched alert functions" relationship="description" positioning="after">
                        <Checkbox
                          label="RTI Phase 2 (Shortcuts + Enrichment)"
                          checked={!config.skip_rti_phase2}
                          onChange={(_, d) => update("skip_rti_phase2", !d.checked)}
                          disabled={config.skip_fabric}
                        />
                      </Tooltip>
                    </div>

                    {/* ── Group 3: HDS Bridge, Semantic UX, and Analytics (Phases 3-6) ── */}
                    <div
                      onClick={() => setSkipGroup3Collapsed(!skipGroup3Collapsed)}
                      style={{
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                        padding: "8px 12px",
                        backgroundColor: tokens.colorNeutralBackground3,
                        borderRadius: tokens.borderRadiusMedium,
                        cursor: "pointer",
                        userSelect: "none",
                        marginTop: tokens.spacingVerticalS,
                        borderLeft: `4px solid ${tokens.colorPaletteTealBorderActive}`
                      }}
                    >
                      <Text weight="semibold" size={300} style={{ color: tokens.colorBrandForeground1 }}>
                        3. HDS Bridge, Semantic UX &amp; Analytics (Phases 3-6)
                      </Text>
                      {skipGroup3Collapsed ? <ChevronDownRegular /> : <ChevronUpRegular />}
                    </div>
                    <div className={`deploy-collapsible-content ${skipGroup3Collapsed ? "deploy-collapsible-collapsed" : ""}`} style={{ display: "flex", flexDirection: "column", gap: tokens.spacingVerticalXS, padding: "8px 12px 12px", transition: "all 0.3s ease" }}>
                      <Tooltip content="Skip DICOM shortcut creation, HDS clinical/imaging/OMOP pipeline triggers, and row-count gates" relationship="description" positioning="after">
                        <Checkbox
                          label="HDS Bridge + Row Gates"
                          checked={!config.skip_hds_pipelines}
                          onChange={(_, d) => update("skip_hds_pipelines", !d.checked)}
                          disabled={config.skip_dicom}
                        />
                      </Tooltip>
                      <Tooltip content="Skip Cohorting Agent, OHIF DICOM Viewer, PBI Imaging Report" relationship="description" positioning="after">
                        <Checkbox
                          label="Imaging Toolkit (Cohorting, Viewer, Report)"
                          checked={!config.skip_imaging}
                          onChange={(_, d) => update("skip_imaging", !d.checked)}
                          disabled={config.skip_dicom}
                        />
                      </Tooltip>
                      <Tooltip content="Skip ClinicalDeviceOntology (9 entities), DeviceAssociation table, agent binding" relationship="description" positioning="after">
                        <Checkbox
                          label="Ontology + Agent Binding"
                          checked={!config.skip_ontology}
                          onChange={(_, d) => update("skip_ontology", !d.checked)}
                        />
                      </Tooltip>
                      <Tooltip content="Skip Patient 360 + Clinical Triage Data Agents deployed after ontology" relationship="description" positioning="after">
                        <Checkbox
                          label="Ontology-aware Data Agents"
                          checked={!config.skip_data_agents}
                          onChange={(_, d) => update("skip_data_agents", !d.checked)}
                        />
                      </Tooltip>
                      <Tooltip content={`Skip Data Activator Reflex + email rule${!config.alert_email ? " (no alert email set)" : ""}`} relationship="description" positioning="after">
                        <Checkbox
                          label="Data Activator (Email Alerts)"
                          checked={!config.skip_activator}
                          onChange={(_, d) => update("skip_activator", !d.checked)}
                          disabled={config.skip_fabric || !config.alert_email}
                        />
                      </Tooltip>
                      <Tooltip content="Skip Population Health & Quality Dashboard — claims materialization, Star Ratings, HCC risk adjustment, readmission risk model, cost & utilization analytics, and Power BI report" relationship="description" positioning="after">
                        <Checkbox
                          label="Population Health & Quality Dashboard (10-page report)"
                          checked={!config.skip_quality_measures}
                          onChange={(_, d) => update("skip_quality_measures", !d.checked)}
                        />
                      </Tooltip>
                    </div>

                    {/* ── Group 4: Payer RTI & Ops (Phase 7) ── */}
                    <div
                      onClick={() => setSkipGroup4Collapsed(!skipGroup4Collapsed)}
                      style={{
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                        padding: "8px 12px",
                        backgroundColor: tokens.colorNeutralBackground3,
                        borderRadius: tokens.borderRadiusMedium,
                        cursor: "pointer",
                        userSelect: "none",
                        marginTop: tokens.spacingVerticalS,
                        borderLeft: `4px solid ${tokens.colorPaletteBerryBorderActive}`
                      }}
                    >
                      <Text weight="semibold" size={300} style={{ color: tokens.colorBrandForeground1 }}>
                        4. Payer RTI &amp; Ops (Phase 7)
                      </Text>
                      {skipGroup4Collapsed ? <ChevronDownRegular /> : <ChevronUpRegular />}
                    </div>
                    <div className={`deploy-collapsible-content ${skipGroup4Collapsed ? "deploy-collapsible-collapsed" : ""}`} style={{ display: "flex", flexDirection: "column", gap: tokens.spacingVerticalXS, padding: "8px 12px 12px", transition: "all 0.3s ease" }}>
                      <Tooltip content="Skip Phase 7 entirely" relationship="description" positioning="after">
                        <Checkbox
                          label="Payer RTI & Ops (Phase 7)"
                          checked={!config.skip_phase7}
                          onChange={(_, d) => update("skip_phase7", !d.checked)}
                          disabled={config.skip_fabric}
                        />
                      </Tooltip>
                      <div style={{ paddingLeft: 24, display: "flex", flexDirection: "column", gap: tokens.spacingVerticalXS }}>
                        <Tooltip content="Deploy claim-stream, payer KQL scoring, claim emulator, and Eventstream extension" relationship="description" positioning="after">
                          <Checkbox
                            label="Payer RTI scoring + claim stream"
                            checked={!config.skip_payer_rti}
                            onChange={(_, d) => update("skip_payer_rti", !d.checked)}
                            disabled={config.skip_phase7}
                          />
                        </Tooltip>
                        <Tooltip content="Deploy PayerOpsActivator Reflex email alerts" relationship="description" positioning="after">
                          <Checkbox
                            label="Payer Ops Activator"
                            checked={!config.skip_payer_activator}
                            onChange={(_, d) => update("skip_payer_activator", !d.checked)}
                            disabled={config.skip_phase7 || (!config.alert_email && !config.payer_ops_email)}
                          />
                        </Tooltip>
                        <Tooltip content="Deploy HealthcareOpsAgent and Payer Ops Triage" relationship="description" positioning="after">
                          <Checkbox
                            label="HealthcareOpsAgent + Payer Ops Triage"
                            checked={!config.skip_ops_agent}
                            onChange={(_, d) => update("skip_ops_agent", !d.checked)}
                            disabled={config.skip_phase7}
                          />
                        </Tooltip>
                        <Tooltip content="Deploy Healthcare Graph Agent shell and manual ontology attach instructions" relationship="description" positioning="after">
                          <Checkbox
                            label="Healthcare Graph Agent shell"
                            checked={!config.skip_graph_agent}
                            onChange={(_, d) => update("skip_graph_agent", !d.checked)}
                            disabled={config.skip_phase7}
                          />
                        </Tooltip>
                        <Field label="Payer Ops Email">
                          <HistoryInput
                            field="payer-ops-email"
                            type="email"
                            value={config.payer_ops_email}
                            onChange={(value) => update("payer_ops_email", value)}
                            placeholder={config.alert_email || "ops@example.com"}
                            disabled={config.skip_phase7 || config.skip_payer_activator}
                            suggestions={config.alert_email ? [config.alert_email] : []}
                            suggestionsLabel="Alert email"
                          />
                        </Field>
                        <Field label="Claim event rate/min">
                          <SpinButton
                            min={1}
                            max={1000}
                            value={config.claim_event_rate_per_minute}
                            onChange={(_, d) => update("claim_event_rate_per_minute", Number(d.value ?? d.displayValue ?? 60))}
                            disabled={config.skip_phase7 || config.skip_payer_rti}
                          />
                        </Field>
                      </div>
                    </div>
                  </div>
                </Card>
              </div>
            </div>

            {error && <div className={styles.error} ref={(el) => el?.scrollIntoView({ behavior: "smooth" })}>{error}</div>}
          </div>
        )}

        {/* Actions */}
        {validationErrors.length > 0 && !initializing && (
          <div style={{
            marginTop: tokens.spacingVerticalL,
            padding: `${tokens.spacingVerticalXS} ${tokens.spacingHorizontalM}`,
            backgroundColor: tokens.colorStatusWarningBackground1,
            borderLeft: `4px solid ${tokens.colorStatusWarningBorderActive}`,
            borderRadius: tokens.borderRadiusMedium,
          }}>
            <Text weight="semibold" size={200} block>Complete these required fields before deployment:</Text>
            {validationErrors.map((issue) => (
              <Text key={issue} size={200} block style={{ color: tokens.colorNeutralForeground2 }}>• {issue}</Text>
            ))}
          </div>
        )}

        {/* Advanced JSON Configuration Editor */}
        <div style={{ marginTop: tokens.spacingVerticalM, marginBottom: tokens.spacingVerticalM }}>
          <Button
            size="small"
            appearance="subtle"
            onClick={() => setShowJsonEditor(v => !v)}
            style={{ color: tokens.colorBrandForeground1, paddingLeft: 0 }}
          >
            {showJsonEditor ? "Hide Raw JSON Configuration" : "Advanced: View/Edit Raw JSON Configuration"}
          </Button>

          {showJsonEditor && (
            <div style={{ marginTop: tokens.spacingVerticalS }}>
              <Text size={100} style={{ color: tokens.colorNeutralForeground3, display: "block", marginBottom: tokens.spacingVerticalXXS }}>
                Directly edit parameters. Note: invalid JSON will prevent deployment.
              </Text>
              <textarea
                value={JSON.stringify(config, null, 2)}
                onChange={(e) => {
                  try {
                    const parsed = JSON.parse(e.target.value);
                    setConfig(parsed);
                  } catch (err) {
                    // Keep typing, don't crash on invalid JSON
                  }
                }}
                style={{
                  width: "100%",
                  height: "220px",
                  fontFamily: "'Cascadia Code', 'Consolas', monospace",
                  fontSize: tokens.fontSizeBase200,
                  backgroundColor: "#0a0a0a",
                  color: "#00f07f", // console green
                  border: `1px solid ${tokens.colorNeutralStroke1}`,
                  borderRadius: tokens.borderRadiusMedium,
                  padding: tokens.spacingHorizontalM,
                  boxShadow: "inset 0 0 10px rgba(0,0,0,0.5)",
                  outline: "none",
                  resize: "vertical"
                }}
              />
            </div>
          )}
        </div>
        <div className={styles.actions}>
          <Tooltip content="Preview the Azure and Fabric assets that this configuration will deploy" relationship="description">
            <Button
              appearance="outline"
              icon={<ClipboardRegular />}
              onClick={() => setShowResourcePreview(true)}
              disabled={validationErrors.length > 0}
            >
              Preview resources
            </Button>
          </Tooltip>
          <Tooltip content="Review the resource graph, then launch the full deployment pipeline" relationship="description">
            <Button
              appearance="primary"
              icon={<RocketRegular />}
              onClick={handleSubmit}
              disabled={!canStartDeployment}
            >
              {loading ? "Starting…" : "Start Deployment"}
            </Button>
          </Tooltip>
          <Tooltip content="Run a simulated deployment to preview the UI (no Azure/Fabric resources created)" relationship="description">
            <Button
              appearance="outline"
              icon={<BeakerRegular />}
              onClick={handleMockDeploy}
              size="small"
            >
              Mock Deploy
            </Button>
          </Tooltip>
        </div>
      </div>

      {/* Summary Sidebar (wide screens only) */}
      {showSummary && (
        <div
          className={styles.summarySidebar}
          style={{
            width: "280px",
          }}
        >
          <style>{`
            @media (max-width: 1199px) {
              .${styles.summarySidebar} {
                width: 100% !important;
                position: static !important;
                max-height: none !important;
              }
            }
          `}</style>
          <Card>
            <CardHeader
              header={<Subtitle1>Deployment Review</Subtitle1>}
              action={<Button size="small" appearance="subtle" icon={<ClipboardRegular />} onClick={copyDeploymentPlan}>Copy plan</Button>}
            />
            <div style={{ padding: "0 16px 16px", display: "flex", flexDirection: "column", gap: 12 }}>
              <div>
                <Text size={200} weight="semibold" block>Deployment Name</Text>
                <Text size={200} block style={{ color: tokens.colorNeutralForeground2 }}>
                  {namingPrefix || config.fabric_workspace_name || "<not set>"}
                </Text>
              </div>
              <div>
                <Text size={200} weight="semibold" block>Subscription</Text>
                <Text size={200} block style={{ color: tokens.colorNeutralForeground2 }}>
                  {subscriptions.find(s => s.id === selectedSubscription)?.name?.substring(0, 25) || "<not selected>"}
                </Text>
              </div>
              <div>
                <Text size={200} weight="semibold" block>Capacity</Text>
                <Text size={200} block style={{ color: tokens.colorNeutralForeground2 }}>
                  {selectedCapacity ? formatSelectedCapacityLabel(selectedCapacity) : "<not selected>"}
                </Text>
              </div>
              <div>
                <Text size={200} weight="semibold" block>Patient Count</Text>
                <Text size={200} block style={{ color: tokens.colorNeutralForeground2 }}>
                  {config.reuse_patients ? `${config.patient_count} (reusing)` : config.patient_count}
                </Text>
              </div>
              <div style={{ borderTop: `1px solid ${tokens.colorNeutralStroke2}`, paddingTop: 12, marginTop: 4 }}>
                <Text size={200} weight="semibold" block>Estimated Duration</Text>
                <Text size={300} weight="bold" block style={{ color: tokens.colorBrandForeground1, marginTop: 4 }}>
                  {getEstimatedDuration()}
                </Text>
              </div>
              <div>
                <Text size={200} weight="semibold" block>Risk / readiness</Text>
                <div style={{ display: "flex", flexDirection: "column", gap: 4, marginTop: 4 }}>
                  {validationErrors.length === 0 ? (
                    <Badge color="success">Required fields complete</Badge>
                  ) : (
                    <Badge color="warning">{validationErrors.length} required field(s) missing</Badge>
                  )}
                  {locationUnsupported && <Badge color="danger">Unsupported AHDS FHIR region</Badge>}
                  {config.patient_count > 500 && <Badge color="warning">Large patient load</Badge>}
                  {existingDeploy && !overridePriorSettings && <Badge color="informative">Existing deployment detected</Badge>}
                  {pauseAfterDeploy && <Badge color="informative">Capacity pause after deploy</Badge>}
                </div>
              </div>
              <div>
                <Text size={200} weight="semibold" block style={{ marginBottom: 4 }}>Components</Text>
                <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
                  {!config.skip_base_infra && <Text size={100}>✓ Infrastructure</Text>}
                  {!config.skip_fhir && <Text size={100}>✓ FHIR Service</Text>}
                  {!config.skip_dicom && <Text size={100}>✓ DICOM Loader</Text>}
                  {!config.skip_fabric && <Text size={100}>✓ Fabric RTI</Text>}
                  {!config.skip_hds_pipelines && <Text size={100}>✓ HDS Pipelines</Text>}
                  {!config.skip_data_agents && <Text size={100}>✓ Data Agents</Text>}
                  {!config.skip_imaging && <Text size={100}>✓ Imaging Toolkit</Text>}
                  {!config.skip_ontology && <Text size={100}>✓ Ontology</Text>}
                  {!config.skip_activator && config.alert_email && <Text size={100}>✓ Alerts</Text>}
                  {!config.skip_quality_measures && <Text size={100}>✓ Pop Health</Text>}
                </div>
              </div>

              <div style={{ borderTop: `1px solid ${tokens.colorNeutralStroke2}`, paddingTop: 12, marginTop: 4 }}>
                <Text size={200} weight="semibold" block style={{ marginBottom: 8 }}>Auto Export Options</Text>
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  <Checkbox
                    label="Auto Export Results to .XLSX"
                    checked={autoExportXlsx}
                    onChange={(_, d) => setAutoExportXlsx(!!d.checked)}
                  />
                  <Checkbox
                    label="Auto Export Results to .CSV"
                    checked={autoExportCsv}
                    onChange={(_, d) => setAutoExportCsv(!!d.checked)}
                  />
                </div>
              </div>

              {Object.keys(config.tags).length > 0 && (
                <div>
                  <Text size={200} weight="semibold" block>Tags</Text>
                  {Object.entries(config.tags).slice(0, 3).map(([k, v]) => (
                    <Text key={k} size={100} block style={{ color: tokens.colorNeutralForeground3 }}>
                      {k}: {v}
                    </Text>
                  ))}
                  {Object.keys(config.tags).length > 3 && (
                    <Text size={100} style={{ color: tokens.colorNeutralForeground3 }}>+{Object.keys(config.tags).length - 3} more</Text>
                  )}
                </div>
              )}
            </div>
          </Card>
        </div>
      )}

      <Dialog open={showResourcePreview} onOpenChange={(_, data) => setShowResourcePreview(data.open)}>
        <DialogSurface style={{ maxWidth: "1520px", width: "96vw" }}>
          <DialogBody>
            <DialogTitle>Prospective deployment resources</DialogTitle>
            <DialogContent>
              <Text block style={{ color: tokens.colorNeutralForeground2, marginBottom: tokens.spacingVerticalM }}>
                This preview is generated from the current wizard settings before anything is deployed. Names with <code>{uniqueSuffix}</code> are ARM/Bicep deterministic names based on the target resource group id.
              </Text>
              {loading && deploymentStartMessage && (
                <div role="status" aria-live="polite" style={{ display: "flex", alignItems: "center", gap: tokens.spacingHorizontalS, marginBottom: tokens.spacingVerticalM, padding: tokens.spacingHorizontalM, borderRadius: tokens.borderRadiusMedium, backgroundColor: tokens.colorBrandBackground2, border: `1px solid ${tokens.colorBrandStroke1}` }}>
                  <Spinner size="tiny" />
                  <Text size={200}>{deploymentStartMessage}</Text>
                </div>
              )}
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: tokens.spacingHorizontalM, marginBottom: tokens.spacingVerticalM, flexWrap: "wrap" }}>
                <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>
                  Based on the architecture docs: emulator → Event Hub → Eventstream/Eventhouse, FHIR/DICOM → ADLS → HDS Lakehouses, then agents/ontology/alerts/reports.
                </Text>
                <div style={{ display: "flex", gap: tokens.spacingHorizontalXS, alignItems: "center", flexWrap: "wrap" }}>
                  <Button size="small" appearance={resourcePreviewMode === "cards" ? "primary" : "secondary"} onClick={() => setResourcePreviewMode("cards")}>Resource boxes</Button>
                  <Button size="small" appearance={resourcePreviewMode === "graph" ? "primary" : "secondary"} onClick={() => setResourcePreviewMode("graph")}>Interconnection graph</Button>
                  {resourcePreviewMode === "graph" && (
                    <>
                      <Button size="small" appearance="subtle" onClick={() => setResourceGraphZoom((z) => Math.max(0.75, Math.round((z - 0.15) * 100) / 100))}>−</Button>
                      <Badge color="subtle">{Math.round(resourceGraphZoom * 100)}%</Badge>
                      <Button size="small" appearance="subtle" onClick={() => setResourceGraphZoom((z) => Math.min(2.25, Math.round((z + 0.15) * 100) / 100))}>+</Button>
                      <Button size="small" appearance="subtle" onClick={() => setResourceGraphZoom(1)}>Reset zoom</Button>
                      <Button size="small" appearance="subtle" onClick={resetGraphLayout}>Reset layout</Button>
                    </>
                  )}
                </div>
              </div>

              {resourcePreviewMode === "cards" ? (
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: tokens.spacingHorizontalL }}>
                  <div style={{ border: `2px solid ${tokens.colorPaletteBlueBorderActive}`, borderRadius: tokens.borderRadiusLarge, padding: tokens.spacingHorizontalL, backgroundColor: tokens.colorNeutralBackground2 }}>
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: tokens.spacingVerticalS }}>
                      <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                        <AzureIcon size={20} />
                        <Subtitle1>Azure resource group</Subtitle1>
                      </div>
                      <Badge color="informative">{prospectiveAzureAssets.length} assets</Badge>
                    </div>
                    <Text weight="semibold" block>{config.resource_group_name || "rg-<deployment>"}</Text>
                    <div style={{ display: "grid", gap: tokens.spacingVerticalXS, marginTop: tokens.spacingVerticalM }}>
                      {prospectiveAzureAssets.map((asset) => (
                        <div key={`${asset.type}-${asset.name}`} style={{ padding: tokens.spacingHorizontalS, borderRadius: tokens.borderRadiusMedium, backgroundColor: tokens.colorNeutralBackground1, border: `1px solid ${tokens.colorNeutralStroke2}` }}>
                          <Text size={100} block style={{ color: tokens.colorNeutralForeground3, textTransform: "uppercase" }}>{asset.type}</Text>
                          <Text size={200} weight="semibold" style={{ overflowWrap: "anywhere" }}>{asset.name}</Text>
                        </div>
                      ))}
                    </div>
                  </div>

                  <div style={{ border: `2px solid ${tokens.colorBrandStroke1}`, borderRadius: tokens.borderRadiusLarge, padding: tokens.spacingHorizontalL, backgroundColor: tokens.colorNeutralBackground2 }}>
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: tokens.spacingVerticalS }}>
                      <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                        <FabricIcon size={20} />
                        <Subtitle1>Fabric workspace</Subtitle1>
                      </div>
                      <Badge color="brand">{prospectiveFabricAssets.length} assets</Badge>
                    </div>
                    <Text weight="semibold" block>{config.fabric_workspace_name || "<workspace>"}</Text>
                    <div style={{ display: "grid", gap: tokens.spacingVerticalXS, marginTop: tokens.spacingVerticalM }}>
                      {prospectiveFabricAssets.map((asset) => (
                        <div key={`${asset.type}-${asset.name}`} style={{ padding: tokens.spacingHorizontalS, borderRadius: tokens.borderRadiusMedium, backgroundColor: tokens.colorNeutralBackground1, border: `1px solid ${tokens.colorNeutralStroke2}` }}>
                          <Text size={100} block style={{ color: tokens.colorNeutralForeground3, textTransform: "uppercase" }}>{asset.type}</Text>
                          <Text size={200} weight="semibold" style={{ overflowWrap: "anywhere" }}>{asset.name}</Text>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              ) : (
                <div style={{ display: "grid", gap: tokens.spacingVerticalXS }}>
                  <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>
                    Tip: drag resource boxes or arrow labels to separate overlaps, then zoom in for detailed reading. Arrows stay connected as boxes move.
                  </Text>
                  <div
                    ref={graphContainerRef}
                    onScroll={handleGraphScroll}
                    style={{ border: `1px solid ${tokens.colorNeutralStroke2}`, borderRadius: tokens.borderRadiusLarge, backgroundColor: tokens.colorNeutralBackground2, overflow: "auto", padding: tokens.spacingHorizontalS, maxHeight: "62vh", position: "relative" }}
                  >
                    <svg
                      viewBox={`0 0 ${GRAPH_WIDTH} ${GRAPH_HEIGHT}`}
                      width={GRAPH_WIDTH * resourceGraphZoom}
                      height={GRAPH_HEIGHT * resourceGraphZoom}
                      role="img"
                      aria-label="Prospective deployment interconnection graph"
                      style={{ display: "block", cursor: graphDrag ? "grabbing" : "default", touchAction: "none" }}
                      onPointerMove={updateGraphDrag}
                      onPointerUp={() => setGraphDrag(null)}
                      onPointerLeave={() => setGraphDrag(null)}
                    >
                      <defs>
                        <marker id="resource-preview-arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
                          <path d="M0,0 L0,6 L9,3 z" fill={tokens.colorNeutralForeground3} />
                        </marker>
                      </defs>
                      <rect x="300" y="20" width="490" height="620" rx="18" fill={tokens.colorPaletteBlueBackground2} opacity="0.28" />
                      <text x="320" y="48" fill={tokens.colorPaletteBlueForeground2} fontSize="18" fontWeight="700">Azure resource group: {config.resource_group_name || "rg-<deployment>"}</text>
                      <rect x="880" y="20" width="1070" height="620" rx="18" fill={tokens.colorBrandBackground2} opacity="0.35" />
                      <text x="900" y="48" fill={tokens.colorBrandForeground1} fontSize="18" fontWeight="700">Fabric workspace: {config.fabric_workspace_name || "<workspace>"}</text>

                      {/* Fabric Workspace Inner Sub-Group Lanes */}
                      <rect x="890" y="62" width="190" height="568" rx="14" fill={tokens.colorNeutralBackground1} stroke={tokens.colorNeutralStroke2} strokeWidth="1.25" opacity="0.18" />
                      <text x="905" y="82" fill={tokens.colorNeutralForeground3} fontSize="10" fontWeight="700" letterSpacing="0.8" style={{ userSelect: "none" }}>STREAMING &amp; KQL</text>

                      <rect x="1180" y="62" width="190" height="568" rx="14" fill={tokens.colorNeutralBackground1} stroke={tokens.colorNeutralStroke2} strokeWidth="1.25" opacity="0.18" />
                      <text x="1195" y="82" fill={tokens.colorNeutralForeground3} fontSize="10" fontWeight="700" letterSpacing="0.8" style={{ userSelect: "none" }}>DELTA LAKE</text>

                      <rect x="1380" y="62" width="560" height="568" rx="14" fill={tokens.colorNeutralBackground1} stroke={tokens.colorNeutralStroke2} strokeWidth="1.25" opacity="0.18" />
                      <text x="1395" y="82" fill={tokens.colorNeutralForeground3} fontSize="10" fontWeight="700" letterSpacing="0.8" style={{ userSelect: "none" }}>SEMANTIC &amp; APPLICATIONS</text>
                      {/* 1. Render all edge paths */}
                      {graphEdges.map((edge) => {
                        const { pathD } = getEdgeGeom(edge);
                        return (
                          <path
                            key={`path-${edge.id}`}
                            d={pathD}
                            stroke={tokens.colorNeutralForeground3}
                            strokeWidth="2.25"
                            fill="none"
                            markerEnd="url(#resource-preview-arrow)"
                            opacity="0.7"
                          />
                        );
                      })}

                      {/* 2. Render all node boxes */}
                      {positionedGraphNodes.map((node) => (
                        <g key={node.id} onPointerDown={(event) => startGraphNodeDrag(event, node.id)} style={{ cursor: "move" }}>
                          <rect x={node.x} y={node.y} width={NODE_WIDTH} height={NODE_HEIGHT} rx="14" fill={graphColor(node.group)} stroke={tokens.colorNeutralStroke1} strokeWidth="1.7" />
                          <foreignObject x={node.x + 10} y={node.y + 8} width={NODE_WIDTH - 20} height={NODE_HEIGHT - 14}>
                            <div style={{ fontSize: 13, lineHeight: "16px", fontWeight: 750, color: tokens.colorNeutralForeground1, textAlign: "center", overflow: "hidden", wordBreak: "break-word", userSelect: "none" }}>
                              {node.label.split("\n").map((part) => <div key={part}>{part}</div>)}
                            </div>
                          </foreignObject>
                        </g>
                      ))}

                      {/* 3. Render all edge labels on top of everything */}
                      {graphLabelVisible && graphEdges.map((edge) => {
                        const { midX, midY, labelWidth } = getEdgeGeom(edge);
                        return (
                          <g key={`label-${edge.id}`} onPointerDown={(event) => startGraphLabelDrag(event, edge.id)} style={{ cursor: "move" }}>
                            <rect x={midX - labelWidth / 2} y={midY - 14} width={labelWidth} height="28" rx="14" fill={tokens.colorNeutralBackground1} stroke={tokens.colorBrandStroke1} strokeWidth="1.25" opacity="0.98" />
                            <text x={midX} y={midY + 5} textAnchor="middle" fill={tokens.colorNeutralForeground1} fontSize="13" fontWeight="700" style={{ userSelect: "none" }}>{edge.label}</text>
                          </g>
                        );
                      })}
                    </svg>

                    {/* Floating Interconnection Mini-Map */}
                    {miniMapCollapsed ? (
                      <Button
                        size="small"
                        appearance="subtle"
                        onClick={() => setMiniMapCollapsed(false)}
                        style={{
                          position: "absolute",
                          bottom: 16,
                          right: 16,
                          zIndex: 10,
                          backgroundColor: tokens.colorNeutralBackground1,
                          border: `1px solid ${tokens.colorNeutralStroke2}`,
                          boxShadow: tokens.shadow16,
                          backdropFilter: "blur(8px)",
                          opacity: 0.95,
                          padding: "6px 10px",
                          display: "flex",
                          alignItems: "center",
                          gap: 4,
                          fontSize: 10,
                          fontWeight: 600,
                        }}
                      >
                        Show Mini-Map
                      </Button>
                    ) : (
                      <div style={{ position: "absolute", bottom: 16, right: 16, width: 200, height: 110, backgroundColor: tokens.colorNeutralBackground1, border: `1px solid ${tokens.colorNeutralStroke2}`, borderRadius: tokens.borderRadiusMedium, boxShadow: tokens.shadow16, padding: 6, display: "flex", flexDirection: "column", gap: 4, zIndex: 10, pointerEvents: "auto", backdropFilter: "blur(8px)", opacity: 0.95 }}>
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", width: "100%" }}>
                          <Text size={100} weight="semibold" style={{ color: tokens.colorNeutralForeground3, fontSize: 9, textTransform: "uppercase", letterSpacing: 0.5 }}>Interconnection Mini-Map</Text>
                          <Button
                            size="small"
                            appearance="subtle"
                            icon={<DismissRegular style={{ fontSize: 10 }} />}
                            onClick={() => setMiniMapCollapsed(true)}
                            style={{ minWidth: "auto", padding: 2, height: 16, width: 16 }}
                          />
                        </div>
                        <div style={{ flex: 1, position: "relative", border: `1px dashed ${tokens.colorNeutralStroke3}`, borderRadius: tokens.borderRadiusSmall, overflow: "hidden", backgroundColor: tokens.colorNeutralBackground2 }}>
                          <svg
                            viewBox={`0 0 ${GRAPH_WIDTH} ${GRAPH_HEIGHT}`}
                            style={{ width: "100%", height: "100%", cursor: "crosshair", touchAction: "none" }}
                            onPointerDown={handleMiniMapPointerDown}
                            onPointerMove={handleMiniMapPointerMove}
                            onPointerUp={handleMiniMapPointerUp}
                          >
                            <rect x="290" y="20" width="520" height="660" rx="35" fill={tokens.colorPaletteBlueBackground2} opacity="0.25" />
                            <rect x="820" y="20" width="1070" height="660" rx="35" fill={tokens.colorBrandBackground2} opacity="0.3" />
                            {graphEdges.map((edge) => {
                              const { pathD } = getEdgeGeom(edge);
                              return <path key={`mini-path-${edge.id}`} d={pathD} stroke={tokens.colorNeutralForeground3} strokeWidth="12" fill="none" opacity="0.45" />;
                            })}
                            {positionedGraphNodes.map((node) => (
                              <rect key={`mini-node-${node.id}`} x={node.x} y={node.y} width={NODE_WIDTH} height={NODE_HEIGHT} rx="25" fill={graphColor(node.group)} opacity="0.85" />
                            ))}
                            {/* Active Viewport Tracking Indicator */}
                            <rect
                              x={resourceGraphZoom > 0 ? scrollState.scrollLeft / resourceGraphZoom : 0}
                              y={resourceGraphZoom > 0 ? scrollState.scrollTop / resourceGraphZoom : 0}
                              width={Math.min(GRAPH_WIDTH, resourceGraphZoom > 0 ? scrollState.clientWidth / resourceGraphZoom : GRAPH_WIDTH)}
                              height={Math.min(GRAPH_HEIGHT, resourceGraphZoom > 0 ? scrollState.clientHeight / resourceGraphZoom : GRAPH_HEIGHT)}
                              fill="rgba(98, 100, 167, 0.1)"
                              stroke={tokens.colorBrandStroke1}
                              strokeWidth="20"
                              rx="18"
                              style={{ transition: "stroke 0.25s, fill 0.25s" }}
                            />
                          </svg>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </DialogContent>
            <DialogActions>
              {existingDeploy && (
                <Button appearance="subtle" onClick={runLiveExistingValidation} disabled={deepCheckingExisting}>
                  {deepCheckingExisting ? "Validating live state…" : "Run live validation"}
                </Button>
              )}
              <Button appearance="subtle" onClick={copyDeploymentPlan} disabled={loading}>Copy plan</Button>
              <Button appearance="secondary" onClick={() => setShowResourcePreview(false)} disabled={loading}>Cancel</Button>
              <Button appearance="primary" icon={<RocketRegular />} onClick={startActualDeployment} disabled={loading || validationErrors.length > 0}>
                {loading ? "Starting deployment…" : "Deploy these resources"}
              </Button>
            </DialogActions>
          </DialogBody>
        </DialogSurface>
      </Dialog>
    </div>
  );
}
