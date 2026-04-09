import { useEffect, useState, useCallback, useRef } from "react";
import { useNavigate } from "react-router-dom";
import {
  Badge,
  Button,
  Card,
  CardHeader,
  Checkbox,
  Dropdown,
  Field,
  Option,
  Subtitle1,
  Text,
  Title2,
  Tooltip,
  makeStyles,
  tokens,
} from "@fluentui/react-components";
import {
  DeleteRegular,
  ArrowSyncRegular,
  SearchRegular,
  ChevronDownRegular,
  ChevronUpRegular,
  LockClosedRegular,
  LockOpenRegular,
  LinkRegular,
} from "@fluentui/react-icons";
import { startTeardown, getDeploymentCapacity, type DeploymentCapacityMapping } from "../api";
import { useAppState } from "../AppState";
import { typeBadge } from "../components/TypeBadges";
import { MockDataBanner } from "../components/MockDataBanner";
import {
  getMockSubscriptions,
  scanForTeardownCandidates,
  startMockTeardown,
  type TeardownCandidate,
  type MockSubscription,
} from "../mockDeployment";

const useStyles = makeStyles({
  header: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: tokens.spacingVerticalL,
  },
  scanControls: {
    display: "flex",
    alignItems: "flex-end",
    gap: tokens.spacingHorizontalL,
    marginBottom: tokens.spacingVerticalL,
  },
  candidateList: {
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalS,
  },
  candidateCard: {
    transition: "box-shadow 0.2s ease, transform 0.15s ease",
    cursor: "pointer",
    ":hover": {
      boxShadow: tokens.shadow4,
      transform: "translateY(-1px)",
    },
  },
  candidateCardSelected: {
    border: `2px solid ${tokens.colorPaletteRedForeground1}`,
    boxShadow: tokens.shadow8,
  },
  candidateCardPaired: {
    borderLeft: `3px solid ${tokens.colorBrandStroke1}`,
    backgroundColor: tokens.colorBrandBackground2,
  },
  candidateRow: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalM,
    width: "100%",
  },
  candidateInfo: {
    flex: 1,
    display: "flex",
    flexDirection: "column",
    gap: tokens.spacingVerticalXXS,
  },
  candidateName: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalS,
  },
  artifactList: {
    padding: `${tokens.spacingVerticalS} ${tokens.spacingHorizontalL}`,
    backgroundColor: tokens.colorNeutralBackground3,
    borderTop: `1px solid ${tokens.colorNeutralStroke2}`,
    fontSize: tokens.fontSizeBase200,
    lineHeight: "1.6",
    fontFamily: "'Cascadia Code', 'Consolas', monospace",
    maxHeight: "200px",
    overflowY: "auto" as const,
  },
  actions: {
    display: "flex",
    gap: tokens.spacingHorizontalM,
    marginTop: tokens.spacingVerticalXL,
  },
  warning: {
    padding: tokens.spacingHorizontalM,
    backgroundColor: tokens.colorStatusDangerBackground1,
    borderLeft: `4px solid ${tokens.colorStatusDangerBorderActive}`,
    borderRadius: tokens.borderRadiusMedium,
    color: tokens.colorStatusDangerForeground1,
    fontSize: tokens.fontSizeBase300,
    fontWeight: tokens.fontWeightSemibold,
    marginBottom: tokens.spacingVerticalL,
  },
  error: {
    color: tokens.colorStatusDangerForeground1,
    fontSize: tokens.fontSizeBase200,
    marginTop: tokens.spacingVerticalS,
  },
  sectionTitle: {
    marginTop: tokens.spacingVerticalXL,
    marginBottom: tokens.spacingVerticalS,
  },
  sectionDesc: {
    color: tokens.colorNeutralForeground3,
    marginBottom: tokens.spacingVerticalS,
    display: "block" as const,
  },
  emptyState: {
    padding: tokens.spacingVerticalXL,
    textAlign: "center" as const,
    color: tokens.colorNeutralForeground3,
  },
  scanStatus: {
    marginBottom: tokens.spacingVerticalL,
    padding: `${tokens.spacingVerticalS} ${tokens.spacingHorizontalM}`,
    backgroundColor: tokens.colorNeutralBackground3,
    borderRadius: tokens.borderRadiusMedium,
    border: `1px solid ${tokens.colorNeutralStroke2}`,
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: tokens.spacingHorizontalL,
    flexWrap: "wrap" as const,
  },
  scanMeta: {
    display: "flex",
    alignItems: "center",
    gap: tokens.spacingHorizontalS,
    flexWrap: "wrap" as const,
  },
});

function statusBadge(status: string) {
  switch (status) {
    case "full":
      return <Badge color="success">Full Deploy</Badge>;
    case "partial":
      return <Badge color="warning">Partial</Badge>;
    case "orphaned":
      return <Badge color="danger">Orphaned</Badge>;
    case "active":
      return <Badge color="informative">Active</Badge>;
    default:
      return <Badge color="subtle">{status}</Badge>;
  }
}

export function TeardownView() {
  const styles = useStyles();
  const navigate = useNavigate();
  const scanPollRef = useRef<number | null>(null);
  const activeScanIdRef = useRef<string | null>(null);
  const hasInitializedRef = useRef(false);
  const normalizeResourceId = useCallback((id: string) => id.replace(/^\//, ""), []);
  const { selectedSubscription, setSelectedSubscription } = useAppState();
  const [loading, setLoading] = useState(false);
  const [scanning, setScanning] = useState(false);
  const [error, setError] = useState("");
  const [subscriptions, setSubscriptions] = useState<MockSubscription[]>(getMockSubscriptions());
  const [candidates, setCandidates] = useState<TeardownCandidate[]>([]);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [lockedIds, setLockedIds] = useState<Set<string>>(new Set());
  const [scanned, setScanned] = useState(false);
  const [usingMock, setUsingMock] = useState(false);
  const [showAllHDS, setShowAllHDS] = useState(false);
  const [capacityMappings, setCapacityMappings] = useState<Map<string, DeploymentCapacityMapping>>(new Map());
  const capacityFetchedRef = useRef<Set<string>>(new Set());
  const [scanPhase, setScanPhase] = useState("");
  const [scanMessage, setScanMessage] = useState("");
  const [scanCounts, setScanCounts] = useState({ fabric: 0, azure: 0, spn: 0 });

  // Load locks from backend on mount
  useEffect(() => {
    fetch("/api/locks")
      .then((r) => r.json())
      .then((ids: string[]) => {
        if (ids.length > 0) setLockedIds(new Set(ids.map(normalizeResourceId)));
      })
      .catch(() => {
        // Fall back to localStorage
        try {
          const saved = localStorage.getItem("teardown-locks");
          if (saved) setLockedIds(new Set(JSON.parse(saved).map((id: string) => normalizeResourceId(id))));
        } catch { /* ignore */ }
      });
  }, [normalizeResourceId]);

  // Persist locks to backend (and localStorage fallback) whenever they change
  const persistLocks = useCallback((ids: Set<string>, prevIds: Set<string>) => {
    const lockPath = (id: string) => `/api/locks/${normalizeResourceId(id)}`;
    // Find added and removed locks
    for (const id of ids) {
      if (!prevIds.has(id)) {
        fetch(lockPath(id), { method: "POST" }).catch(() => {});
      }
    }
    for (const id of prevIds) {
      if (!ids.has(id)) {
        fetch(lockPath(id), { method: "DELETE" }).catch(() => {});
      }
    }
    localStorage.setItem("teardown-locks", JSON.stringify([...ids]));
  }, [normalizeResourceId]);

  useEffect(() => {
    // Guard against React StrictMode double-mount
    if (hasInitializedRef.current) return;
    hasInitializedRef.current = true;

    // Fetch real subscriptions first, then scan with the correct default
    fetch("/api/scan/subscriptions")
      .then((r) => r.json())
      .then((subs: MockSubscription[]) => {
        if (subs.length > 0) {
          setSubscriptions(subs);
          // Set default subscription from API if none selected yet
          if (!selectedSubscription) {
            setSelectedSubscription(subs[0].id);
          }
        }
      })
      .catch(() => {}) // Fall back to mock subscriptions
      .finally(() => {
        handleScan();
      });
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    return () => {
      if (scanPollRef.current) {
        window.clearInterval(scanPollRef.current);
        scanPollRef.current = null;
      }
      activeScanIdRef.current = null;
    };
  }, []);

  useEffect(() => {
    const azureRgs = candidates.filter((candidate) => candidate.type === "azure");
    for (const rg of azureRgs) {
      if (capacityFetchedRef.current.has(rg.name)) {
        continue;
      }
      capacityFetchedRef.current.add(rg.name);
      getDeploymentCapacity(rg.name)
        .then((mapping) => {
          if (mapping) {
            setCapacityMappings((prev) => {
              const next = new Map(prev);
              next.set(rg.name, mapping);
              return next;
            });
          }
        })
        .catch(() => {});
    }
  }, [candidates]);

  const handleScan = () => {
    if (scanPollRef.current) {
      window.clearInterval(scanPollRef.current);
      scanPollRef.current = null;
    }
    // Generate a nonce for this scan call. If another handleScan fires (e.g. React
    // StrictMode double-mount), the earlier one will see a stale nonce and bail out.
    const scanNonce = Math.random().toString(36).slice(2);
    activeScanIdRef.current = scanNonce;

    setScanning(true);
    setScanned(false);
    setUsingMock(false);
    setError("");
    setSelectedIds(new Set()); // Clear selection on rescan
    setCandidates([]);
    setCapacityMappings(new Map());
    capacityFetchedRef.current = new Set();
    setScanPhase("starting");
    setScanMessage("Starting teardown scan...");
    setScanCounts({ fabric: 0, azure: 0, spn: 0 });

    fetch(`/api/scan/resources/start?subscription_id=${encodeURIComponent(selectedSubscription)}`, {
      method: "POST",
    })
      .then((r) => r.json())
      .then((data: { scanId: string }) => {
        // If this handleScan call was superseded, bail out
        if (activeScanIdRef.current !== scanNonce) return;

        let stopped = false;

        const stopPolling = () => {
          stopped = true;
          if (scanPollRef.current !== null) {
            window.clearInterval(scanPollRef.current);
            scanPollRef.current = null;
          }
        };

        const poll = () => {
          if (stopped || activeScanIdRef.current !== scanNonce) {
            stopPolling();
            return;
          }
          fetch(`/api/scan/resources/${encodeURIComponent(data.scanId)}`)
            .then((r) => r.json())
            .then((job) => {
              if (stopped || activeScanIdRef.current !== scanNonce) { stopPolling(); return; }
              setCandidates(job.candidates ?? []);
              setScanPhase(job.phase ?? "");
              setScanMessage(job.message ?? "");
              setScanCounts(job.counts ?? { fabric: 0, azure: 0, spn: 0 });

              if (job.status === "completed") {
                stopPolling();
                setScanned(true);
                setScanning(false);
              } else if (job.status === "missing") {
                stopPolling();
                setScanning(false);
                setScanned(false);
                setError("The last scan expired after a backend restart. Run the scan again.");
              } else if (job.status === "failed") {
                stopPolling();
                setScanning(false);
                setError(job.error || job.message || "Scan failed");
              }
            })
            .catch(() => {
              stopPolling();
              setScanning(false);
              setError("Scan polling failed. Try scanning again.");
            });
        };

        scanPollRef.current = window.setInterval(poll, 2000);
        poll(); // Initial poll after interval is stored so stopPolling() can clear it
      })
      .catch(() => {
        // Fall back to mock data if backend unavailable
        setCandidates(scanForTeardownCandidates(selectedSubscription));
        setScanned(true);
        setScanning(false);
        setUsingMock(true);
      });
  };

  const toggleSelected = (id: string) => {
    if (lockedIds.has(normalizeResourceId(id))) return;
    const candidate = candidates.find((c) => c.id === id);

    setSelectedIds((prev) => {
      const next = new Set(prev);
      const selecting = !next.has(id);

      if (selecting) {
        next.add(id);
        // Auto-select matching SPNs when a Fabric workspace is selected
        if (candidate?.type === "fabric") {
          const matchingSpns = candidates.filter(
            (c) => c.type === "spn" && c.name === candidate.name && !lockedIds.has(normalizeResourceId(c.id))
          );
          for (const spn of matchingSpns) {
            next.add(spn.id);
          }
        }
      } else {
        next.delete(id);
        // Auto-deselect matching SPNs when a Fabric workspace is deselected
        if (candidate?.type === "fabric") {
          const matchingSpns = candidates.filter(
            (c) => c.type === "spn" && c.name === candidate.name
          );
          for (const spn of matchingSpns) {
            next.delete(spn.id);
          }
        }
      }
      return next;
    });
  };

  const selectAll = () => {
    setSelectedIds(new Set(candidates.filter((c) => !lockedIds.has(normalizeResourceId(c.id))).map((c) => c.id)));
  };

  const deselectAll = () => {
    setSelectedIds(new Set());
  };

  const toggleLocked = (id: string) => {
    setLockedIds((prev) => {
      const next = new Set(prev);
      const candidate = candidates.find((c) => c.id === id);
      const normalizedId = normalizeResourceId(id);
      const locking = !next.has(normalizedId);

      if (locking) {
        next.add(normalizedId);
        setSelectedIds((sel) => {
          const nextSel = new Set(sel);
          nextSel.delete(id);
          return nextSel;
        });

        if (candidate?.type === "fabric") {
          const matchingSpns = candidates.filter(
            (c) => c.type === "spn" && c.name === candidate.name
          );
          for (const spn of matchingSpns) {
            next.add(normalizeResourceId(spn.id));
            setSelectedIds((sel) => {
              const nextSel = new Set(sel);
              nextSel.delete(spn.id);
              return nextSel;
            });
          }
        }
      } else {
        next.delete(normalizedId);

        if (candidate?.type === "fabric") {
          const matchingSpns = candidates.filter(
            (c) => c.type === "spn" && c.name === candidate.name
          );
          for (const spn of matchingSpns) {
            next.delete(normalizeResourceId(spn.id));
          }
        }
      }
      persistLocks(next, prev);
      return next;
    });
  };

  const unlocked = candidates.filter((c) => !lockedIds.has(normalizeResourceId(c.id)));
  const allSelected = unlocked.length > 0 && selectedIds.size === unlocked.length;
  const someSelected = selectedIds.size > 0 && selectedIds.size < unlocked.length;

  const toggleExpanded = (id: string) => {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const handleTeardown = async () => {
    if (selectedIds.size === 0) {
      setError("Select at least one resource to tear down.");
      return;
    }

    const selected = candidates.filter((c) => selectedIds.has(c.id));
    const names = selected.map((c) => `${c.type}: ${c.name}`).join("\n  ");
    if (!window.confirm(`Permanently delete ${selectedIds.size} resource(s)?\n\n  ${names}\n\nThis action cannot be undone.`)) return;

    setLoading(true);
    setError("");

    const fabricWs = selected.find((c) => c.type === "fabric");
    const azureRg = selected.find((c) => c.type === "azure");

    // Try real backend first
    try {
      await startTeardown({
        fabric_workspace_name: fabricWs?.name ?? "",
        resource_group_name: azureRg?.name ?? "",
        delete_workspace: !!fabricWs,
        delete_azure_rg: !!azureRg,
      });
      // Real backend accepted — navigate to deploy monitor to see real logs
      setLoading(false);
      // Use the history tab to track real teardowns (they appear as deployments)
      navigate("/history");
      return;
    } catch {
      // Backend unavailable — fall back to mock teardown
      for (const candidate of selected) {
        startMockTeardown(candidate);
      }
    }

    setLoading(false);
    navigate("/teardown/monitor");
  };

  const renderCandidate = (c: TeardownCandidate) => {
    const isSelected = selectedIds.has(c.id);
    const isExpanded = expandedIds.has(c.id);
    const isLocked = lockedIds.has(normalizeResourceId(c.id));
    const isPaired = pairedIds.has(c.id);
    const colorIdx = pairColorIndex.get(c.id) ?? 0;
    const pairColor = PAIR_COLORS[colorIdx % PAIR_COLORS.length];

    // For paired Azure RGs, look up the matching workspace name for the tooltip
    const pairedWorkspaceName = isPaired && c.type === "azure" && capacityMappings.has(c.name)
      ? capacityMappings.get(c.name)!.workspaceName
      : undefined;
    const pairedRgName = isPaired && c.type === "fabric"
      ? [...capacityMappings.entries()].find(([, m]) => m.workspaceName === c.name)?.[0]
      : undefined;

    return (
      <Card
        key={c.id}
        className={`${styles.candidateCard} ${isSelected ? styles.candidateCardSelected : ""} ${isPaired && !isSelected ? styles.candidateCardPaired : ""}`}
        size="small"
        style={{
          ...(isLocked ? { opacity: 0.6 } : {}),
          ...(isPaired && !isSelected
            ? {
                backgroundColor: pairColor.bg,
                borderLeft: `3px solid ${pairColor.border}`,
              }
            : {}),
        }}
      >
        <CardHeader
          header={
            <div className={styles.candidateRow}>
              <Checkbox
                checked={isSelected}
                onChange={() => toggleSelected(c.id)}
                disabled={isLocked}
              />
              <div className={styles.candidateInfo}>
                <div className={styles.candidateName}>
                  {typeBadge(c.type)}
                  <Text weight="semibold">{c.name}</Text>
                  {statusBadge(c.status)}
                  {c.previouslyDeployed && <Badge color="informative" size="small">Previously Deployed</Badge>}
                  {isLocked && <Badge color="subtle">Locked</Badge>}
                </div>
                <Text size={200} style={{ color: tokens.colorNeutralForeground3 }}>
                  {c.detail}
                </Text>
                {c.type === "fabric" && isPaired && pairedRgName && capacityMappings.has(pairedRgName) && (() => {
                  const m = capacityMappings.get(pairedRgName)!;
                  return (
                    <div style={{ display: "flex", gap: tokens.spacingHorizontalS, alignItems: "center", marginTop: 2 }}>
                      <Badge color="brand" size="small">Fabric Capacity</Badge>
                      <Text size={200}>{m.capacityName}</Text>
                    </div>
                  );
                })()}
                {c.resourceCount !== undefined && c.type === "azure" && (
                  <Text size={200}>
                    Resources discovered: {c.resourceCount}
                  </Text>
                )}
                {c.resourceCount !== undefined && c.type !== "azure" && (
                  <Text size={200}>
                    Resources: {c.resourceCount}/{c.expectedCount}
                  </Text>
                )}
              </div>
              {isPaired && (
                <Tooltip
                  content={pairedWorkspaceName
                    ? `Linked with Fabric workspace: ${pairedWorkspaceName}`
                    : pairedRgName
                      ? `Linked with Azure RG: ${pairedRgName}`
                      : "Linked deployment"}
                  relationship="label"
                >
                  <div style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "3px",
                    padding: `2px ${tokens.spacingHorizontalS}`,
                    borderRadius: tokens.borderRadiusMedium,
                    backgroundColor: pairColor.badge,
                    color: pairColor.badgeText,
                    fontSize: tokens.fontSizeBase100,
                    fontWeight: tokens.fontWeightSemibold,
                    cursor: "default",
                    whiteSpace: "nowrap" as const,
                  }}>
                    <LinkRegular style={{ fontSize: "11px" }} />
                    <span>{pairedWorkspaceName ?? pairedRgName ?? "Linked"}</span>
                  </div>
                </Tooltip>
              )}
              <Tooltip
                content={isLocked ? "Unlock to allow teardown" : "Lock to prevent accidental deletion"}
                relationship="label"
              >
                <Button
                  appearance="subtle"
                  icon={isLocked ? <LockClosedRegular /> : <LockOpenRegular />}
                  onClick={(e) => {
                    e.stopPropagation();
                    toggleLocked(c.id);
                  }}
                  size="small"
                  style={isLocked ? { color: tokens.colorPaletteRedForeground1 } : undefined}
                />
              </Tooltip>
              <Button
                appearance="subtle"
                icon={isExpanded ? <ChevronUpRegular /> : <ChevronDownRegular />}
                onClick={(e) => {
                  e.stopPropagation();
                  toggleExpanded(c.id);
                }}
                size="small"
              />
            </div>
          }
        />
        {isExpanded && c.matchedArtifacts && (
          <div className={styles.artifactList}>
            {c.matchedArtifacts.map((a, i) => (
              <div key={i}>• {a}</div>
            ))}
          </div>
        )}
      </Card>
    );
  };

  // Compute paired IDs: Azure RG ↔ Fabric workspace that share a name via capacity mapping
  // Assign each pair a unique color index for visual distinction
  const pairedIds = new Set<string>();
  const pairColorIndex = new Map<string, number>(); // candidate id → color index
  let pairIdx = 0;
  for (const [rgName, mapping] of capacityMappings) {
    if (mapping.workspaceName) {
      const rgCandidate = candidates.find((c) => c.type === "azure" && c.name === rgName);
      const wsCandidate = candidates.find((c) => c.type === "fabric" && c.name === mapping.workspaceName);
      if (rgCandidate && wsCandidate) {
        pairedIds.add(rgCandidate.id);
        pairedIds.add(wsCandidate.id);
        pairColorIndex.set(rgCandidate.id, pairIdx);
        pairColorIndex.set(wsCandidate.id, pairIdx);
        pairIdx++;
      }
    }
  }

  // Palette of distinct accent colors for paired deployments
  const PAIR_COLORS: Array<{ border: string; bg: string; badge: string; badgeText: string }> = [
    { border: tokens.colorBrandStroke1,                bg: tokens.colorBrandBackground2,                badge: tokens.colorBrandBackground,                badgeText: tokens.colorNeutralForegroundOnBrand },
    { border: tokens.colorPalettePurpleForeground2,    bg: tokens.colorPalettePurpleBackground2,        badge: tokens.colorPalettePurpleForeground2,        badgeText: "#fff" },
    { border: tokens.colorPaletteTealForeground2,      bg: tokens.colorPaletteTealBackground2,          badge: tokens.colorPaletteTealForeground2,          badgeText: "#fff" },
    { border: tokens.colorPaletteMarigoldForeground2,  bg: tokens.colorPaletteMarigoldBackground2,      badge: tokens.colorPaletteMarigoldForeground2,      badgeText: "#fff" },
    { border: tokens.colorPaletteBerryForeground2,     bg: tokens.colorPaletteBerryBackground2,         badge: tokens.colorPaletteBerryForeground2,         badgeText: "#fff" },
  ];

  const allFabricCandidates = candidates.filter((c) => c.type === "fabric");
  // Default: only show workspaces with all 3 criteria (qualified). When showAllHDS is on, also show partial HDS workspaces.
  const qualifiedFabricCandidates = allFabricCandidates.filter((c) => c.qualified !== false);
  const partialHdsCandidates = allFabricCandidates.filter((c) => c.qualified === false);
  const fabricCandidates = showAllHDS && !scanning ? allFabricCandidates : qualifiedFabricCandidates;
  const azureCandidates = candidates.filter((c) => c.type === "azure");

  // SPNs: only show SPNs whose workspace is qualified by default; show all when showAllHDS is on
  const qualifiedWsNames = new Set(qualifiedFabricCandidates.map((c) => c.name));
  const allSpnCandidates = candidates.filter((c) => c.type === "spn");
  const spnCandidates = showAllHDS && !scanning
    ? allSpnCandidates
    : allSpnCandidates.filter((c) => qualifiedWsNames.has(c.name));

  return (
    <div>
      {usingMock && <MockDataBanner />}
      <div className={styles.header}>
        <Title2>Teardown — Resource Scanner</Title2>
      </div>

      {/* Subscription selector + scan */}
      <div className={styles.scanControls}>
        <Field label="Azure Subscription" style={{ minWidth: 300 }}>
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
        <Button
          appearance="primary"
          icon={scanning ? <ArrowSyncRegular /> : <SearchRegular />}
          onClick={handleScan}
          disabled={scanning}
        >
          {scanning ? "Scanning…" : "Scan Resources"}
        </Button>
      </div>

      {scanning && (
        <div className={styles.scanStatus}>
          <div>
            <Text weight="semibold">{scanMessage || "Scanning resources..."}</Text>
            <Text size={200} style={{ color: tokens.colorNeutralForeground3, display: "block" }}>
              Fully-qualified candidates appear below as discovered. Partial matches will appear after the scan completes.
            </Text>
          </div>
          <div className={styles.scanMeta}>
            <Badge color="brand">{scanPhase || "starting"}</Badge>
            <Badge color="informative">Fabric {scanCounts.fabric}</Badge>
            <Badge color="informative">Azure {scanCounts.azure}</Badge>
            <Badge color="informative">Entra {scanCounts.spn}</Badge>
          </div>
        </div>
      )}

      {scanned && candidates.length > 0 && (
        <div style={{
          display: "flex",
          alignItems: "center",
          gap: tokens.spacingHorizontalM,
          marginBottom: tokens.spacingVerticalM,
          padding: `${tokens.spacingVerticalS} ${tokens.spacingHorizontalM}`,
          backgroundColor: tokens.colorNeutralBackground3,
          borderRadius: tokens.borderRadiusMedium,
        }}>
          <Checkbox
            checked={allSelected ? true : someSelected ? "mixed" : false}
            onChange={() => (allSelected ? deselectAll() : selectAll())}
            label={
              allSelected
                ? "Deselect all"
                : lockedIds.size > 0
                  ? `Select all unlocked (${unlocked.length} of ${candidates.length})`
                  : `Select all (${candidates.length} resources)`
            }
          />
        </div>
      )}

      {scanned && candidates.length === 0 && (
        <div className={styles.emptyState}>No matching deployment resources found.</div>
      )}

      {(fabricCandidates.length > 0 || partialHdsCandidates.length > 0) && (
        <>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginTop: tokens.spacingVerticalXL, marginBottom: tokens.spacingVerticalXXS }}>
            <Subtitle1 className={styles.sectionTitle} style={{ marginTop: 0, marginBottom: 0 }}>
              Fabric Workspaces ({fabricCandidates.length})
            </Subtitle1>
            {!scanning && partialHdsCandidates.length > 0 && (
              <Checkbox
                checked={showAllHDS}
                onChange={(_, d) => setShowAllHDS(!!d.checked)}
                label={`Show all HDS workspaces (${partialHdsCandidates.length} partial)`}
              />
            )}
          </div>
          <Text size={200} className={styles.sectionDesc}>
            Workspaces with HDS, MasimoEventhouse, and fn_ClinicalAlerts deployed
          </Text>
          <div className={styles.candidateList}>
            {fabricCandidates.map(renderCandidate)}
          </div>
          {fabricCandidates.length === 0 && (
            <div className={styles.emptyState} style={{ padding: tokens.spacingVerticalM }}>
              No fully-qualified workspaces found. {partialHdsCandidates.length > 0 ? "Enable \"Show all HDS workspaces\" to see partial deployments." : ""}
            </div>
          )}
        </>
      )}

      {azureCandidates.length > 0 && (
        <>
          <Subtitle1 className={styles.sectionTitle}>
            Azure Resource Groups ({azureCandidates.length})
          </Subtitle1>
          <Text size={200} className={styles.sectionDesc}>
            Resource groups with Event Hub, ACR, FHIR Service, and emulator resources (expected: 11)
          </Text>
          <div className={styles.candidateList}>
            {azureCandidates.map(renderCandidate)}
          </div>
        </>
      )}

      {spnCandidates.length > 0 && (
        <>
          <Subtitle1 className={styles.sectionTitle}>
            Workspace Identity SPNs ({spnCandidates.length}{!showAllHDS && allSpnCandidates.length > spnCandidates.length ? ` of ${allSpnCandidates.length}` : ""})
          </Subtitle1>
          <Text size={200} className={styles.sectionDesc}>
            App registrations matching workspace identity naming from prior deployments
            {!showAllHDS && allSpnCandidates.length > spnCandidates.length && (
              <> — {allSpnCandidates.length - spnCandidates.length} hidden (partial workspaces)</>
            )}
          </Text>
          <div className={styles.candidateList}>
            {spnCandidates.map(renderCandidate)}
          </div>
        </>
      )}

      {selectedIds.size > 0 && (() => {
        const selected = candidates.filter((c) => selectedIds.has(c.id));
        const hasFabric = selected.some((c) => c.type === "fabric");
        const hasAzure = selected.some((c) => c.type === "azure");
        const hasBoth = hasFabric && hasAzure;
        const mode = hasBoth
          ? "Teardown-All"
          : hasFabric
            ? "Fabric Teardown"
            : hasAzure
              ? "Azure Teardown"
              : "SPN Cleanup";

        return (
          <>
            <div className={styles.warning} style={{ marginTop: tokens.spacingVerticalXL }}>
              {selectedIds.size} resource(s) selected for deletion.
              {hasBoth && " Both Fabric workspace and Azure RG selected — will run Teardown-All (complete cleanup)."}
              {" "}This action cannot be undone.
            </div>
            <div className={styles.actions}>
              <Button
                appearance="primary"
                icon={<DeleteRegular />}
                onClick={handleTeardown}
                disabled={loading}
                style={{ backgroundColor: tokens.colorPaletteRedBackground3 }}
              >
                {loading ? "Starting teardown…" : `${mode}: Delete ${selectedIds.size} resource(s)`}
              </Button>
              <Button appearance="subtle" onClick={deselectAll}>
                Clear selection
              </Button>
            </div>
          </>
        );
      })()}

      {error && <div className={styles.error}>{error}</div>}
    </div>
  );
}
