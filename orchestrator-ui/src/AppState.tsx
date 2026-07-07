import { createContext, useContext, useEffect, useRef, useState, type ReactNode } from "react";
import {
  getAuthContext,
  getLive,
  getResourceScan,
  listCapacities,
  listSubscriptions,
  startResourceScan,
  type AuthContext,
  type LiveStatus,
  type FabricCapacity,
  type Subscription,
} from "./api";

interface BackgroundScanState {
  // Teardown resource scan — kicked off at app-mount so the Teardown tab
  // never has to wait for a fresh scan when the user navigates to it.
  scanId: string;
  status: "idle" | "running" | "completed" | "failed" | "missing";
  candidates: unknown[];
  counts: { fabric: number; azure: number; spn: number };
  startedAt: string | null;
  completedAt: string | null;
  phase: string;
  message: string;
  error: string;
}

interface AppState {
  selectedSubscription: string;
  setSelectedSubscription: (id: string) => void;
  // Globally cached results from the background prefetch.
  // Consumers may still re-fetch when the user explicitly clicks Refresh.
  subscriptions: Subscription[];
  capacities: FabricCapacity[];
  authContext: AuthContext | null;
  authContextLoading: boolean;
  liveStatus: LiveStatus | null;
  liveStatusLoading: boolean;
  refreshAuthContext: () => Promise<void>;
  teardownScan: BackgroundScanState;
  // Force a new teardown scan (used by the Teardown page refresh button).
  refreshTeardownScan: (subscriptionId?: string) => void;
}

const defaultScan: BackgroundScanState = {
  scanId: "",
  status: "idle",
  candidates: [],
  counts: { fabric: 0, azure: 0, spn: 0 },
  startedAt: null,
  completedAt: null,
  phase: "",
  message: "",
  error: "",
};

const AppStateContext = createContext<AppState>({
  selectedSubscription: "",
  setSelectedSubscription: () => {},
  subscriptions: [],
  capacities: [],
  authContext: null,
  authContextLoading: true,
  liveStatus: null,
  liveStatusLoading: true,
  refreshAuthContext: async () => {},
  teardownScan: defaultScan,
  refreshTeardownScan: () => {},
});

export function AppStateProvider({ children }: { children: ReactNode }) {
  const [selectedSubscription, setSelectedSubscription] = useState("");
  const [subscriptions, setSubscriptions] = useState<Subscription[]>([]);
  const [capacities, setCapacities] = useState<FabricCapacity[]>([]);
  const [authContext, setAuthContext] = useState<AuthContext | null>(null);
  const [authContextLoading, setAuthContextLoading] = useState(true);
  const [liveStatus, setLiveStatus] = useState<LiveStatus | null>(null);
  const [liveStatusLoading, setLiveStatusLoading] = useState(true);
  const [teardownScan, setTeardownScan] = useState<BackgroundScanState>(defaultScan);
  const pollTimerRef = useRef<number | null>(null);
  const activeScanIdRef = useRef<string>("");
  const hasBootstrappedRef = useRef(false);

  const stopPolling = () => {
    if (pollTimerRef.current !== null) {
      window.clearInterval(pollTimerRef.current);
      pollTimerRef.current = null;
    }
  };

  const startTeardownScan = (subscriptionId: string) => {
    if (!subscriptionId) {
      setTeardownScan((s) => ({ ...s, status: "failed", error: "Select a subscription before scanning." }));
      return;
    }

    stopPolling();
    setTeardownScan({
      ...defaultScan,
      status: "running",
      startedAt: new Date().toISOString(),
    });

    startResourceScan(subscriptionId)
      .then((data) => {
        let transientFailures = 0;
        activeScanIdRef.current = data.scanId;
        setTeardownScan((s) => ({ ...s, scanId: data.scanId }));

        const poll = () => {
          if (activeScanIdRef.current !== data.scanId) { stopPolling(); return; }
          getResourceScan(data.scanId)
            .then((job) => {
              transientFailures = 0;
              if (activeScanIdRef.current !== data.scanId) return;
              const status = job.status ?? "running";
              setTeardownScan({
                scanId: data.scanId,
                status,
                candidates: job.candidates ?? [],
                counts: job.counts ?? { fabric: 0, azure: 0, spn: 0 },
                startedAt: job.startedAt ?? null,
                completedAt: job.completedAt ?? null,
                phase: job.phase ?? "",
                message: job.message ?? "",
                error: job.error ?? "",
              });
              if (status === "completed" || status === "failed" || status === "missing") {
                stopPolling();
              }
            })
            .catch((error) => {
              transientFailures += 1;
              if (transientFailures >= 5) {
                stopPolling();
                setTeardownScan((s) => ({
                  ...s,
                  status: "failed",
                  error: error instanceof Error ? error.message : "Background scan polling failed.",
                }));
              }
            });
        };
        pollTimerRef.current = window.setInterval(poll, 3000);
        poll();
      })
      .catch((error) => {
        setTeardownScan((s) => ({
          ...s,
          status: "failed",
          error: error instanceof Error ? error.message : "Background scan unavailable",
        }));
      });
  };

  const refreshTeardownScan = (subscriptionId?: string) => {
    startTeardownScan(subscriptionId ?? selectedSubscription);
  };

  const refreshAuthContext = async () => {
    setAuthContextLoading(true);
    setLiveStatusLoading(true);
    try {
      const [liveResult, context] = await Promise.all([getLive(), getAuthContext(true)]);
      setLiveStatus(liveResult);
      setAuthContext(context);
      const preferredSubscriptionId = context?.cli.subscriptionId || context?.pwsh.subscriptionId || "";
      if (preferredSubscriptionId && subscriptions.some((subscription) => subscription.id === preferredSubscriptionId)) {
        setSelectedSubscription(preferredSubscriptionId);
      }
    } finally {
      setLiveStatusLoading(false);
      setAuthContextLoading(false);
    }
  };

  // Bootstrap on app mount: load subscriptions, capacities, and kick off
  // the teardown scan in the background so any page the user opens has data ready.
  useEffect(() => {
    if (hasBootstrappedRef.current) return;
    hasBootstrappedRef.current = true;

    Promise.allSettled([getLive(), getAuthContext(), listSubscriptions()])
      .then(([liveResult, authResult, subsResult]) => {
        const live = liveResult.status === "fulfilled" ? liveResult.value : null;
        const context = authResult.status === "fulfilled" ? authResult.value : null;
        setLiveStatus(live);
        setLiveStatusLoading(false);
        setAuthContext(context);
        setAuthContextLoading(false);
        const subs = subsResult.status === "fulfilled" && Array.isArray(subsResult.value)
          ? subsResult.value
          : [];
        if (subs.length === 0) return;

        setSubscriptions(subs);
        const preferredSubscriptionId = context?.cli.subscriptionId || context?.pwsh.subscriptionId || "";
        const preferred = subs.find((subscription) => subscription.id === preferredSubscriptionId);
        const initialSub = selectedSubscription || preferred?.id || subs[0].id;
        if (!selectedSubscription) setSelectedSubscription(initialSub);

        // Start Fabric capacity scan across all accessible subscriptions.
        listCapacities()
          .then((results) => setCapacities(results))
          .catch(() => { /* non-fatal */ });

        // Teardown scans are intentionally lazy because they can enumerate
        // Fabric workspaces, KQL functions, Azure resources, and Entra SPNs.
        // The Teardown page starts the scan when the user opens it.
      })
      .catch(() => {
        setAuthContext(null);
        setLiveStatus(null);
        setLiveStatusLoading(false);
        setAuthContextLoading(false);
      });

    return () => stopPolling();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (selectedSubscription || subscriptions.length === 0) return;
    const preferredSubscriptionId =
      authContext?.cli.subscriptionId || authContext?.pwsh.subscriptionId || "";
    if (!preferredSubscriptionId) return;
    const match = subscriptions.find((subscription) => subscription.id === preferredSubscriptionId);
    if (match) {
      setSelectedSubscription(match.id);
    }
  }, [authContext, selectedSubscription, subscriptions]);

  return (
    <AppStateContext.Provider
      value={{
        selectedSubscription,
        setSelectedSubscription,
        subscriptions,
        capacities,
        authContext,
        authContextLoading,
        liveStatus,
        liveStatusLoading,
        refreshAuthContext,
        teardownScan,
        refreshTeardownScan,
      }}
    >
      {children}
    </AppStateContext.Provider>
  );
}

export function useAppState() {
  return useContext(AppStateContext);
}
