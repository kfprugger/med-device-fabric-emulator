import { createContext, useContext, useEffect, useRef, useState, type ReactNode } from "react";
import { listCapacities, type FabricCapacity } from "./api";

interface Subscription {
  id: string;
  name: string;
}

interface BackgroundScanState {
  // Teardown resource scan — kicked off at app-mount so the Teardown tab
  // never has to wait for a fresh scan when the user navigates to it.
  scanId: string;
  status: "idle" | "running" | "completed" | "failed" | "missing";
  candidates: unknown[];
  counts: { fabric: number; azure: number; spn: number };
  startedAt: string | null;
  completedAt: string | null;
  error: string;
}

interface AppState {
  selectedSubscription: string;
  setSelectedSubscription: (id: string) => void;
  // Globally cached results from the background prefetch.
  // Consumers may still re-fetch when the user explicitly clicks Refresh.
  subscriptions: Subscription[];
  capacities: FabricCapacity[];
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
  error: "",
};

const AppStateContext = createContext<AppState>({
  selectedSubscription: "",
  setSelectedSubscription: () => {},
  subscriptions: [],
  capacities: [],
  teardownScan: defaultScan,
  refreshTeardownScan: () => {},
});

export function AppStateProvider({ children }: { children: ReactNode }) {
  const [selectedSubscription, setSelectedSubscription] = useState("");
  const [subscriptions, setSubscriptions] = useState<Subscription[]>([]);
  const [capacities, setCapacities] = useState<FabricCapacity[]>([]);
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
    stopPolling();
    setTeardownScan({
      ...defaultScan,
      status: "running",
      startedAt: new Date().toISOString(),
    });
    fetch(`/api/scan/resources/start?subscription_id=${encodeURIComponent(subscriptionId)}`, {
      method: "POST",
    })
      .then((r) => r.json())
      .then((data: { scanId: string }) => {
        activeScanIdRef.current = data.scanId;
        setTeardownScan((s) => ({ ...s, scanId: data.scanId }));

        const poll = () => {
          if (activeScanIdRef.current !== data.scanId) { stopPolling(); return; }
          fetch(`/api/scan/resources/${encodeURIComponent(data.scanId)}`)
            .then((r) => r.json())
            .then((job) => {
              if (activeScanIdRef.current !== data.scanId) return;
              setTeardownScan({
                scanId: data.scanId,
                status: job.status ?? "running",
                candidates: job.candidates ?? [],
                counts: job.counts ?? { fabric: 0, azure: 0, spn: 0 },
                startedAt: job.startedAt ?? null,
                completedAt: job.completedAt ?? null,
                error: job.error ?? "",
              });
              if (job.status === "completed" || job.status === "failed" || job.status === "missing") {
                stopPolling();
              }
            })
            .catch(() => { /* transient — let next tick retry */ });
        };
        pollTimerRef.current = window.setInterval(poll, 3000);
        poll();
      })
      .catch(() => {
        setTeardownScan((s) => ({ ...s, status: "failed", error: "Background scan unavailable" }));
      });
  };

  const refreshTeardownScan = (subscriptionId?: string) => {
    startTeardownScan(subscriptionId ?? selectedSubscription);
  };

  // Bootstrap on app mount: load subscriptions, capacities, and kick off
  // the teardown scan in the background so any page the user opens has data ready.
  useEffect(() => {
    if (hasBootstrappedRef.current) return;
    hasBootstrappedRef.current = true;

    fetch("/api/scan/subscriptions")
      .then((r) => r.json())
      .then((subs: Subscription[]) => {
        if (!Array.isArray(subs) || subs.length === 0) return;
        setSubscriptions(subs);
        const initialSub = selectedSubscription || subs[0].id;
        if (!selectedSubscription) setSelectedSubscription(subs[0].id);

        // Start Fabric capacity scan across all subscriptions in parallel
        Promise.all(subs.map((s) => listCapacities(s.id)))
          .then((results) => setCapacities(results.flat()))
          .catch(() => { /* non-fatal */ });

        // Start the teardown resource scan eagerly so the Teardown tab is
        // immediately populated if the user clicks on it.
        startTeardownScan(initialSub);
      })
      .catch(() => { /* backend unavailable — individual pages fall back to mocks */ });

    return () => stopPolling();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <AppStateContext.Provider
      value={{
        selectedSubscription,
        setSelectedSubscription,
        subscriptions,
        capacities,
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
