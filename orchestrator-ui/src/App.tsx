import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { Layout } from "./components/Layout";
import { DeployWizard } from "./pages/DeployWizard";
import { PhaseMonitor } from "./pages/PhaseMonitor";
import { DeploymentHistory } from "./pages/DeploymentHistory";
import { TeardownView } from "./pages/TeardownView";
import { TeardownMonitor } from "./pages/TeardownMonitor";
import { AppStateProvider } from "./AppState";

export function App() {
  return (
    <AppStateProvider>
      <BrowserRouter>
        <Routes>
          <Route element={<Layout />}>
            <Route path="/" element={<Navigate to="/deploy" replace />} />
            <Route path="/deploy" element={<DeployWizard />} />
            <Route path="/monitor/:instanceId" element={<PhaseMonitor />} />
            <Route path="/history" element={<DeploymentHistory />} />
            <Route path="/teardown" element={<TeardownView />} />
            <Route path="/teardown/monitor" element={<TeardownMonitor />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </AppStateProvider>
  );
}
