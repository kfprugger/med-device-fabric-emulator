import { createContext, useContext, useState, type ReactNode } from "react";

interface AppState {
  selectedSubscription: string;
  setSelectedSubscription: (id: string) => void;
}

const AppStateContext = createContext<AppState>({
  selectedSubscription: "",
  setSelectedSubscription: () => {},
});

export function AppStateProvider({ children }: { children: ReactNode }) {
  const [selectedSubscription, setSelectedSubscription] = useState("");

  return (
    <AppStateContext.Provider
      value={{ selectedSubscription, setSelectedSubscription }}
    >
      {children}
    </AppStateContext.Provider>
  );
}

export function useAppState() {
  return useContext(AppStateContext);
}
