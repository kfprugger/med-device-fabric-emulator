import React from "react";
import { Button, Card, CardHeader, Text, tokens } from "@fluentui/react-components";

interface State { error: Error | null }

export class RouteErrorBoundary extends React.Component<React.PropsWithChildren, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error) {
    // Keep this visible in dev tools without crashing the whole app shell.
    console.error("Route render failed", error);
  }

  render() {
    if (!this.state.error) return this.props.children;
    return (
      <Card style={{ margin: tokens.spacingVerticalXL }}>
        <CardHeader
          header={<Text weight="semibold">Page failed to render</Text>}
          description="The app shell is still running. Use the safe navigation below or reload after fixing the route issue."
        />
        <div style={{ display: "grid", gap: tokens.spacingVerticalS, padding: `0 ${tokens.spacingHorizontalL} ${tokens.spacingVerticalM}` }}>
          <Text size={200} style={{ color: tokens.colorStatusDangerForeground1 }}>{this.state.error.message}</Text>
          <div style={{ display: "flex", gap: tokens.spacingHorizontalS }}>
            <Button appearance="primary" onClick={() => window.location.assign("/history")}>Open History</Button>
            <Button onClick={() => window.location.reload()}>Reload</Button>
          </div>
        </div>
      </Card>
    );
  }
}
