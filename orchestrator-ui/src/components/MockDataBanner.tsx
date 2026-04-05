import { MessageBar, MessageBarBody } from "@fluentui/react-components";

export function MockDataBanner() {
  return (
    <MessageBar intent="warning" style={{ marginBottom: 12 }}>
      <MessageBarBody>
        <strong>Mock data in use</strong> — The backend is unavailable. Displaying simulated data.
      </MessageBarBody>
    </MessageBar>
  );
}
