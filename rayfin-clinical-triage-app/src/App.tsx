import { useCallback, useEffect, useState } from 'react';
import { ensureSignedInWithFabric, initEmbeddedAuth } from '@microsoft/rayfin-auth-provider-fabric';
import { fabricAuthOptions, rayfinClient } from './rayfinClient';
import type { AlertTriage } from '../rayfin/data/AlertTriage';

const ALERT_FIELDS = [
  'id',
  'patientId',
  'vitalsType',
  'severity',
  'status',
  'timestamp',
  'clinicianNotes',
] as const;

function App() {
  const [alerts, setAlerts] = useState<AlertTriage[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [authenticating, setAuthenticating] = useState(false);

  const fetchAlerts = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await rayfinClient.data.AlertTriage
        .select(ALERT_FIELDS)
        .orderBy({ timestamp: 'desc' })
        .execute();
      setAlerts(data as AlertTriage[]);
    } catch (err) {
      console.error('Failed to fetch alerts from Rayfin backend', err);
      setError('Sign in with Fabric to load clinical triage alerts.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const initialize = async () => {
      try {
        await initEmbeddedAuth(rayfinClient.auth, fabricAuthOptions);
      } catch (err) {
        console.warn('Embedded Fabric auth was not established', err);
      }
      await fetchAlerts();
    };
    initialize();
  }, [fetchAlerts]);

  const signInWithFabric = async () => {
    setAuthenticating(true);
    setError(null);
    try {
      await ensureSignedInWithFabric(rayfinClient.auth, fabricAuthOptions);
      await fetchAlerts();
    } catch (err) {
      console.error('Fabric sign-in failed', err);
      setError('Fabric sign-in failed. Open this app from the Fabric item or allow the popup sign-in flow.');
    } finally {
      setAuthenticating(false);
    }
  };

  const updateStatus = async (id: string, status: AlertTriage['status']) => {
    const previous = alerts;
    setAlerts(alerts.map((a) => (a.id === id ? { ...a, status } : a)));
    try {
      await rayfinClient.data.AlertTriage.update({ id }, { status });
    } catch (e) {
      console.error('Failed to update alert', e);
      setAlerts(previous); // rollback optimistic update
    }
  };

  if (loading) return <div style={{ padding: '2rem' }}>Loading alerts...</div>;

  const activeCount = alerts.filter((a) => a.status !== 'Resolved').length;

  return (
    <div className="dashboard-container">
      <header className="header">
        <h1>Clinical Alert Triage</h1>
        <div style={{ color: 'var(--text-secondary)' }}>Active Alerts: {activeCount}</div>
      </header>

      {error && (
        <div className="alert-banner" role="alert">
          <span>{error}</span>
          <button className="btn btn-primary" onClick={signInWithFabric} disabled={authenticating}>
            {authenticating ? 'Signing in…' : 'Sign in with Fabric'}
          </button>
        </div>
      )}

      <div className="alert-grid">
        {alerts.map((alert) => (
          <div key={alert.id} className={`alert-card status-${alert.status}`}>
            <div className="alert-header">
              <span className="alert-type">{alert.vitalsType}</span>
              <span className={`alert-severity severity-${alert.severity}`}>
                {alert.status === 'Resolved' ? 'Resolved' : alert.severity}
              </span>
            </div>

            <div className="alert-details">
              <div>
                <span>Patient ID:</span> {alert.patientId}
              </div>
              <div>
                <span>Status:</span> {alert.status}
              </div>
              <div>
                <span>Time:</span> {new Date(alert.timestamp).toLocaleTimeString()}
              </div>
            </div>

            <div className="alert-actions">
              {alert.status === 'Open' && (
                <button
                  className="btn btn-primary"
                  onClick={() => updateStatus(alert.id, 'Acknowledged')}
                >
                  Acknowledge
                </button>
              )}
              {alert.status !== 'Resolved' && (
                <button
                  className="btn btn-success"
                  onClick={() => updateStatus(alert.id, 'Resolved')}
                >
                  Resolve
                </button>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default App;
