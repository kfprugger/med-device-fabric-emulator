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
  'patientAlias',
  'alertTier',
  'locationName',
  'deviceId',
  'alertReason',
  'spo2',
  'pulseRate',
  'assignedTo',
  'escalationLevel',
  'disposition',
  'followUpDue',
] as const;

function toAlertDate(timestamp: Date | string) {
  return new Date(timestamp).toISOString().slice(0, 10);
}

function normalizeSeverity(alert: AlertTriage) {
  const rawTier = alert.alertTier || alert.vitalsType.split(' ')[0] || alert.severity;
  const tier = rawTier.toLowerCase();
  if (tier === 'critical') return 'Critical';
  if (tier === 'urgent') return 'Urgent';
  return 'Warning';
}

function fallbackPatientAlias(alert: AlertTriage) {
  if (alert.patientAlias) return alert.patientAlias;

  const noteName = alert.clinicianNotes?.split('|')[0]?.trim();
  if (noteName) {
    const [firstName, ...rest] = noteName.split(/\s+/);
    const lastName = rest.at(-1) ?? '';
    if (firstName && lastName) return `${firstName[0].toUpperCase()}. ${lastName.slice(0, 5)}`;
  }

  return `${alert.patientId.slice(0, 1).toUpperCase()}. ${alert.patientId.slice(1, 6)}`;
}

type StatusFilter = 'unacknowledged' | 'unresolved';
type AlertSeverity = 'Warning' | 'Urgent' | 'Critical';

const SEVERITY_FILTERS: AlertSeverity[] = ['Critical', 'Urgent', 'Warning'];

function App() {
  const [alerts, setAlerts] = useState<AlertTriage[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [authenticating, setAuthenticating] = useState(false);
  const [dateFilter, setDateFilter] = useState('');
  const [statusFilter, setStatusFilter] = useState<StatusFilter | null>(null);
  const [severityFilters, setSeverityFilters] = useState<AlertSeverity[]>([]);
  const [hospitalFilters, setHospitalFilters] = useState<string[]>([]);
  const [selectedAlertId, setSelectedAlertId] = useState<string | null>(null);
  const [lastSyncedAt, setLastSyncedAt] = useState<Date | null>(null);

  const fetchAlerts = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await rayfinClient.data.AlertTriage
        .select(ALERT_FIELDS)
        .orderBy({ timestamp: 'desc' })
        .execute();
      setAlerts(data as AlertTriage[]);
      setLastSyncedAt(new Date());
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

  useEffect(() => {
    const id = window.setInterval(() => {
      void fetchAlerts();
    }, 30000);

    return () => window.clearInterval(id);
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

  const updateAlert = async (id: string, changes: Partial<AlertTriage>) => {
    const previous = alerts;
    setAlerts(alerts.map((a) => (a.id === id ? { ...a, ...changes } : a)));
    try {
      await rayfinClient.data.AlertTriage.update({ id }, changes);
    } catch (e) {
      console.error('Failed to update alert', e);
      setAlerts(previous);
    }
  };

  const updateStatus = async (id: string, status: AlertTriage['status']) => {
    await updateAlert(id, { status });
  };

  const toggleStatusFilter = (filter: StatusFilter) => {
    setStatusFilter((current) => (current === filter ? null : filter));
  };

  const toggleSeverityFilter = (severity: AlertSeverity) => {
    setSeverityFilters((current) =>
      current.includes(severity)
        ? current.filter((item) => item !== severity)
        : [...current, severity],
    );
  };

  const toggleHospitalFilter = (hospital: string) => {
    setHospitalFilters((current) =>
      current.includes(hospital)
        ? current.filter((item) => item !== hospital)
        : [...current, hospital],
    );
  };

  if (loading) return <div style={{ padding: '2rem' }}>Loading alerts...</div>;

  const dateScopedAlerts = dateFilter
    ? alerts.filter((alert) => toAlertDate(alert.timestamp) === dateFilter)
    : alerts;
  const hospitalOptions = Array.from(
    new Set(dateScopedAlerts.map((alert) => alert.locationName).filter(Boolean) as string[]),
  ).sort();
  const hospitalScopedAlerts = hospitalFilters.length > 0
    ? dateScopedAlerts.filter((alert) => alert.locationName && hospitalFilters.includes(alert.locationName))
    : dateScopedAlerts;
  const severityScopedAlerts = severityFilters.length > 0
    ? hospitalScopedAlerts.filter((alert) => severityFilters.includes(normalizeSeverity(alert)))
    : hospitalScopedAlerts;
  const statusScopedAlerts = statusFilter
    ? hospitalScopedAlerts.filter((alert) => {
        if (statusFilter === 'unacknowledged') return alert.status === 'Open';
        return alert.status !== 'Resolved';
      })
    : hospitalScopedAlerts;
  const unacknowledgedCount = severityScopedAlerts.filter((a) => a.status === 'Open').length;
  const unresolvedCount = severityScopedAlerts.filter((a) => a.status !== 'Resolved').length;
  const filteredAlerts = severityScopedAlerts.filter((alert) => {
    if (statusFilter === 'unacknowledged') return alert.status === 'Open';
    if (statusFilter === 'unresolved') return alert.status !== 'Resolved';
    return true;
  });
  const selectedAlert = alerts.find((alert) => alert.id === selectedAlertId) ?? filteredAlerts[0] ?? null;
  const latestAlertTime = alerts.length > 0
    ? Math.max(...alerts.map((alert) => new Date(alert.timestamp).getTime()))
    : null;
  const dataAgeMinutes = latestAlertTime ? Math.round((Date.now() - latestAlertTime) / 60000) : null;
  const isDataStale = dataAgeMinutes !== null && dataAgeMinutes > 5;

  return (
    <div className="dashboard-container">
      <header className="header">
        <div>
          <h1>Clinical Alert Triage</h1>
          <div className="sync-strip">
            <span>Last synced: {lastSyncedAt ? lastSyncedAt.toLocaleTimeString() : 'Not synced'}</span>
            <span className={isDataStale ? 'stale' : 'fresh'}>
              Data age: {dataAgeMinutes === null ? 'unknown' : `${dataAgeMinutes}m`}
            </span>
            <button className="link-button" type="button" onClick={() => void fetchAlerts()}>
              Refresh now
            </button>
          </div>
          <div className="selector-bar" aria-label="Alert filters">
            <div className="selector-group" aria-label="Status filters">
              <span className="selector-label">Status</span>
              <button
                type="button"
                className={statusFilter === 'unacknowledged' ? 'selector-chip status-selector active' : 'selector-chip status-selector'}
                onClick={() => toggleStatusFilter('unacknowledged')}
                aria-pressed={statusFilter === 'unacknowledged'}
              >
                Unacknowledged: {unacknowledgedCount}
              </button>
              <button
                type="button"
                className={statusFilter === 'unresolved' ? 'selector-chip status-selector active' : 'selector-chip status-selector'}
                onClick={() => toggleStatusFilter('unresolved')}
                aria-pressed={statusFilter === 'unresolved'}
              >
                Unresolved: {unresolvedCount}
              </button>
              <button
                type="button"
                className={statusFilter === null ? 'selector-chip status-selector active' : 'selector-chip status-selector'}
                onClick={() => setStatusFilter(null)}
                aria-pressed={statusFilter === null}
              >
                All statuses: {severityScopedAlerts.length}
              </button>
            </div>

            <div className="selector-group" aria-label="Severity filters">
              <span className="selector-label">Severity</span>
              {SEVERITY_FILTERS.map((severity) => {
                const active = severityFilters.includes(severity);
                const count = statusScopedAlerts.filter((alert) => normalizeSeverity(alert) === severity).length;

                return (
                  <button
                    type="button"
                    key={severity}
                    className={active ? `selector-chip severity-selector severity-${severity} active` : `selector-chip severity-selector severity-${severity}`}
                    onClick={() => toggleSeverityFilter(severity)}
                    aria-pressed={active}
                  >
                    {severity}: {count}
                  </button>
                );
              })}
            </div>
          </div>
          <div className="selector-bar" aria-label="Hospital filters">
            <div className="selector-group hospital-selector-group">
              <span className="selector-label">Hospitals</span>
              {hospitalOptions.slice(0, 12).map((hospital) => (
                <button
                  type="button"
                  key={hospital}
                  className={hospitalFilters.includes(hospital) ? 'selector-chip hospital-selector active' : 'selector-chip hospital-selector'}
                  onClick={() => toggleHospitalFilter(hospital)}
                  aria-pressed={hospitalFilters.includes(hospital)}
                >
                  {hospital}
                </button>
              ))}
            </div>
          </div>
        </div>

        <label className="date-filter">
          Alert date
          <input
            type="date"
            value={dateFilter}
            onChange={(event) => setDateFilter(event.target.value)}
          />
        </label>
      </header>

      {error && (
        <div className="alert-banner" role="alert">
          <span>{error}</span>
          <button className="btn btn-primary" onClick={signInWithFabric} disabled={authenticating}>
            {authenticating ? 'Signing in…' : 'Sign in with Fabric'}
          </button>
        </div>
      )}

      <div className="triage-layout">
        <div className="alert-grid">
          {filteredAlerts.map((alert) => {
            const severity = normalizeSeverity(alert);

            return (
              <button
                type="button"
                key={alert.id}
                className={selectedAlert?.id === alert.id ? `alert-card selected status-${alert.status}` : `alert-card status-${alert.status}`}
                onClick={() => setSelectedAlertId(alert.id)}
              >
                <div className="alert-header">
                  <span className="alert-type">{alert.alertReason || alert.vitalsType}</span>
                  <span className={`alert-severity severity-${severity}`}>
                    {alert.status === 'Resolved' ? 'Resolved' : severity}
                  </span>
                </div>

                <div className="vitals-row">
                  <span>SpO₂ {alert.spo2 || '—'}</span>
                  <span>PR {alert.pulseRate || '—'}</span>
                </div>

                <div className="alert-details">
                  <div>
                    <span>Patient:</span> {fallbackPatientAlias(alert)}
                  </div>
                  <div>
                    <span>Location:</span> {alert.locationName || 'Unknown'}
                  </div>
                  <div>
                    <span>Status:</span> {alert.status}
                  </div>
                  <div>
                    <span>Time:</span> {new Date(alert.timestamp).toLocaleString()}
                  </div>
                </div>

                <div className="alert-actions">
                  {alert.status === 'Open' && (
                    <span className="btn btn-primary" onClick={(event) => { event.stopPropagation(); void updateStatus(alert.id, 'Acknowledged'); }}>
                      Acknowledge
                    </span>
                  )}
                  {alert.status !== 'Resolved' && (
                    <span className="btn btn-success" onClick={(event) => { event.stopPropagation(); void updateStatus(alert.id, 'Resolved'); }}>
                      Resolve
                    </span>
                  )}
                </div>
              </button>
            );
          })}
        </div>

        {selectedAlert && (
          <aside className="detail-panel" aria-label="Patient alert details">
            <div className="detail-header">
              <div>
                <span className="eyebrow">Patient drill-in</span>
                <h2>{fallbackPatientAlias(selectedAlert)}</h2>
              </div>
              <span className={`alert-severity severity-${normalizeSeverity(selectedAlert)}`}>
                {normalizeSeverity(selectedAlert)}
              </span>
            </div>

            <div className="detail-section">
              <h3>Alert reason</h3>
              <p>{selectedAlert.alertReason || selectedAlert.vitalsType}</p>
              <div className="vitals-row large">
                <span>SpO₂ {selectedAlert.spo2 || '—'}</span>
                <span>PR {selectedAlert.pulseRate || '—'}</span>
              </div>
            </div>

            <div className="detail-section detail-grid">
              <div><span>Hospital</span>{selectedAlert.locationName || 'Unknown'}</div>
              <div><span>Device</span>{selectedAlert.deviceId || 'Unknown'}</div>
              <div><span>Status</span>{selectedAlert.status}</div>
              <div><span>Last alert</span>{new Date(selectedAlert.timestamp).toLocaleString()}</div>
            </div>

            <div className="detail-section">
              <h3>Clinical context</h3>
              <p>{selectedAlert.clinicianNotes || 'No clinical rationale recorded.'}</p>
            </div>

            <div className="detail-section workflow-controls">
              <h3>Action workflow</h3>
              <label>
                Assigned to
                <input
                  value={selectedAlert.assignedTo || ''}
                  placeholder="Charge nurse"
                  onChange={(event) => void updateAlert(selectedAlert.id, { assignedTo: event.target.value })}
                />
              </label>
              <label>
                Escalation
                <select
                  value={selectedAlert.escalationLevel || ''}
                  onChange={(event) => void updateAlert(selectedAlert.id, { escalationLevel: event.target.value })}
                >
                  <option value="">None</option>
                  <option value="Nurse review">Nurse review</option>
                  <option value="Rapid response">Rapid response</option>
                  <option value="Physician escalation">Physician escalation</option>
                </select>
              </label>
              <label>
                Disposition
                <select
                  value={selectedAlert.disposition || ''}
                  onChange={(event) => void updateAlert(selectedAlert.id, { disposition: event.target.value })}
                >
                  <option value="">Pending</option>
                  <option value="Monitor">Monitor</option>
                  <option value="Escalated">Escalated</option>
                  <option value="False positive">False positive</option>
                  <option value="Resolved at bedside">Resolved at bedside</option>
                </select>
              </label>
              <label>
                Follow-up due
                <input
                  type="datetime-local"
                  value={selectedAlert.followUpDue ? new Date(selectedAlert.followUpDue).toISOString().slice(0, 16) : ''}
                  onChange={(event) => void updateAlert(selectedAlert.id, { followUpDue: event.target.value ? new Date(event.target.value) : undefined })}
                />
              </label>
              <label>
                Notes
                <textarea
                  value={selectedAlert.clinicianNotes || ''}
                  onChange={(event) => void updateAlert(selectedAlert.id, { clinicianNotes: event.target.value })}
                />
              </label>
            </div>
          </aside>
        )}
      </div>
    </div>
  );
}

export default App;
