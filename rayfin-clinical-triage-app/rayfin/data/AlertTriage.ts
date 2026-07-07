import { entity, authenticated, uuid, text, set, date } from '@microsoft/rayfin-core';

@entity('AlertTriage')
@authenticated('*') // any signed-in user can read/write (demo)
export class AlertTriage {
  @uuid() id!: string;

  @text({ max: 64 }) patientId!: string;

  @text({ max: 64 }) vitalsType!: string;

  @set('Warning', 'Critical')
  severity!: 'Warning' | 'Critical';

  @set('Open', 'Acknowledged', 'Resolved')
  status!: 'Open' | 'Acknowledged' | 'Resolved';

  @date() timestamp!: Date;

  @text({ max: 1000, optional: true }) clinicianNotes?: string;

  @text({ max: 32, optional: true }) patientAlias?: string;

  @text({ max: 16, optional: true }) alertTier?: string;

  @text({ max: 160, optional: true }) locationName?: string;

  @text({ max: 64, optional: true }) deviceId?: string;

  @text({ max: 64, optional: true }) alertReason?: string;

  @text({ max: 16, optional: true }) spo2?: string;

  @text({ max: 16, optional: true }) pulseRate?: string;

  @text({ max: 80, optional: true }) assignedTo?: string;

  @text({ max: 32, optional: true }) escalationLevel?: string;

  @text({ max: 64, optional: true }) disposition?: string;

  @date({ optional: true }) followUpDue?: Date;
}
