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
}
