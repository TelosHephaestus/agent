/** Published once a session replacement has settled; no business request is retried. */
export const SESSION_RECOVERY_SETTLED = 'session-recovery-settled';

/** Carries the sanitized exception itself: public connection snapshots erase its prototype. */
export interface SessionRecoverySettlement {
  readonly error?: Error;
}
