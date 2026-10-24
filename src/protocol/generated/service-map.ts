import type { AccountsEmptyParams, DaemonVersionParams, DsAccountAttemptParams, DsAccountBalanceResult, DsAccountBonus, DsAccountBonusAckParams, DsAccountModel, DsAccountSignInStartParams, DsAccountSignInStartResult, DsAccountSignInStatusResult, DsAccountStatusResult, DsAccountVerifyResult, FsEntry, FsListParams, FsReadParams, FsReadResult, SessionRenameParams, SiwcFlowParams, SiwcModel, SiwcProfileParams, SiwcProfileSelectParams, SiwcProfilesResult, SiwcSignInStartParams, SiwcSignInStartResult, SiwcSignInStatusResult, SiwcSignOutResult, SiwcStatusResult } from "./index";

export interface ServiceMap {
  "daemon.version": { params: DaemonVersionParams; result: string; };
  "session.rename": { params: SessionRenameParams; result: null; };
  "fs.list": { params: FsListParams; result: FsEntry[]; };
  "fs.read": { params: FsReadParams; result: FsReadResult; };
  "siwc.status": { params: AccountsEmptyParams; result: SiwcStatusResult; };
  "siwc.profiles": { params: AccountsEmptyParams; result: SiwcProfilesResult; };
  "siwc.models": { params: SiwcProfileParams; result: SiwcModel[]; };
  "siwc.sign_in.start": { params: SiwcSignInStartParams; result: SiwcSignInStartResult; };
  "siwc.sign_in.status": { params: SiwcFlowParams; result: SiwcSignInStatusResult; };
  "siwc.sign_in.cancel": { params: SiwcFlowParams; result: null; };
  "siwc.profile.select": { params: SiwcProfileSelectParams; result: SiwcStatusResult; };
  "siwc.sign_out": { params: SiwcProfileParams; result: SiwcSignOutResult; };
  "dsaccount.status": { params: AccountsEmptyParams; result: DsAccountStatusResult; };
  "dsaccount.balance": { params: AccountsEmptyParams; result: DsAccountBalanceResult; };
  "dsaccount.bonuses": { params: AccountsEmptyParams; result: DsAccountBonus[]; };
  "dsaccount.sign_in.start": { params: DsAccountSignInStartParams; result: DsAccountSignInStartResult; };
  "dsaccount.sign_in.status": { params: AccountsEmptyParams; result: DsAccountSignInStatusResult; };
  "dsaccount.sign_in.cancel": { params: DsAccountAttemptParams; result: null; };
  "dsaccount.sign_out": { params: AccountsEmptyParams; result: null; };
  "dsaccount.bonus.ack": { params: DsAccountBonusAckParams; result: null; };
  "dsaccount.models": { params: AccountsEmptyParams; result: DsAccountModel[]; };
  "dsaccount.verify": { params: AccountsEmptyParams; result: DsAccountVerifyResult; };
}

export const serviceMetadata = {
  "daemon.version": { kind: "read", requires_session: false, admin_only: false },
  "session.rename": { kind: "write", requires_session: true, admin_only: false },
  "fs.list": { kind: "read", requires_session: false, admin_only: false },
  "fs.read": { kind: "read", requires_session: false, admin_only: false },
  "siwc.status": { kind: "read", requires_session: false, admin_only: true },
  "siwc.profiles": { kind: "read", requires_session: false, admin_only: true },
  "siwc.models": { kind: "read", requires_session: false, admin_only: true },
  "siwc.sign_in.start": { kind: "write", requires_session: false, admin_only: true },
  "siwc.sign_in.status": { kind: "read", requires_session: false, admin_only: true },
  "siwc.sign_in.cancel": { kind: "write", requires_session: false, admin_only: true },
  "siwc.profile.select": { kind: "write", requires_session: false, admin_only: true },
  "siwc.sign_out": { kind: "write", requires_session: false, admin_only: true },
  "dsaccount.status": { kind: "read", requires_session: false, admin_only: true },
  "dsaccount.balance": { kind: "read", requires_session: false, admin_only: true },
  "dsaccount.bonuses": { kind: "read", requires_session: false, admin_only: true },
  "dsaccount.sign_in.start": { kind: "write", requires_session: false, admin_only: true },
  "dsaccount.sign_in.status": { kind: "read", requires_session: false, admin_only: true },
  "dsaccount.sign_in.cancel": { kind: "write", requires_session: false, admin_only: true },
  "dsaccount.sign_out": { kind: "write", requires_session: false, admin_only: true },
  "dsaccount.bonus.ack": { kind: "write", requires_session: false, admin_only: true },
  "dsaccount.models": { kind: "read", requires_session: false, admin_only: true },
  "dsaccount.verify": { kind: "write", requires_session: false, admin_only: true },
} as const;
