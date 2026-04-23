# backend/lib/subscription/service.py
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.dal import (
    DALEntitlements,
    DALSubscriptionEvents,
    DAOEntitlements,
    DAOEntitlementsCreate,
    DAOEntitlementsUpdate,
    DAOSubscriptionEventsCreate,
    FilterOp,
    OrderDirection,
    safe_transaction,
)
from backend.db.data_models import (
    SubscriptionEventSource,
    SubscriptionStatus,
)
from backend.lib.subscription.revenuecat.client import (
    RCClient,
    RCGetSubscriberResponse,
    RCSubscriber,
    RCSubscription,
)
from backend.lib.utils.common import none_throws

# Tunables
_EAGER_SKIP_RC_EVENT_FRESH_S = 45
_EAGER_SKIP_ENT_UPDATED_FRESH_S = 30
_ACTIVE_EVENT_TYPES = {
    "INITIAL_PURCHASE",
    "RENEWAL",
    "UNCANCELLATION",
    "PRODUCT_CHANGE",
}

# --------- Helpers & types ---------

UTC = timezone.utc
AWARE_MIN = datetime(1900, 1, 1, tzinfo=UTC)  # safe sentinel far in the past


@dataclass(frozen=True)
class CanonicalEntitlementStatus:
    active: bool
    will_auto_renew_on_expiration: Optional[bool]
    entitlement: Optional[DAOEntitlements]


def _ensure_aware_utc_optional(dt: datetime | None) -> datetime | None:
    """
    Normalize any datetime to tz-aware UTC.
    - None -> None
    - Naive -> assume UTC (common for legacy rows) and set tzinfo=UTC
    - Aware (any tz) -> convert to UTC
    """
    if dt is None:
        return None
    return _ensure_aware_utc(dt)


def _ensure_aware_utc(dt: datetime) -> datetime:
    """
    Normalize any datetime to tz-aware UTC.
    - None -> None
    - Naive -> assume UTC (common for legacy rows) and set tzinfo=UTC
    - Aware (any tz) -> convert to UTC
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    # Convert any non-UTC to UTC (handles rare cases)
    return dt.astimezone(UTC)


def _aware_or_min(dt: Optional[datetime]) -> datetime:
    return _ensure_aware_utc_optional(dt) or AWARE_MIN


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _ms_to_dt_optional(ms: Optional[int]) -> Optional[datetime]:
    if ms is None:
        return None
    return _ms_to_dt(ms)


@dataclass(frozen=True)
class StatusApplyContext:
    """
    How we will audit the attempt to reconcile/apply RC state.
    """

    source: SubscriptionEventSource
    rc_event_type: str
    rc_event_id: Optional[str] = None
    signature_verified: Optional[bool] = None
    extra_payload: dict[str, Any] = field(default_factory=dict[str, Any])


@dataclass(frozen=True)
class RCSubscriptionSnapshot:
    """
    Parsed, normalized view over RC webhook 'event'.
    """

    user_id: UUID
    product_id: str
    entitlement_id: Optional[str]
    event_type: str  # e.g. INITIAL_PURCHASE, RENEWAL, EXPIRATION
    event_id: Optional[str]
    event_timestamp_ms: Optional[int]
    purchased_at: Optional[datetime]
    expires_at: Optional[datetime]
    cancel_reason: Optional[str]


# --- Parsing & mapping ---------------------------------------------------------


def parse_rc_payload(
    payload: dict[str, Any],
) -> tuple[RCSubscriptionSnapshot, dict[str, Any]]:
    event: dict[str, Any] = (payload or {}).get("event", {})  # keep raw too

    # entitlement_ids (array) preferred over deprecated entitlement_id
    ent_ids: Optional[list[str]] = event.get("entitlement_ids")
    entitlement_id: Optional[str] = (
        ent_ids[0] if isinstance(ent_ids, list) and ent_ids else None
    )

    app_user_id = event.get("app_user_id")
    user_uuid = UUID(str(app_user_id))  # let caller catch if this fails

    snap = RCSubscriptionSnapshot(
        user_id=user_uuid,
        product_id=none_throws(event.get("product_id")),
        entitlement_id=entitlement_id,
        event_type=(event.get("type") or "").upper(),
        event_id=event.get("id"),
        event_timestamp_ms=event.get("event_timestamp_ms"),
        purchased_at=_ms_to_dt_optional(event.get("purchased_at_ms")),
        expires_at=_ms_to_dt_optional(event.get("expiration_at_ms")),
        cancel_reason=event.get("cancel_reason"),
    )
    return snap, event


def map_rc_event_to_status(
    event_type: str, cancel_reason: Optional[str]
) -> SubscriptionStatus:
    et = (event_type or "").upper()
    if et in {"INITIAL_PURCHASE", "RENEWAL", "UNCANCELLATION", "PRODUCT_CHANGE"}:
        return SubscriptionStatus.ACTIVE
    if et == "BILLING_ISSUE":
        return SubscriptionStatus.BILLING_ISSUE
    if et == "CANCELLATION":
        # status is "cancelled" but access may last until expiration; we still reflect the lifecycle state
        return SubscriptionStatus.CANCELLED
    if et == "EXPIRATION":
        return SubscriptionStatus.EXPIRED
    # conservative default based on eventual access
    return SubscriptionStatus.ACTIVE


# --- FSM / advancement rules ---------------------------------------------------


@dataclass(frozen=True)
class ProposedEntitlement:
    active: bool
    expires_at: Optional[datetime]
    product_id: Optional[str]
    entitlement_id: Optional[str]
    applied_status: SubscriptionStatus


def _propose_entitlement_from_rc(snap: RCSubscriptionSnapshot) -> ProposedEntitlement:
    now: datetime = _ensure_aware_utc(_utcnow())
    exp: datetime | None = _ensure_aware_utc_optional(snap.expires_at)
    et = snap.event_type
    status = map_rc_event_to_status(et, snap.cancel_reason)

    if et in {"INITIAL_PURCHASE", "RENEWAL", "UNCANCELLATION", "PRODUCT_CHANGE"}:
        return ProposedEntitlement(
            active=True,
            expires_at=exp,
            product_id=snap.product_id,
            entitlement_id=snap.entitlement_id,
            applied_status=status,
        )

    if et in {"BILLING_ISSUE", "CANCELLATION"}:
        return ProposedEntitlement(
            active=bool(exp and exp > now),
            expires_at=exp,
            product_id=snap.product_id,
            entitlement_id=snap.entitlement_id,
            applied_status=status,
        )

    if et == "EXPIRATION":
        return ProposedEntitlement(
            active=False,
            expires_at=exp,
            product_id=snap.product_id,
            entitlement_id=snap.entitlement_id,
            applied_status=status,
        )

    return ProposedEntitlement(
        active=bool(exp and exp > now),
        expires_at=exp,
        product_id=snap.product_id,
        entitlement_id=snap.entitlement_id,
        applied_status=status,
    )


# Precedence ranking to prevent regressions from out-of-order events.
_EVENT_PRECEDENCE: dict[str, int] = {
    # higher means stronger / more authoritative for current access window
    "RENEWAL": 100,
    "INITIAL_PURCHASE": 90,
    "UNCANCELLATION": 85,
    "PRODUCT_CHANGE": 80,
    "BILLING_ISSUE": 60,
    "CANCELLATION": 50,
    "EXPIRATION": 10,
}


def _precedence(evt_type: str) -> int:
    return _EVENT_PRECEDENCE.get(evt_type.upper(), 1)


def _should_advance_entitlement(
    current: Optional[DAOEntitlements],
    proposed: ProposedEntitlement,
    snap: RCSubscriptionSnapshot,
) -> bool:
    # ... (docstring unchanged)

    if current is None:
        return True

    cur_active = bool(current.active)

    # Normalize all datetimes we compare
    cur_expires = _ensure_aware_utc_optional(current.expires_at)
    prop_expires = _ensure_aware_utc_optional(proposed.expires_at)
    cur_updated = _ensure_aware_utc_optional(getattr(current, "updated_at", None))

    # 1) Prefer later expiration
    if (prop_expires or cur_expires) and _aware_or_min(prop_expires) > _aware_or_min(
        cur_expires
    ):
        return True

    # 2) Prefer ACTIVE if proposed isn’t expired
    if not cur_active and proposed.active:
        return True

    # 3) Event precedence (don’t shorten an already-later window)
    prop_prec = _precedence(snap.event_type)
    cur_prec = 0
    if prop_prec > cur_prec:
        if cur_expires and prop_expires and prop_expires < cur_expires and cur_active:
            return False
        return True

    # 4) Tiebreaker: event timestamp monotonicity vs current.updated_at (if present)
    if snap.event_timestamp_ms is not None:
        evt_ts = _ensure_aware_utc(_ms_to_dt(snap.event_timestamp_ms))
        # Only advance if we have no updated_at or this event is newer
        if evt_ts and (cur_updated is None or evt_ts > cur_updated):
            return True

    return False


def _compute_will_auto_renew(
    event_type: Optional[str],
) -> bool | None:
    et = (event_type or "").upper()
    if et in {"INITIAL_PURCHASE", "RENEWAL", "UNCANCELLATION"}:
        return True
    if et == "PRODUCT_CHANGE":
        # Immediate upgrade -> True; scheduled downgrade still auto-renews (to different product) → True.
        return True
    if et == "CANCELLATION":
        return False
    if et == "BILLING_ISSUE":
        # RC may also emit CANCELLATION on failed renewal. Be conservative unless we *know* otherwise.
        # Since we only see one event at a time here, return None (unknown) — the paired CANCELLATION event
        # will mark False when it arrives.
        # If you pre-aggregate batch payloads, prefer False when a paired CANCELLATION is present.
        return None
    if et == "EXPIRATION":
        return False
    # Fallback: if we still have time left, assume the plan would renew unless told otherwise? Be conservative.
    return None


# --- Public entrypoint (mirrors Stripe's reconcile_and_apply_* pattern) --------


async def reconcile_and_apply_subscription_status_in_txn(
    session: AsyncSession,
    *,
    user_id: UUID,
    snap: RCSubscriptionSnapshot,
    ctx: StatusApplyContext,
) -> Tuple[DAOEntitlements, SubscriptionStatus, bool]:
    """
    In one DB transaction:
      - select/create entitlement row for user
      - compute proposed snapshot from RC event
      - if advancement rules say yes, update entitlement (idempotent)
      - write a single audit event row recording applied_status and raw payload
    Returns: (fresh entitlement row, applied_status, did_advance)
    """
    if not session.in_transaction():
        raise RuntimeError(
            "[reconcile_and_apply_subscription_status_in_txn] must run inside an active transaction"
        )

    # Load current entitlement row; we keep a single row per user for "pro" (or first entitlement).
    ent_rows = await DALEntitlements.list_all(
        session,
        filters={"user_id": (FilterOp.EQ, user_id)},
        limit=1,
    )
    current = ent_rows[0] if ent_rows else None

    proposed = _propose_entitlement_from_rc(snap)
    applied_status = proposed.applied_status

    did_advance = _should_advance_entitlement(current, proposed, snap)

    if current is None:
        # create minimal row if missing, respecting proposal
        current = await DALEntitlements.create(
            session,
            DAOEntitlementsCreate(
                user_id=user_id,
                product_id=proposed.product_id or "",
                entitlement_id=proposed.entitlement_id,
                active=proposed.active,
                expires_at=proposed.expires_at,
            ),
        )
        did_advance = True
    elif did_advance:
        await DALEntitlements.update_by_id(
            session,
            current.id,
            DAOEntitlementsUpdate(
                product_id=proposed.product_id or current.product_id,
                entitlement_id=proposed.entitlement_id or current.entitlement_id,
                active=proposed.active,
                expires_at=proposed.expires_at,
                updated_at=_utcnow(),
            ),
        )
        # refresh
        refreshed = await DALEntitlements.get_by_id(session, current.id)
        assert refreshed is not None
        current = refreshed

    will_auto_renew = _compute_will_auto_renew(
        snap.event_type,
    )

    # Always append an audit event for the attempt (just like payments)
    await DALSubscriptionEvents.create(
        session,
        DAOSubscriptionEventsCreate(
            user_id=user_id,
            product_id=snap.product_id,
            entitlement_id=snap.entitlement_id,
            rc_event_id=snap.event_id,
            rc_event_type=snap.event_type,
            source=ctx.source,
            payload={
                "applied_status": applied_status.value,
                "will_auto_renew": will_auto_renew,
                "proposed": {
                    "active": proposed.active,
                    "expires_at": proposed.expires_at.isoformat()
                    if proposed.expires_at
                    else None,
                    "product_id": proposed.product_id,
                    "entitlement_id": proposed.entitlement_id,
                },
                "current_after": {
                    "active": bool(current.active),
                    "expires_at": current.expires_at.isoformat()
                    if current.expires_at
                    else None,
                    "product_id": current.product_id,
                    "entitlement_id": current.entitlement_id,
                },
                **(ctx.extra_payload or {}),
            },
            signature_verified=ctx.signature_verified,
            applied_status=applied_status,
            event_timestamp_ms=snap.event_timestamp_ms,
        ),
    )

    return current, applied_status, did_advance


async def compute_canonical_entitlement_status(
    session: AsyncSession, *, user_id: UUID
) -> CanonicalEntitlementStatus:
    """
    Returns the canonical entitlement snapshot for a user, plus a *derived*
    will_auto_renew_on_expiration flag from the most recent RC event.

    This does **not** mutate DB; it only reads and computes.
    """
    # 1) Fetch the (single) entitlement row for this user (if any)
    ent_rows = await DALEntitlements.list_all(
        session, filters={"user_id": (FilterOp.EQ, user_id)}, limit=1
    )
    ent: Optional[DAOEntitlements] = ent_rows[0] if ent_rows else None

    # 2) Compute "active now" by respecting expires_at if present
    active_now = False
    if ent is not None:
        exp = _ensure_aware_utc_optional(ent.expires_at)
        active_now = bool(ent.active) and (exp is None or _utcnow() < exp)

    # 3) Look up the most recent RC event for this user and derive will_auto_renew
    # Note: the generated DAL commonly defaults to ORDER BY created_at DESC.
    # If your DAL supports explicit ordering, feel free to add it here.
    last_evt = None
    last_evt_rows = await DALSubscriptionEvents.list_all(
        session,
        filters={"user_id": (FilterOp.EQ, user_id)},
        order_by=[("event_timestamp_ms", OrderDirection.DESC)],
        limit=1,
    )
    if last_evt_rows:
        last_evt = last_evt_rows[0]

    will_auto_renew = _compute_will_auto_renew(
        getattr(last_evt, "rc_event_type", None) if last_evt else None
    )

    return CanonicalEntitlementStatus(
        active=active_now,
        will_auto_renew_on_expiration=will_auto_renew,
        entitlement=ent,
    )


def _pick_best_entitlement_from_subscriber(
    sub: RCSubscriber,
) -> tuple[Optional[str], Optional[datetime], Optional[str], Optional[datetime]]:
    """
    Returns (entitlement_id, expires_at, product_id, purchase_date)
    - Prefer lifetime (expires_date=None) first
    - else farthest expires_date
    """
    best_ent_id: Optional[str] = None
    best_exp: Optional[datetime] = None
    best_prod: Optional[str] = None
    best_purchase: Optional[datetime] = None

    for ent_id, ent in sub.entitlements.items():
        exp = ent.expires_date
        purchase = ent.purchase_date
        prod = ent.product_identifier
        if exp is None:
            return ent_id, None, prod, purchase
        if best_exp is None or (exp and exp > best_exp):
            best_ent_id, best_exp, best_prod, best_purchase = (
                ent_id,
                exp,
                prod,
                purchase,
            )
    return best_ent_id, best_exp, best_prod, best_purchase


def _latest_subscription_from_subscriber(sub: RCSubscriber) -> Optional[RCSubscription]:
    latest_sub: Optional[RCSubscription] = None
    latest_exp: Optional[datetime] = None
    for s in sub.subscriptions.values():
        exp = s.expires_date
        if latest_exp is None or (exp and exp > latest_exp):
            latest_sub, latest_exp = s, exp
    return latest_sub


def derive_snapshot_from_customer_response(
    *,
    user_id: UUID,
    resp: "RCGetSubscriberResponse",
    now: Optional[datetime] = None,
) -> RCSubscriptionSnapshot:
    """
    Pure converter:
      RCGetSubscriberResponse  -> RCSubscriptionSnapshot
    Mirrors the logic in eager_fetch to select event_type + fields.
    """
    _now = _ensure_aware_utc(now or _utcnow())
    req_ms = resp.request_date_ms
    sub = resp.subscriber

    ent_id, ent_exp, ent_prod, ent_purchase = _pick_best_entitlement_from_subscriber(
        sub
    )
    latest_sub = _latest_subscription_from_subscriber(sub)

    product_id = ent_prod or (latest_sub.product_identifier if latest_sub else None)
    expires_at = ent_exp

    billing_issues_detected_at = (
        latest_sub.billing_issues_detected_at if latest_sub else None
    )
    grace_expires = latest_sub.grace_period_expires_date if latest_sub else None
    unsubscribe_detected_at = latest_sub.unsubscribe_detected_at if latest_sub else None
    auto_resume_date = latest_sub.auto_resume_date if latest_sub else None

    if expires_at is None and product_id:
        event_type = "RENEWAL"
        cancel_reason = None
    elif expires_at and _now < expires_at:
        if billing_issues_detected_at and (not grace_expires or _now < grace_expires):
            event_type = "BILLING_ISSUE"
        elif unsubscribe_detected_at and not auto_resume_date:
            event_type = "CANCELLATION"
        else:
            purchase_date = (
                latest_sub.purchase_date if latest_sub else None
            ) or ent_purchase
            if purchase_date and (_now - purchase_date).total_seconds() < 60:
                event_type = "INITIAL_PURCHASE"
            else:
                event_type = "RENEWAL"
        cancel_reason = None
    else:
        event_type = "EXPIRATION"
        cancel_reason = None

    return RCSubscriptionSnapshot(
        user_id=user_id,
        product_id=product_id or "",
        entitlement_id=ent_id,
        event_type=event_type,
        event_id=f"subscribers:{req_ms}" if req_ms is not None else None,
        event_timestamp_ms=req_ms,
        purchased_at=ent_purchase,
        expires_at=expires_at,
        cancel_reason=cancel_reason,
    )


def build_ctx_for_eager_fetch(
    snap: RCSubscriptionSnapshot, resp: RCGetSubscriberResponse
) -> StatusApplyContext:
    """
    Pure: decide StatusApplyContext fields for eager fetch.
    (You currently use SubscriptionEventSource.SYSTEM; keep that for compatibility.)
    """
    resp_payload: dict[str, Any] = dict()
    try:
        resp_payload = resp.model_dump(mode="json")
    except Exception:
        pass

    return StatusApplyContext(
        source=SubscriptionEventSource.SYSTEM,
        rc_event_type=snap.event_type,
        rc_event_id=snap.event_id,
        signature_verified=None,
        extra_payload={"eager_fetch": True, "raw_payload": resp_payload},
    )


def _get_rc_app_user_id(user_id: UUID) -> str:
    return str(user_id).upper()


async def eager_fetch_and_apply_rc_status_owning_txn(
    session: AsyncSession,
    *,
    user_id: UUID,
    rc_client: RCClient,
) -> tuple[DAOEntitlements, SubscriptionStatus, bool]:
    if session.in_transaction():
        raise RuntimeError(
            "[eager_fetch_and_apply_rc_status_owning_txn] Must be called with no active transaction on the session. "
            "Commit/close the caller transaction first."
        )

    resp: RCGetSubscriberResponse = await rc_client.get_customer(
        app_user_id=_get_rc_app_user_id(user_id)
    )

    snap = derive_snapshot_from_customer_response(user_id=user_id, resp=resp)
    ctx = build_ctx_for_eager_fetch(snap, resp)

    async with safe_transaction(session, "apply subscription status"):
        return await reconcile_and_apply_subscription_status_in_txn(
            session=session, user_id=user_id, snap=snap, ctx=ctx
        )


async def should_skip_eager_fetch(session: AsyncSession, user_id: UUID) -> bool:
    """
    Return True when DB indicates the entitlement is already fresh enough that an eager
    RC pull is very likely redundant. Be conservative: skip only when clearly fresh.
    """
    # Load entitlement
    ent_rows = await DALEntitlements.list_all(
        session,
        filters={"user_id": (FilterOp.EQ, user_id)},
        limit=1,
    )
    ent = ent_rows[0] if ent_rows else None
    if ent is None:
        return False

    now = datetime.now(timezone.utc)

    # Active + not basically expiring right now
    exp = ent.expires_at if ent.expires_at else None
    if not (ent.active and (exp is None or now < exp - timedelta(seconds=10))):
        return False

    # Updated very recently? Good enough to skip if so.
    ent_updated = getattr(ent, "updated_at", None)
    if ent_updated and (
        now - ent_updated <= timedelta(seconds=_EAGER_SKIP_ENT_UPDATED_FRESH_S)
    ):
        return True

    # Otherwise look at the most recent RC event
    last_evt_rows = await DALSubscriptionEvents.list_all(
        session,
        filters={"user_id": (FilterOp.EQ, user_id)},
        order_by=[("event_timestamp_ms", OrderDirection.DESC)],
        limit=1,
    )
    if not last_evt_rows:
        return False

    last_evt = last_evt_rows[0]
    if (last_evt.rc_event_type or "").upper() not in _ACTIVE_EVENT_TYPES:
        return False

    evt_ms = getattr(last_evt, "event_timestamp_ms", None)
    if evt_ms is None:
        return False  # missing timestamp ⇒ don’t assume freshness

    evt_dt = datetime.fromtimestamp(evt_ms / 1000, tz=timezone.utc)
    return (now - evt_dt) <= timedelta(seconds=_EAGER_SKIP_RC_EVENT_FRESH_S)
