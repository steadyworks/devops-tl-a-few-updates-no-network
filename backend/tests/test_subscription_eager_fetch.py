# backend/tests/test_subscription_eager_fetch.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

import backend.lib.subscription.service as svc_mod
from backend.db.dal import DALEntitlements, DAOEntitlementsCreate, FilterOp
from backend.db.data_models import (
    DAOEntitlements,
    SubscriptionEventSource,
    SubscriptionStatus,
)

# Import the pydantic types directly to build RC payloads
from backend.lib.subscription.revenuecat.client import (
    RCEntitlement,
    RCGetSubscriberResponse,
    RCSubscriber,
    RCSubscription,
)
from backend.lib.subscription.service import (
    RCSubscriptionSnapshot,
    StatusApplyContext,
    build_ctx_for_eager_fetch,
    derive_snapshot_from_customer_response,
    eager_fetch_and_apply_rc_status_owning_txn,
)

UTC = timezone.utc


def _fixnow() -> datetime:
    # Deterministic "now"
    return datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)


def _mk_resp(
    *,
    request_ms: int = 1735737600000,  # arbitrary
    entitlements: dict[str, dict[str, Any]] | None = None,
    subscriptions: dict[str, dict[str, Any]] | None = None,
) -> RCGetSubscriberResponse:
    ent_models = {
        k: RCEntitlement.model_validate(v) for k, v in (entitlements or {}).items()
    }
    sub_models = {
        k: RCSubscription.model_validate(v) for k, v in (subscriptions or {}).items()
    }
    return RCGetSubscriberResponse(
        request_date_ms=request_ms,
        request_date=None,
        subscriber=RCSubscriber(
            entitlements=ent_models,
            subscriptions=sub_models,
            original_app_user_id="ignored",
        ),
    )


# ---------- PURE mapper tests ----------


def test_SE1_lifetime_entitlement_maps_to_RENEWAL() -> None:
    u = uuid4()
    resp = _mk_resp(
        entitlements={
            "pro": {
                "expires_date": None,  # lifetime
                "product_identifier": "pro.lifetime",
                "purchase_date": "2024-12-31T23:59:10Z",
            }
        },
        subscriptions={},  # empty fallback
    )
    snap = derive_snapshot_from_customer_response(user_id=u, resp=resp, now=_fixnow())
    assert snap.event_type == "RENEWAL"
    assert snap.product_id == "pro.lifetime"
    assert snap.entitlement_id == "pro"
    assert snap.expires_at is None
    assert snap.event_id == f"subscribers:{resp.request_date_ms}"


def test_SE2_active_with_billing_issue_maps_to_BILLING_ISSUE() -> None:
    u = uuid4()
    future = (_fixnow() + timedelta(days=3)).isoformat()
    grace_future = (_fixnow() + timedelta(days=2)).isoformat()
    resp = _mk_resp(
        entitlements={
            "pro": {
                "expires_date": future,
                "product_identifier": "pro.monthly",
                "purchase_date": "2024-12-31T12:00:00Z",
            }
        },
        subscriptions={
            # later expiry matches entitlement; billing issue active, grace not passed
            "pro.monthly": {
                "expires_date": future,
                "purchase_date": "2024-12-31T12:00:00Z",
                "billing_issues_detected_at": (
                    _fixnow() - timedelta(minutes=5)
                ).isoformat(),
                "grace_period_expires_date": grace_future,
            }
        },
    )
    snap = derive_snapshot_from_customer_response(user_id=u, resp=resp, now=_fixnow())
    assert snap.event_type == "BILLING_ISSUE"
    assert snap.product_id == "pro.monthly"


def test_SE3_active_unsubscribed_maps_to_CANCELLATION() -> None:
    u = uuid4()
    future = (_fixnow() + timedelta(days=10)).isoformat()
    resp = _mk_resp(
        entitlements={
            "pro": {
                "expires_date": future,
                "product_identifier": "pro.yearly",
            }
        },
        subscriptions={
            "pro.yearly": {
                "expires_date": future,
                "unsubscribe_detected_at": (_fixnow() - timedelta(days=1)).isoformat(),
                # no auto_resume_date -> real cancellation
            }
        },
    )
    snap = derive_snapshot_from_customer_response(user_id=u, resp=resp, now=_fixnow())
    assert snap.event_type == "CANCELLATION"
    assert snap.product_id == "pro.yearly"


def test_SE4_expired_maps_to_EXPIRATION() -> None:
    u = uuid4()
    past = (_fixnow() - timedelta(days=1)).isoformat()
    resp = _mk_resp(
        entitlements={
            "pro": {
                "expires_date": past,
                "product_identifier": "pro.monthly",
            }
        }
    )
    snap = derive_snapshot_from_customer_response(user_id=u, resp=resp, now=_fixnow())
    assert snap.event_type == "EXPIRATION"


def test_SE5_recent_purchase_looks_like_INITIAL_PURCHASE_else_RENEWAL() -> None:
    u = uuid4()
    # Active; purchase within same minute → INITIAL_PURCHASE
    future = (_fixnow() + timedelta(days=7)).isoformat()
    recent = (_fixnow() - timedelta(seconds=20)).isoformat()
    resp_recent = _mk_resp(
        entitlements={
            "pro": {
                "expires_date": future,
                "product_identifier": "pro.monthly",
                "purchase_date": recent,
            }
        },
        subscriptions={
            "pro.monthly": {"expires_date": future, "purchase_date": recent}
        },
    )
    snap_recent = derive_snapshot_from_customer_response(
        user_id=u, resp=resp_recent, now=_fixnow()
    )
    assert snap_recent.event_type == "INITIAL_PURCHASE"

    # Same but old purchase → RENEWAL
    old = (_fixnow() - timedelta(hours=2)).isoformat()
    resp_old = _mk_resp(
        entitlements={
            "pro": {
                "expires_date": future,
                "product_identifier": "pro.monthly",
                "purchase_date": old,
            }
        },
        subscriptions={"pro.monthly": {"expires_date": future, "purchase_date": old}},
    )
    snap_old = derive_snapshot_from_customer_response(
        user_id=u, resp=resp_old, now=_fixnow()
    )
    assert snap_old.event_type == "RENEWAL"


def test_SE6_ctx_builder_sets_source_system_and_event_id() -> None:
    u = uuid4()
    resp = _mk_resp(
        entitlements={
            "pro": {"expires_date": None, "product_identifier": "pro.lifetime"}
        }
    )
    snap = derive_snapshot_from_customer_response(user_id=u, resp=resp, now=_fixnow())
    ctx = build_ctx_for_eager_fetch(snap, resp)
    assert ctx.source == SubscriptionEventSource.SYSTEM  # current code path
    assert ctx.rc_event_type == "RENEWAL"
    assert ctx.rc_event_id == snap.event_id
    assert ctx.extra_payload.get("eager_fetch") is True


# ---------- Thin integration test (wires into eager_fetch_and_apply_rc_status_owning_txn) ----------


@pytest.mark.asyncio
async def test_SE7_eager_fetch_calls_reconcile_with_expected_snapshot_and_ctx(
    db_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = uuid4()

    # 1) Freeze time for deterministic mapping
    monkeypatch.setattr(svc_mod, "_utcnow", lambda: _fixnow(), raising=True)

    # 2) RC client fake
    class _RCClientFake:
        def __init__(self, resp: RCGetSubscriberResponse) -> None:
            self._resp = resp
            self.calls = 0

        async def get_customer(self, *, app_user_id: str) -> RCGetSubscriberResponse:
            self.calls += 1
            return self._resp

    future = (_fixnow() + timedelta(days=30)).isoformat()
    rc_resp = _mk_resp(
        request_ms=111222333444,
        entitlements={
            "pro": {
                "expires_date": future,
                "product_identifier": "pro.monthly",
                "purchase_date": (_fixnow() - timedelta(hours=1)).isoformat(),
            }
        },
        subscriptions={"pro.monthly": {"expires_date": future}},
    )
    rc_fake = _RCClientFake(rc_resp)

    # 3) Patch reconcile to capture inputs and simulate a DB advance
    captured: dict[str, Any] = {}

    async def _reconcile_stub(
        session: AsyncSession,
        *,
        user_id: UUID,
        snap: RCSubscriptionSnapshot,
        ctx: StatusApplyContext,
    ) -> tuple[DAOEntitlements, SubscriptionStatus, bool]:
        captured["snap"] = snap
        captured["ctx"] = ctx
        # Create entitlement row so the function can return something reasonable
        # (A real reconcile would upsert and return DAOEntitlements)
        await DALEntitlements.create(
            session,
            DAOEntitlementsCreate(
                user_id=user_id,
                product_id=snap.product_id,
                entitlement_id=snap.entitlement_id,
                active=True,
                expires_at=snap.expires_at,
            ),
        )
        # Return tuple: (DAOEntitlements, SubscriptionStatus, did_advance)
        ent_rows = await DALEntitlements.list_all(
            session, filters={"user_id": (FilterOp.EQ, user_id)}, limit=1
        )
        return ent_rows[0], SubscriptionStatus.ACTIVE, True

    monkeypatch.setattr(
        svc_mod,
        "reconcile_and_apply_subscription_status_in_txn",
        _reconcile_stub,
        raising=True,
    )

    # 4) Call SUT
    (
        ent_after,
        applied_status,
        did_advance,
    ) = await eager_fetch_and_apply_rc_status_owning_txn(
        db_session,
        user_id=user_id,
        rc_client=rc_fake,  # type: ignore[arg-type] # pyright: ignore[reportArgumentType]
    )

    # 5) Assertions on wiring and mapping
    snap: RCSubscriptionSnapshot = captured["snap"]
    ctx = captured["ctx"]

    assert rc_fake.calls == 1
    assert snap.user_id == user_id
    assert snap.product_id == "pro.monthly"
    assert snap.event_type == "RENEWAL"
    assert snap.event_id == "subscribers:111222333444"
    assert ctx.source == SubscriptionEventSource.SYSTEM
    assert ctx.extra_payload.get("eager_fetch") is True

    assert applied_status == SubscriptionStatus.ACTIVE
    assert did_advance is True
    assert ent_after.user_id == user_id  # a DAOEntitlements row exists
