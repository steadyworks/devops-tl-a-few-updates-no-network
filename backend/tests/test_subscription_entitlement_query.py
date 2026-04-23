from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.data_models import (
    DAOEntitlements,
    DAOSubscriptionEvents,
    SubscriptionEventSource,
)
from backend.lib.subscription.service import compute_canonical_entitlement_status

UTC = timezone.utc


# -------------------------
# Helpers
# -------------------------


async def _insert_entitlement(
    session: AsyncSession,
    *,
    user_id: UUID,
    active: bool,
    expires_at: datetime | None,
    product_id: str = "pro_yearly",
    entitlement_id: str | None = "pro",
) -> DAOEntitlements:
    row = DAOEntitlements(
        user_id=user_id,
        product_id=product_id,
        entitlement_id=entitlement_id,
        active=active,
        expires_at=expires_at,
    )
    session.add(row)
    await session.commit()
    return row


async def _insert_rc_event(
    session: AsyncSession,
    *,
    user_id: UUID,
    rc_event_type: str,
    product_id: str = "pro_yearly",
    entitlement_id: str | None = "pro",
    rc_event_id: str | None = None,
    event_timestamp_ms: int | None = None,
) -> DAOSubscriptionEvents:
    evt = DAOSubscriptionEvents(
        user_id=user_id,
        product_id=product_id,
        entitlement_id=entitlement_id,
        rc_event_id=rc_event_id,
        rc_event_type=rc_event_type,
        source=SubscriptionEventSource.SYSTEM,
        payload={},  # minimal required; your schema allows empty JSON
        signature_verified=True,
        applied_status=None,
        event_timestamp_ms=event_timestamp_ms,
    )
    session.add(evt)
    await session.commit()
    return evt


# -------------------------
# Tests
# -------------------------


@pytest.mark.asyncio
async def test_EC1_no_entitlement_returns_inactive_and_no_auto_renew(
    db_session: AsyncSession,
) -> None:
    user_id = uuid4()

    canon = await compute_canonical_entitlement_status(db_session, user_id=user_id)

    assert canon.entitlement is None
    assert canon.active is False
    # No events → unknown
    assert canon.will_auto_renew_on_expiration is None


@pytest.mark.asyncio
async def test_EC2_active_future_and_last_event_renewal_sets_auto_renew_true(
    db_session: AsyncSession,
) -> None:
    user_id = uuid4()
    exp = datetime.now(UTC) + timedelta(days=30)

    await _insert_entitlement(db_session, user_id=user_id, active=True, expires_at=exp)
    await _insert_rc_event(
        db_session,
        user_id=user_id,
        rc_event_type="RENEWAL",
        rc_event_id="evt_renew",
    )

    canon = await compute_canonical_entitlement_status(db_session, user_id=user_id)

    assert canon.entitlement is not None
    assert canon.active is True  # active flag && expires_in_future
    assert canon.will_auto_renew_on_expiration is True


@pytest.mark.asyncio
async def test_EC3_cancellation_keeps_active_until_expiry_but_auto_renew_false(
    db_session: AsyncSession,
) -> None:
    user_id = uuid4()
    exp_soon = datetime.now(UTC) + timedelta(days=1)

    await _insert_entitlement(
        db_session, user_id=user_id, active=True, expires_at=exp_soon
    )
    await _insert_rc_event(
        db_session,
        user_id=user_id,
        rc_event_type="CANCELLATION",
        rc_event_id="evt_cancel",
    )

    canon = await compute_canonical_entitlement_status(db_session, user_id=user_id)

    assert canon.entitlement is not None
    # Still active now because expires_at is in the future
    assert canon.active is True
    assert canon.will_auto_renew_on_expiration is False


@pytest.mark.asyncio
async def test_EC4_billing_issue_returns_unknown_auto_renew(
    db_session: AsyncSession,
) -> None:
    user_id = uuid4()
    exp_future = datetime.now(UTC) + timedelta(days=7)

    await _insert_entitlement(
        db_session, user_id=user_id, active=True, expires_at=exp_future
    )
    await _insert_rc_event(
        db_session,
        user_id=user_id,
        rc_event_type="BILLING_ISSUE",
        rc_event_id="evt_bi",
    )

    canon = await compute_canonical_entitlement_status(db_session, user_id=user_id)

    assert canon.entitlement is not None
    assert canon.active is True
    # Ambiguous without the paired cancellation; keep as None
    assert canon.will_auto_renew_on_expiration is None


@pytest.mark.asyncio
async def test_EC5_expired_entitlement_is_inactive_and_auto_renew_false_if_last_event_expiration(
    db_session: AsyncSession,
) -> None:
    user_id = uuid4()
    exp_past = datetime.now(UTC) - timedelta(days=1)

    await _insert_entitlement(
        db_session, user_id=user_id, active=False, expires_at=exp_past
    )
    await _insert_rc_event(
        db_session,
        user_id=user_id,
        rc_event_type="EXPIRATION",
        rc_event_id="evt_exp",
    )

    canon = await compute_canonical_entitlement_status(db_session, user_id=user_id)

    assert canon.entitlement is not None
    assert canon.active is False
    assert canon.will_auto_renew_on_expiration is False
