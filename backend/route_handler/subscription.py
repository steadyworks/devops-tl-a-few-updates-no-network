# backend/route_handler/subscription

import logging
from typing import Optional
from uuid import UUID

from fastapi import Query, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.dal import safe_transaction
from backend.db.externals import EntitlementsOverviewResponse
from backend.lib.posthog import posthog_capture
from backend.lib.subscription.service import (
    compute_canonical_entitlement_status,
    eager_fetch_and_apply_rc_status_owning_txn,
    should_skip_eager_fetch,
)
from backend.route_handler.base import RouteHandler, enforce_response_model


class EntitlementStatusResponse(BaseModel):
    active: bool
    entitlement: Optional[EntitlementsOverviewResponse]
    will_auto_renew_on_expiration: Optional[bool] = None


class EntitlementAPIHandler(RouteHandler):
    """
    Returns the canonical entitlement snapshot for the authenticated user.
    Web and native should reflect this value for gating.
    """

    def register_routes(self) -> None:
        self.route("/api/me/entitlement", "me_entitlement", methods=["GET"])

    @enforce_response_model
    @posthog_capture()
    async def me_entitlement(
        self,
        request: Request,
        *,
        eager_fetch: bool = Query(default=False),
    ) -> EntitlementStatusResponse:
        async with self.app.db_session_factory.new_session() as session:
            rcx = await self.get_request_context(request)

            if eager_fetch:
                # Pull latest from RC and apply *within* our FSM/transaction.
                try:
                    async with safe_transaction(session):
                        do_fetch = not await should_skip_eager_fetch(
                            session, rcx.user_id
                        )

                    if do_fetch:
                        await eager_fetch_and_apply_rc_status_owning_txn(
                            session=session,
                            user_id=rcx.user_id,
                            rc_client=self.app.revenuecat_client,
                        )
                except Exception as e:
                    # Don’t block entitlement reads on RC flakiness; just log.
                    logging.exception(
                        "eager_fetch failed for user %s: %s", rcx.user_id, e
                    )

            async with safe_transaction(session):
                return await get_entitlement_status_response_for_user_id(
                    session, rcx.user_id
                )


async def get_entitlement_status_response_for_user_id(
    session: AsyncSession,
    user_id: UUID,
) -> EntitlementStatusResponse:
    status = await compute_canonical_entitlement_status(session, user_id=user_id)

    return EntitlementStatusResponse(
        active=status.active,
        will_auto_renew_on_expiration=status.will_auto_renew_on_expiration,
        entitlement=None
        if status.entitlement is None
        else EntitlementsOverviewResponse.from_dao(status.entitlement),
    )
