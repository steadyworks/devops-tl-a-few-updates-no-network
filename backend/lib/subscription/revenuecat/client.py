from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, TypeVar, cast

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.lib.utils.retryable import retryable_with_backoff

TModel = TypeVar("TModel", bound=BaseModel)

UTC = timezone.utc


def _parse_iso_to_utc(v: Optional[str]) -> Optional[datetime]:
    if not v:
        return None
    # Normalize trailing Z
    s = v[:-1] + "+00:00" if v.endswith("Z") else v
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class RCEntitlement(BaseModel):
    """
    One entitlement under subscriber.entitlements.<entitlement_id>
    """

    model_config = ConfigDict(extra="ignore")

    expires_date: Optional[datetime] = None
    grace_period_expires_date: Optional[datetime] = None
    product_identifier: Optional[str] = None
    purchase_date: Optional[datetime] = None

    # --- Coerce ISO8601 -> aware UTC ---
    _coerce_expires = field_validator("expires_date", mode="before")(_parse_iso_to_utc)
    _coerce_grace = field_validator("grace_period_expires_date", mode="before")(
        _parse_iso_to_utc
    )
    _coerce_purchase = field_validator("purchase_date", mode="before")(
        _parse_iso_to_utc
    )


class RCSubscription(BaseModel):
    """
    One subscription under subscriber.subscriptions.<product_id>
    We inject the map key into product_identifier (see RCSubscriber validator).
    """

    model_config = ConfigDict(extra="ignore")

    product_identifier: Optional[str] = None

    auto_resume_date: Optional[datetime] = None
    billing_issues_detected_at: Optional[datetime] = None
    expires_date: Optional[datetime] = None
    grace_period_expires_date: Optional[datetime] = None
    original_purchase_date: Optional[datetime] = None
    purchase_date: Optional[datetime] = None
    refunded_at: Optional[datetime] = None
    unsubscribe_detected_at: Optional[datetime] = None

    display_name: Optional[str] = None
    is_sandbox: Optional[bool] = None
    ownership_type: Optional[str] = None
    period_type: Optional[str] = None
    price: Optional[dict[str, Any]] = None
    store: Optional[str] = None
    store_transaction_id: Optional[str] = None

    _coerce_auto_resume = field_validator("auto_resume_date", mode="before")(
        _parse_iso_to_utc
    )
    _coerce_billing = field_validator("billing_issues_detected_at", mode="before")(
        _parse_iso_to_utc
    )
    _coerce_expires = field_validator("expires_date", mode="before")(_parse_iso_to_utc)
    _coerce_grace = field_validator("grace_period_expires_date", mode="before")(
        _parse_iso_to_utc
    )
    _coerce_orig = field_validator("original_purchase_date", mode="before")(
        _parse_iso_to_utc
    )
    _coerce_purchase = field_validator("purchase_date", mode="before")(
        _parse_iso_to_utc
    )
    _coerce_refund = field_validator("refunded_at", mode="before")(_parse_iso_to_utc)
    _coerce_unsub = field_validator("unsubscribe_detected_at", mode="before")(
        _parse_iso_to_utc
    )


class RCSubscriber(BaseModel):
    """
    The core 'subscriber' object from GET /subscribers/{app_user_id}.
    subscriptions/entitlements are maps; we keep them as dicts but ensure
    each RCSubscription has product_identifier injected from the key.
    """

    model_config = ConfigDict(extra="ignore")

    entitlements: Dict[str, RCEntitlement] = Field(default_factory=dict)
    subscriptions: Dict[str, RCSubscription] = Field(default_factory=dict)
    original_app_user_id: str

    @field_validator("subscriptions", mode="before")
    @classmethod
    def _inject_product_identifier(cls, v: dict[str, Any] | Any) -> Any:
        """
        RevenueCat returns a map: { "<product_id>": { ... } }.
        Inject the key as product_identifier inside the value for convenience.
        """
        if not isinstance(v, dict):
            return v
        out: dict[str, Any] = {}
        for k, sub in cast("dict[str, Any]", v).items():
            if isinstance(sub, dict):
                sub_typed: dict[str, Any] = dict(cast("dict[str, Any]", sub))
                out[k] = {"product_identifier": k, **sub_typed}
            else:
                out[k] = sub
        return out


class RCGetSubscriberResponse(BaseModel):
    """
    Top-level envelope from GET /subscribers/{app_user_id}
    """

    model_config = ConfigDict(extra="ignore")

    request_date: Optional[str] = None
    request_date_ms: Optional[int] = None
    subscriber: RCSubscriber


class RCError(Exception):
    pass


class RCRateLimited(RCError):
    """HTTP 429."""

    pass


class RCServerError(RCError):
    """HTTP 5xx (retryable)."""

    def __init__(self, status_code: int, payload: str) -> None:
        super().__init__(f"RC server error {status_code}")
        self.status_code = status_code
        self.payload = payload


class RCHTTPError(RCError):
    """Non-JSON or unexpected HTTP response payloads."""

    def __init__(self, status_code: int, payload: str) -> None:
        super().__init__(f"HTTP {status_code}: {payload}")
        self.status_code = status_code
        self.payload = payload


class RCAPIError(RCError):
    """
    JSON body with error fields (when available).
    RevenueCat doesn't have a single universal error envelope; we capture raw.
    """

    def __init__(self, status_code: int, raw: dict[str, Any]) -> None:
        super().__init__(f"RC API error {status_code}")
        self.status_code = status_code
        self.raw = raw


class RCClient:
    """
    Strongly-typed RC client:
      - Bearer secret key
      - Retries 429/network/5xx
      - Centralized Pydantic parsing
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://api.revenuecat.com/v1",
        http_timeout: float = 12.0,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._http = httpx.AsyncClient(
            http2=True,
            timeout=httpx.Timeout(connect=3.0, read=http_timeout, write=10.0, pool=5.0),
        )

    def _headers(self) -> dict[str, str]:
        h = {
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Accept-Encoding": "identity",
        }
        return h

    # -------- Public API --------

    async def get_customer(self, *, app_user_id: str) -> RCGetSubscriberResponse:
        return await self._retried_get_json(
            path=f"/subscribers/{app_user_id}",
            params=None,
            response_model=RCGetSubscriberResponse,
        )

    # -------- Core flow --------

    async def _retried_get_json(
        self,
        *,
        path: str,
        params: Mapping[str, Any] | None,
        response_model: type[TModel],
    ) -> TModel:
        async def _get() -> TModel:
            try:
                resp_json = await self._get_json(path=path, params=params)
                return response_model.model_validate(resp_json)
            except Exception as e:
                logging.warning(f"[rc] exception: {e}")
                raise

        return await retryable_with_backoff(
            _get,
            (
                httpx.ConnectError,
                httpx.ReadTimeout,
                httpx.WriteError,
                RCRateLimited,
                RCServerError,
            ),
            max_attempts=3,
            base_delay=0.5,
        )

    async def _get_json(
        self,
        *,
        path: str,
        params: Mapping[str, Any] | None,
    ) -> Mapping[str, Any]:
        url = f"{self._base_url}{path}"
        r = await self._http.get(url, params=params, headers=self._headers())
        return self._decode_or_raise(r)

    def _decode_or_raise(self, r: httpx.Response) -> Mapping[str, Any]:
        # RC uses 200 for GET success; treat 201 as success too (docs mention 201 in some flows)
        if r.status_code == 429:
            raise RCRateLimited("Rate limited by RevenueCat (429).")

        text = r.text
        try:
            data = r.json()
        except ValueError:
            if 500 <= r.status_code < 600:
                raise RCServerError(r.status_code, text)
            raise RCHTTPError(r.status_code, text)

        if r.status_code in (200, 201):
            return cast("dict[str, Any]", data)

        # No universal envelope; still surface structured body if present
        if 500 <= r.status_code < 600:
            raise RCServerError(r.status_code, text)

        # 4xx (and other non-2xx)
        raise RCAPIError(r.status_code, raw=data)

    async def close(self) -> None:
        if not self._http.is_closed:
            await self._http.aclose()
