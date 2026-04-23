# backend/lib/notifs/email/resend.py

from typing import Any

import httpx
import resend
from resend.exceptions import ResendError

from backend.db.data_models import ShareProvider
from backend.env_loader import EnvLoader
from backend.lib.utils.rate_limiter import AsyncRateLimiter
from backend.lib.utils.retryable import retryable_with_backoff

from .base import AbstractEmailProvider
from .types import EmailMessage, EmailSendResult


class ResendHTTPClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = httpx.AsyncClient(
            base_url="https://api.resend.com",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=10.0,
        )

    async def send_email(
        self, params: resend.Emails.SendParams, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        headers: dict[str, Any] = {}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        resp = await self.client.post("/emails", json=params, headers=headers)
        resp.raise_for_status()
        return resp.json()

    async def aclose(self) -> None:
        await self.client.aclose()


class ResendEmailProvider(AbstractEmailProvider):
    def __init__(self) -> None:
        api_key = EnvLoader.get("RESEND_API_KEY")
        self._http_client = ResendHTTPClient(api_key)
        self._limiter = AsyncRateLimiter(rate=2, per=1.0)

    @classmethod
    def get_share_provider(cls) -> ShareProvider:
        return ShareProvider.RESEND

    def _build_params(self, msg: EmailMessage) -> resend.Emails.SendParams:
        send_params: resend.Emails.SendParams = {
            "from": msg.from_.as_rfc822(),
            "to": [addr.email for addr in msg.to_],
            "subject": msg.subject,
            "html": msg.html,
        }
        if msg.reply_to:
            send_params["reply_to"] = msg.reply_to.email
        return send_params

    async def _send_once(self, msg: EmailMessage) -> EmailSendResult:
        params = self._build_params(msg)
        async with self._limiter:
            resp = await self._http_client.send_email(params, msg.idempotency_key)
        return EmailSendResult(
            message_id=resp["id"], idempotency_key=msg.idempotency_key
        )

    async def send(self, msg: EmailMessage) -> EmailSendResult:
        """
        Public entrypoint. Retries on ResendError with backoff,
        and enforces global 2QPS limit.
        """
        return await retryable_with_backoff(
            lambda: self._send_once(msg),
            retryable=(ResendError, OSError, TimeoutError, ConnectionError),
            max_attempts=3,
            base_delay=0.5,
        )

    async def aclose(self) -> None:
        await self._http_client.aclose()
