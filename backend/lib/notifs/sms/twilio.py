from twilio.base.exceptions import TwilioRestException
from twilio.http.async_http_client import AsyncTwilioHttpClient
from twilio.rest import Client
from twilio.rest.api.v2010.account.message import MessageInstance

from backend.db.data_models import ShareProvider
from backend.env_loader import EnvLoader
from backend.lib.utils.retryable import retryable_with_backoff

from .base import AbstractSMSProvider
from .types import SMSMessage, SMSSendResult, SMSStatus


class TwilioSMSProvider(AbstractSMSProvider):
    """
    Thin async Twilio SMS wrapper.

    Usage:
        provider = TwilioSMSProvider()
        await provider.send(SMSMessage(
            to="+15551234567",
            body="Hello from Memry",
            idempotency_key="some-stable-key",
        ))

    This is intended to be constructed once per service lifecycle and reused.
    """

    def __init__(self) -> None:
        account_sid = EnvLoader.get("TWILIO_ACCOUNT_SID")
        auth_token = EnvLoader.get("TWILIO_AUTH_TOKEN")
        self._messaging_service_sid = EnvLoader.get("TWILIO_MESSAGING_SERVICE_SID")

        self._http_client = AsyncTwilioHttpClient()
        self._client = Client(account_sid, auth_token, http_client=self._http_client)

    @classmethod
    def get_share_provider(cls) -> ShareProvider:
        return ShareProvider.TWILIO

    async def _send_once(self, msg: SMSMessage) -> SMSSendResult:
        # No first-class idempotency in Twilio SMS; we keep idempotency_key
        # at our layer (DB, logs, etc). Could be attached as a custom tag
        # via other mechanisms if desired.
        twilio_msg: MessageInstance = await self._client.messages.create_async(
            messaging_service_sid=self._messaging_service_sid,
            to=msg.to,
            body=msg.body,
            media_url=msg.media_url,
        )

        return SMSSendResult(
            message_id=twilio_msg.sid,
            idempotency_key=msg.idempotency_key,
            to=twilio_msg.to,
            status=SMSStatus(twilio_msg.status)
            if isinstance(twilio_msg.status, str)
            else None,
        )

    async def send(self, msg: SMSMessage) -> SMSSendResult:
        """
        Public entrypoint.
        - Uses AsyncTwilioHttpClient under the hood.
        - Light retry with backoff on transient + Twilio errors.
        """
        return await retryable_with_backoff(
            lambda: self._send_once(msg),
            retryable=(TwilioRestException, OSError, TimeoutError, ConnectionError),
            max_attempts=3,
            base_delay=0.5,
        )

    async def aclose(self) -> None:
        if self._http_client:
            await self._http_client.close()
