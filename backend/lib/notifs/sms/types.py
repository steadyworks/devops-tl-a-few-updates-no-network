from dataclasses import dataclass
from enum import StrEnum

from pydantic import BaseModel


class SMSStatus(StrEnum):
    QUEUED = "queued"
    SENDING = "sending"
    SENT = "sent"
    FAILED = "failed"
    DELIVERED = "delivered"
    UNDELIVERED = "undelivered"
    RECEIVING = "receiving"
    RECEIVED = "received"
    ACCEPTED = "accepted"
    SCHEDULED = "scheduled"
    READ = "read"
    PARTIALLY_DELIVERED = "partially_delivered"
    CANCELED = "canceled"


@dataclass(frozen=True)
class SMSMessage:
    """
    Provider-agnostic SMS shape.

    - `to`: E.164 phone number.
    - At least one of `from_` or `messaging_service_sid` must be set
      (either on the message or via env defaults in the provider).
    """

    to: str
    body: str
    media_url: list[str]
    idempotency_key: str


class SMSSendResult(BaseModel):
    message_id: str | None = None
    idempotency_key: str | None = None
    to: str | None = None
    status: SMSStatus | None = None
