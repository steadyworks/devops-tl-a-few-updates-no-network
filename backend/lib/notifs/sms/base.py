from abc import ABC, abstractmethod

from backend.db.data_models import ShareChannelType, ShareProvider
from backend.lib.notifs.protocol import NotificationProviderProtocol

from .types import SMSMessage, SMSSendResult


class AbstractSMSProvider(NotificationProviderProtocol, ABC):
    @classmethod
    def get_share_channel_type(cls) -> ShareChannelType:
        return ShareChannelType.SMS

    @classmethod
    @abstractmethod
    def get_share_provider(cls) -> ShareProvider:
        """
        Concrete providers must specify the backing provider enum.
        e.g. ShareProvider.TWILIO
        """

    @abstractmethod
    async def send(self, msg: SMSMessage) -> SMSSendResult:
        """
        Send a single SMS, returning provider-agnostic result info.
        """

    async def aclose(self) -> None:
        pass
