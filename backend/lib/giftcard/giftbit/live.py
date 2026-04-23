from backend.env_loader import EnvLoader

from .base import AbstractBaseGiftbitGiftcardClient
from .client import GiftbitAuth, GiftbitClient, GiftbitEndpoint, GiftbitRootURL


class GiftbitGiftcardClientLive(AbstractBaseGiftbitGiftcardClient):
    def _init_giftbit_client(self) -> GiftbitClient:
        return GiftbitClient(
            auth=GiftbitAuth(api_token=EnvLoader.get("GIFTBIT_API_KEY_PROD")),
            endpoint=GiftbitEndpoint.PROD,
            gift_link_endpoint=GiftbitRootURL.PROD,
        )
