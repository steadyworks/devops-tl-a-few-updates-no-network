from backend.env_loader import EnvLoader

from .base import AbstractBaseAGCODGiftcardClient
from .client import AGCODClient, AGCODEndpoint, AWSRegion, Credentials


class AGCODGiftcardClientLive(AbstractBaseAGCODGiftcardClient):
    def _init_agcod_client(self) -> AGCODClient:
        return AGCODClient(
            partner_id=EnvLoader.get("AGCOD_PARTNER_ID"),
            credentials=Credentials(
                access_key_id=EnvLoader.get("AGCOD_ACCESS_KEY_ID_PRODUCTION"),
                secret_access_key=EnvLoader.get("AGCOD_ACCESS_SECRET_PRODUCTION"),
            ),
            endpoint=AGCODEndpoint.NA_PROD,
            region=AWSRegion.US_EAST_1,
        )
