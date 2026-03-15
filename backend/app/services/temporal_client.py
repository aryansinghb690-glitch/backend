from functools import lru_cache

from temporalio.client import Client

from app.core.config import get_settings


@lru_cache
def _settings_tuple() -> tuple[str, str, str | None]:
    settings = get_settings()
    return settings.TEMPORAL_SERVER_URL, settings.TEMPORAL_NAMESPACE, settings.TEMPORAL_API_KEY


_client: Client | None = None


async def get_temporal_client() -> Client:
    global _client
    if _client is None:
        settings = get_settings()

        _client = await Client.connect(
            settings.TEMPORAL_SERVER_URL,
            namespace=settings.TEMPORAL_NAMESPACE,
            rpc_metadata={
                "authorization": f"Bearer {settings.TEMPORAL_API_KEY}"
            },
            tls=True,
        )

    return _client

print("SERVER:", settings.TEMPORAL_SERVER_URL)
print("NAMESPACE:", settings.TEMPORAL_NAMESPACE)
print("API_KEY:", settings.TEMPORAL_API_KEY)
