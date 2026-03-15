from functools import lru_cache
from temporalio.client import Client
from app.core.config import get_settings


@lru_cache
def _settings_tuple():
    settings = get_settings()

    server = settings.TEMPORAL_SERVER_URL.strip()
    namespace = settings.TEMPORAL_NAMESPACE.strip()
    api_key = getattr(settings, "TEMPORAL_API_KEY", None)

    return server, namespace, api_key


_client: Client | None = None


async def get_temporal_client() -> Client:
    global _client

    if _client is None:
        settings = get_settings()

        _client = await Client.connect(
            settings.TEMPORAL_SERVER_URL,
            namespace=settings.TEMPORAL_NAMESPACE,
            api_key=settings.TEMPORAL_API_KEY,
            tls=True,
        )

    return _client
