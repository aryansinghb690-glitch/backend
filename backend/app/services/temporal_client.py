from functools import lru_cache

from temporalio.client import Client

from app.core.config import get_settings


@lru_cache
def _settings_tuple() -> tuple[str, str]:
    settings = get_settings()
    return settings.TEMPORAL_SERVER_URL, settings.TEMPORAL_NAMESPACE


_client: Client | None = None


async def get_temporal_client() -> Client:
    global _client
    if _client is None:
        server, namespace = _settings_tuple()
        _client = await Client.connect(server, namespace=namespace)
    return _client
