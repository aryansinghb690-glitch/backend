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
        server, namespace, api_key = _settings_tuple()

        connect_kwargs: dict[str, Any] = {
            "namespace": namespace,
        }

        if api_key:
            connect_kwargs["api_key"] = api_key
            connect_kwargs["tls"] = True

        _client = await Client.connect(
            server,
            **connect_kwargs,
        )

    return _client
