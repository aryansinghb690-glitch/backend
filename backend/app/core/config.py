from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    APP_NAME: str = "SagePilot API"
    API_PREFIX: str = "/api"
    DATABASE_URL: str = "postgresql+psycopg://postgres:postgres@localhost:5432/sagepilot"

    TEMPORAL_SERVER_URL: str = "ap-northeast-1.aws.api.temporal.io:7233"
    TEMPORAL_NAMESPACE: str = "default"
    TEMPORAL_API_KEY: str | None = None
    TEMPORAL_TASK_QUEUE: str = "sagepilot-task-queue"

    CORS_ORIGINS: str = "http://localhost:3000"


@lru_cache
def get_settings() -> Settings:
    return Settings()
