"""Application settings, loaded from the environment (.env file)."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Central place for all configurable values. Add more as the app grows."""

    database_url: str
    app_name: str = "Setu Solutions Engineer Assignment"

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)


settings = Settings()
