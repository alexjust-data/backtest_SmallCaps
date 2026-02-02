from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # R2
    R2_ACCOUNT_ID: str
    R2_ACCESS_KEY_ID: str
    R2_SECRET_ACCESS_KEY: str
    R2_BUCKET: str
    R2_ENDPOINT: str
    R2_REGION: str = "auto"

    # Paths
    DATA_CACHE_DIR: str = "./data/cache"
    DATA_DERIVED_DIR: str = "./data/derived"
    RUNS_DIR: str = "./runs"

    # Defaults
    PROJECT_TZ: str = "America/New_York"
    CALENDAR: str = "nyse"
    LOG_LEVEL: str = "INFO"
    STRICT_VALIDATION: bool = False
    MAX_WORKERS: int = 8


settings = Settings()
