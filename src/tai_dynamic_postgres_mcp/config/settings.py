from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings

load_dotenv()


class PostgresSettings(BaseSettings):
    model_config = {
        "env_prefix": "PG_",
    }
    host: str = Field("localhost", description="PostgreSQL host")
    port: int = Field(5432, description="PostgreSQL port")
    db: str = Field("postgres", description="Database name")
    user: str = Field("postgres", description="Database user")
    password: str = Field("password", description="Database password")

    # Pool configuration
    pool_min_size: int = Field(1, description="Minimum number of pooled connections")
    pool_max_size: int = Field(10, description="Maximum number of pooled connections")
    pool_timeout: int = Field(10, description="Pool acquire timeout in seconds")
    pool_max_lifetime: int = Field(300, description="Max lifetime of connection in seconds")


pg_settings = PostgresSettings()
