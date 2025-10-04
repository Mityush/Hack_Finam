from dataclasses import dataclass

from dotenv import load_dotenv

from .base import create_empty_config, fill_from_env


@dataclass
class PostgresDB:
    username: str
    password: str
    host: str
    port: int
    table: str
    do_backup: bool

    @property
    def alchemy_url(self) -> str:
        return (f"postgresql+asyncpg://"
                f"{self.username}:{self.password}@{self.host}:{self.port}/{self.table}")

    @property
    def url(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.table}"


# @dataclass
# class Bot:
#     token: str
#
#
# @dataclass
# class Services:
#     bot: Bot


@dataclass
class Config:
    db: PostgresDB
    # services: Services


def load_config() -> Config:
    load_dotenv()

    config = create_empty_config(Config)

    fill_from_env(config)

    return config
