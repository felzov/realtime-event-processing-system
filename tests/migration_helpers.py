"""Running Alembic from tests, without a subprocess and without touching logging."""

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine

ALEMBIC_INI = Path(__file__).resolve().parent.parent / "alembic.ini"


def alembic_config() -> Config:
    return Config(str(ALEMBIC_INI))


def migrate(engine: Engine, direction: str, revision: str) -> None:
    """Upgrade or downgrade to `revision` in one committed transaction on `engine`.

    The connection is handed to migrations/env.py, which then uses it instead of building an engine
    from the environment's DATABASE_URL.
    """
    config = alembic_config()
    with engine.begin() as connection:
        config.attributes["connection"] = connection
        if direction == "up":
            command.upgrade(config, revision)
        elif direction == "down":
            command.downgrade(config, revision)
        else:
            raise ValueError(f"direction must be 'up' or 'down', not {direction!r}")
