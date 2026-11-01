"""uv run python -m mem0_postgres_store.migrate --dimensions 1536"""

import argparse
import os

from alembic import command
from alembic.config import Config


def migration_config(url: str, *, dimensions: int, runtime_role: str) -> Config:
    config = Config()
    config.set_main_option("script_location", "mem0_postgres_store:migrations")
    config.attributes["url"] = url
    config.cmd_opts = argparse.Namespace(
        x=[
            f"dimensions={dimensions}",
            f"runtime_role={runtime_role}",
        ]
    )
    return config


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply packaged Alembic migrations")
    parser.add_argument("--dimensions", type=int, required=True)
    parser.add_argument("--runtime-role", default="mem0_runtime")
    args = parser.parse_args()
    url = os.environ["MEM0_MIGRATION_URL"]
    command.upgrade(
        migration_config(url, dimensions=args.dimensions, runtime_role=args.runtime_role), "head"
    )


if __name__ == "__main__":
    main()
