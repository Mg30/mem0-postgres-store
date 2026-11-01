"""Migration credentials come only from the Alembic composition boundary."""

from alembic import context
from sqlalchemy import create_engine, pool


def run() -> None:
    config = context.config
    url = config.attributes.get("url") or config.get_main_option("sqlalchemy.url")
    if not url:
        raise ValueError("Pass migration URL through Config.attributes['url'] or sqlalchemy.url")
    if context.is_offline_mode():
        context.configure(url=url, literal_binds=True)
        with context.begin_transaction():
            context.run_migrations()
        return
    engine = create_engine(url, poolclass=pool.NullPool)
    try:
        with engine.connect() as connection:
            context.configure(connection=connection)
            with context.begin_transaction():
                context.run_migrations()
    finally:
        engine.dispose()


run()
