import asyncio
import sys
import os
import aiomysql
from sqlalchemy import inspect
from typing import Dict, List, Tuple, Callable, Optional

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.lofig import Config
from app.db import cfg, engine, Base
from app.users.models import User


async def migrate_table(dbtable, engine_to_use=None):
    """Recreate a table while preserving data for matching columns.

    Args:
        dbtable: SQLAlchemy Table object or declarative model class
        engine_to_use: optional AsyncEngine to operate on (useful for tests)
    """
    if engine_to_use is None:
        engine_to_use = engine

    # get Table object whether a declarative class or a Table
    table = getattr(dbtable, '__table__', dbtable)

    async with engine_to_use.begin() as conn:
        # check table existence
        exists = await conn.run_sync(lambda sync_conn: inspect(sync_conn).has_table(table.name))
        if not exists:
            await conn.run_sync(lambda sync_conn: table.create(bind=sync_conn))
            return

        # dump existing rows as list of dicts
        def _dump(sync_conn):
            # prefer selecting all columns from the actual DB table (avoids mismatches when model changed)
            try:
                from sqlalchemy import text
                stmt = text(f"SELECT * FROM `{table.name}`")
            except Exception:
                # fallback to building a select from the Table object
                try:
                    from sqlalchemy import select
                    stmt = select(*table.columns)
                except Exception:
                    stmt = table.select()

            # Attempt to execute the statement using multiple possible APIs
            try:
                res = sync_conn.execute(stmt)
            except Exception:
                try:
                    # some implementations expose the raw DBAPI connection at `.connection`
                    res = sync_conn.connection.execute(stmt)
                except Exception:
                    try:
                        # fallback to executing raw SQL
                        res = sync_conn.exec_driver_sql(str(stmt))
                    except Exception as e:
                        # re-raise with context for easier debugging
                        raise RuntimeError(f"failed to execute statement using available sync connection adapters: {e}")

            # 1) Preferred: use mappings() if available
            try:
                return res.mappings().all()
            except Exception:
                pass

            # 2) Fallback: iterate rows and use _mapping (Row._mapping available in modern SQLAlchemy)
            try:
                return [dict(r._mapping) for r in res]
            except Exception:
                pass

            # 3) Fallback: fetchall and zip with column names
            rows = res.fetchall()
            cols = [c.name for c in table.columns]
            return [dict(zip(cols, r)) for r in rows]

        old_rows = await conn.run_sync(_dump)
        if not old_rows:
            # nothing to preserve, just recreate
            await conn.run_sync(lambda sync_conn: table.drop(bind=sync_conn))
            await conn.run_sync(lambda sync_conn: table.create(bind=sync_conn))
            return

        old_cols = set(old_rows[0].keys())
        new_cols = [c.name for c in table.columns]
        # keep order: new table column order intersecting old columns
        common_cols = [c for c in new_cols if c in old_cols]

        # prepare rows filtered to common columns
        rows_to_insert = [{k: r.get(k) for k in common_cols} for r in old_rows]

        # drop and recreate table, then bulk insert preserved columns
        await conn.run_sync(lambda sync_conn: table.drop(bind=sync_conn))
        await conn.run_sync(lambda sync_conn: table.create(bind=sync_conn))

        if rows_to_insert:
            await conn.run_sync(lambda sync_conn: sync_conn.execute(table.insert(), rows_to_insert))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(migrate_table(User))
