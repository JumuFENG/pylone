from typing import AsyncGenerator
from functools import cached_property
from sqlalchemy import select, delete, func, or_
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.lofig import Config
from contextlib import asynccontextmanager


Base = declarative_base()

class LazyDatabase:
    """懒加载数据库管理器"""
    @cached_property
    def database_url(self) -> str:
        cfg = {'user': 'root', 'password': '', 'host': 'localhost', 'port': 3306, 'dbname': 'user_management'}
        cfg.update(Config.database_config())
        return (
            f"mysql+aiomysql://{cfg['user']}:{Config.simple_decrypt(cfg['password'])}@"
            f"{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
        )
    
    @cached_property
    def async_session_maker(self):
        return sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)

    @cached_property
    def engine(self) -> AsyncEngine:
        return create_async_engine(self.database_url, echo=False, future=True)

    @asynccontextmanager
    async def get_async_session(self) -> AsyncGenerator[AsyncSession, None]:
        """获取异步session"""
        async with self.async_session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    
    async def query_one_value(self, model, field, *clauses):
        """查询单个值"""
        async with self.get_async_session() as session:
            column = getattr(model, field) if isinstance(field, str) else field
            result = await session.execute(select(column).where(*clauses))
            return result.scalar()
        
    async def close(self):
        """关闭数据库连接"""
        if self.engine:
            await self.engine.dispose()


db = LazyDatabase()

async def query_one_value(model, field, *clauses):
    """
    查询单个字段的值
    field: 字段名称，如 'name'
    返回: 字段值
    """
    async with db.get_async_session() as session:
        column = getattr(model, field) if isinstance(field, str) else field
        result = await session.execute(select(column).where(*clauses))
        return result.scalar()

async def query_one_record(model, *clauses):
    """
    查询单条记录
    返回: 记录对象
    """
    async with db.get_async_session() as session:
        result = await session.execute(select(model).where(*clauses))
        return result.scalar_one_or_none()

async def query_values(model, fields=None, *clauses):
    """
    查询多个字段的值
    fields: 字段列表，如 [Model.field1, Model.field2]
    返回: 结果列表，每个元素是一个元组
    """
    async with db.get_async_session() as session:
        if not fields:
            query = select(model.__table__.columns)
        elif isinstance(fields, (list, tuple)):
            if isinstance(fields[0], str):
                query = select(*[getattr(model, field) for field in fields])
            else:
                query = select(*fields)
        elif isinstance(fields, str):
            query = select(getattr(model, fields))
        else:
            query = select(fields)

        if clauses:
            query = query.where(*clauses)
        result = await session.execute(query)
        return result.all()

async def query_aggregate(func_type, model, field, *clauses):
    """
    查询聚合值
    func_type: 'max', 'min', 'count', 'sum', 'avg' 等
    """
    async with db.get_async_session() as session:
        func_map = {
            "max": func.max,
            "min": func.min,
            "count": func.count,
            "sum": func.sum,
            "avg": func.avg,
        }
        agg_func = func_map.get(func_type)
        if not agg_func:
            raise ValueError(f"Unsupported function type: {func_type}")

        query = select(agg_func(getattr(model, field) if isinstance(field, str) else field))
        if clauses:
            query = query.where(*clauses)
        result = await session.execute(query)
        return result.scalar()

async def query_group_counts(model, field, *clauses):
    """
    查询分组计数
    field: 分组字段名称，如 'category'
    返回: 字典，键为字段值，值为计数
    """
    async with db.get_async_session() as session:
        column = getattr(model, field) if isinstance(field, str) else field
        query = select(column, func.count()).group_by(column)
        if clauses:
            query = query.where(*clauses)
        result = await session.execute(query)
        return {row[0]: row[1] for row in result.all()}


async def upsert_one(model, data, unique_fields):
    """
    插入或更新单条记录
    data: 字典，包含字段和值
    unique_fields: 唯一字段列表，用于判断记录是否存在
    """
    async with db.get_async_session() as session:
        filters = [getattr(model, field) == data[field] for field in unique_fields]
        existing = await session.execute(select(model).where(*filters))
        instance = existing.scalar_one_or_none()

        if not instance:
            instance = model(**data)
            session.add(instance)
            await session.commit()
            return

        updated = False
        for key, value in data.items():
            if getattr(instance, key) != value:
                setattr(instance, key, value)
                updated = True

        if updated:
            await session.commit()

async def insert_many(model, data_list, unique_fields=[]):
    """
    批量插入多条记录，忽略已存在的记录
    data_list: 字典列表，包含字段和值
    unique_fields: 唯一字段列表，用于判断记录是否存在
    """
    if not data_list:
        return

    async with db.get_async_session() as session:
        if not unique_fields:
            to_add = [model(**data) for data in data_list]
            session.add_all(to_add)
            await session.commit()
            return

        to_add = []
        for data in data_list:
            filters = [getattr(model, field) == data[field] for field in unique_fields]
            existing = await session.execute(select(model).where(*filters))
            instance = existing.scalar_one_or_none()

            if not instance:
                to_add.append(model(**data))

        if to_add:
            session.add_all(to_add)
            await session.commit()

async def upsert_many_bulk(model, data_list, unique_fields):
    """
    批量插入或更新多条记录
    data_list: 字典列表，包含字段和值
    unique_fields: 唯一字段列表，用于判断记录是否存在
    """
    async with db.get_async_session() as session:
        to_add = []
        update_count = 0
        for data in data_list:
            filters = [getattr(model, field) == data[field] for field in unique_fields]
            existing = await session.execute(select(model).where(*filters))
            instance = existing.scalar_one_or_none()

            if not instance:
                to_add.append(model(**data))
                continue

            updated = False
            for key, value in data.items():
                if getattr(instance, key) != value:
                    setattr(instance, key, value)
                    updated = True
            if updated:
                update_count += 1

        if to_add:
            session.add_all(to_add)

        if to_add or updated:
            await session.commit()

        return len(to_add), update_count

async def upsert_many(model, data_list, unique_fields, chunk_size=1000):
    """
    批量插入或更新多条记录
    data_list: 字典列表，包含字段和值
    unique_fields: 唯一字段列表，用于判断记录是否存在
    bulk_size: 每次批量处理的记录数
    """
    added, updated = 0, 0

    for i in range(0, len(data_list), chunk_size):
        chunk = data_list[i:i + chunk_size]
        result = await upsert_many_bulk(model, chunk, unique_fields)
        added += result[0]
        updated += result[1]

    return added, updated

async def delete_records(model, *clauses):
    """
    删除符合条件的记录
    """
    async with db.get_async_session() as session:
        try:
            result = await session.execute(
                delete(model).where(*clauses)
            )
            await session.commit()
            return result.rowcount
        except Exception as e:
            await session.rollback()
            raise e

# 也可以保留原有变量名（通过属性懒加载）
async def get_engine():
    return await db.engine

async def get_async_session_maker():
    return await db.async_session_maker

