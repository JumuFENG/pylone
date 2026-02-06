import asyncio
import sys
import os
from sqlalchemy import inspect

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.lofig import Config
from app.db import cfg, engine, Base
from app.users.models import User
from app.stock.models import MdlStockShare
from app.selectors.models import *
from app.users.schemas import UserCreate
from app.users.manager import get_user_db, UserManager


cfg['password'] = Config.simple_decrypt(cfg['password'])


async def check_database():
    """检查数据库是否存在，不存在则创建"""
    if Config.database_type() == 'sqlite':
        await check_sqlite_database()
    else:  # mysql
        await check_mysql_database()


async def check_mysql_database():
    """检查MySQL数据库是否存在，不存在则创建"""
    import aiomysql

    # 连接到 MySQL 服务器（不指定数据库）
    conn = await aiomysql.connect(
        host=cfg['host'],
        port=cfg['port'],
        user=cfg['user'],
        password=cfg['password']
    )

    try:
        async with conn.cursor() as cursor:
            # 检查数据库是否存在
            await cursor.execute(
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s",
                (cfg['dbname'],)
            )
            result = await cursor.fetchone()

            if not result:
                # 数据库不存在，创建它
                await cursor.execute(f"CREATE DATABASE `{cfg['dbname']}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                await conn.commit()
                print(f"MySQL数据库 '{cfg['dbname']}' 创建成功")
            else:
                print(f"MySQL数据库 '{cfg['dbname']}' 已存在")
    finally:
        conn.close()


async def check_sqlite_database():
    """检查SQLite数据库，确保目录存在"""
    db_dir = Config.h5_history_dir()
    db_path = f"{db_dir}/{Config.database_config().get('dbname', 'pylone')}.db"

    # 确保目录存在
    if not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
        print(f"创建SQLite数据库目录: {db_dir}")

    # SQLite数据库文件会在首次连接时自动创建
    if os.path.exists(db_path):
        print(f"SQLite数据库 '{db_path}' 已存在")
    else:
        print(f"SQLite数据库 '{db_path}' 将在首次连接时创建")

async def create_admin():
    """创建管理员用户"""

    admin_data = {
        "username": "admin",
        "email": "admin@admin.com",
        "password": "admin123456",
        "is_superuser": True,
        "is_active": True,
        "is_verified": True
    }

    async for user_db in get_user_db():
        # 检查管理员是否已存在
        existing_user = await user_db.get_by_email(admin_data["email"])

        if existing_user:
            print(f"管理员用户 '{admin_data['username']}' 已存在")
        else:
            # 使用 UserManager 创建管理员用户
            user_manager = UserManager(user_db)
            user_create = UserCreate(**admin_data)
            await user_manager.create(user_create)
            print(f"管理员用户 '{admin_data['username']}' 创建成功")
        break


async def create_single_table(table_name):
    async with engine.begin() as conn:
        table = Base.metadata.tables.get(table_name)
        if table is None:
            print(f"表 '{table_name}' 在元数据中未找到，跳过")
            return

        exists = await conn.run_sync(lambda sync_conn: inspect(sync_conn).has_table(table.name))
        if exists:
            await conn.run_sync(lambda sync_conn: table.drop(bind=sync_conn))
            # print(f"表 '{table_name}' 已存在，跳过创建")
            # return

        await conn.run_sync(lambda sync_conn: table.create(bind=sync_conn))

async def main():
    await check_database()

    for table_name in ['ucostdog_urque']:
        await create_single_table(table_name)

    # Create all tables for SQLite
    if Config.database_type() == 'sqlite':
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            print("所有表创建完成")

    await create_admin()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
