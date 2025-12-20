import asyncio
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.lofig import Config
from app.db import cfg, engine, Base
from app.users.models import User
from app.stock.models import MdlStockShare
from app.users.schemas import UserCreate
from app.users.manager import get_user_db, UserManager


cfg['password'] = Config.simple_decrypt(cfg['password'])


async def check_database():
    """检查数据库是否存在，不存在则创建"""
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
                print(f"数据库 '{cfg['dbname']}' 创建成功")
            else:
                print(f"数据库 '{cfg['dbname']}' 已存在")
    finally:
        conn.close()

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
        await conn.run_sync(Base.metadata.tables[table_name].create)

async def main():
    await check_database()

    for table_name in ['holidays']:
        await create_single_table(table_name)

    # async with engine.begin() as conn:
    #     await conn.run_sync(Base.metadata.create_all)

    await create_admin()

    await engine.dispose()

if __name__ == '__main__':
    asyncio.run(main())
