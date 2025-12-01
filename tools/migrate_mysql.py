import asyncio
import sys
import os
import aiomysql
from typing import Dict, List, Tuple, Callable, Optional

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.lofig import Config
from app.db import cfg, engine, Base
cfg['password'] = Config.simple_decrypt(cfg['password'])


async def get_connection():
    """创建数据库连接"""
    return await aiomysql.connect(
        host=cfg['host'],
        port=cfg['port'],
        user=cfg['user'],
        password=cfg['password']
    )


async def check_database_exists(cursor, dbname: str) -> bool:
    """检查数据库是否存在"""
    await cursor.execute(
        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s",
        (dbname,)
    )
    result = await cursor.fetchone()
    return result is not None


def transform_rows(rows: List[Tuple], unique_keys: List[int], transform_func: Optional[Callable] = None) -> List[Tuple]:
    """
    转换和去重数据行

    Args:
        rows: 原始数据行列表
        unique_keys: 用于去重的字段索引列表，如 [0, 1] 表示使用第0和第1个字段
        transform_func: 可选的转换函数，用于转换每一行数据

    Returns:
        去重后的数据行列表
    """
    seen = set()
    unique_rows = []

    for r in rows:
        # 应用转换函数（如果提供）
        transformed_row = transform_func(r) if transform_func else r

        # 构建唯一键
        key = tuple(transformed_row[i] for i in unique_keys)

        if key not in seen:
            seen.add(key)
            unique_rows.append(transformed_row)

    return unique_rows


async def migrate_table(
    conn,
    from_dbname: str,
    to_dbname: str,
    table_name: str,
    columns_mapping: Dict[str, str],
    unique_keys: List[int],
    transform_func: Optional[Callable] = None
):
    """
    通用的表迁移函数

    Args:
        conn: 数据库连接
        from_dbname: 源数据库名
        to_dbname: 目标数据库名
        table_name: 表名
        columns_mapping: 列映射字典，格式为 {目标列名: 源列名}
        unique_keys: 用于去重的字段索引列表
        transform_func: 可选的行转换函数
    """
    async with conn.cursor() as cursor:
        # 检查源数据库是否存在
        if not await check_database_exists(cursor, from_dbname):
            print(f"源数据库 '{from_dbname}' 不存在，无法进行数据迁移。")
            return

        # 查询源数据
        source_columns = ', '.join(columns_mapping.values())
        await cursor.execute(f"SELECT {source_columns} FROM `{from_dbname}`.`{table_name}`")
        rows = await cursor.fetchall()

        if not rows:
            print(f"源表 '{table_name}' 中没有数据可迁移。")
            return

        # 转换和去重数据
        unique_rows = transform_rows(rows, unique_keys, transform_func)

        if not unique_rows:
            print(f"表 '{table_name}' 去重后没有数据可迁移。")
            return

        # 构建插入语句
        target_columns = ', '.join(columns_mapping.keys())
        placeholders = ', '.join(['%s'] * len(columns_mapping))
        insert_sql = f"INSERT INTO `{to_dbname}`.`{table_name}` ({target_columns}) VALUES ({placeholders})"

        # 批量插入数据
        await cursor.executemany(insert_sql, unique_rows)
        await conn.commit()

        print(f"表 '{table_name}' 数据迁移完成，共迁移 {len(unique_rows)} 条记录（去重前 {len(rows)} 条）。")


async def migrate_stock_bonus_shares():
    """迁移 stock_bonus_shares 表"""
    conn = await get_connection()

    from_dbname = 'history_db'
    to_dbname = cfg['dbname']
    table_name = 'stock_bonus_shares'

    columns_mapping = {
        'code': 'code',
        'report_date': '报告日期',
        'register_date': '登记日期',
        'ex_dividend_date': '除权除息日期',
        'progress': '进度',
        'total_bonus': '总送转',
        'bonus_share': '送股',
        'transfer_share': '转股',
        'cash_dividend': '派息',
        'dividend_yield': '股息率',
        'eps': '每股收益',
        'bvps': '每股净资产',
        'total_shares': '总股本',
        'bonus_details': '分红送配详情'
    }

    # 定义转换函数：将 code 字段转换为小写
    def transform_func(row):
        return (row[0].lower(),) + row[1:]

    try:
        await migrate_table(
            conn=conn,
            from_dbname=from_dbname,
            to_dbname=to_dbname,
            table_name=table_name,
            columns_mapping=columns_mapping,
            unique_keys=[0, 1],  # 使用 code 和 report_date 去重
            transform_func=transform_func
        )
    finally:
        conn.close()

if __name__ == '__main__':
    asyncio.run(migrate_stock_bonus_shares())
