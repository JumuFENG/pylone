import asyncio
import sys
import os
import aiomysql
import numpy as np
from typing import Dict, List, Tuple, Callable, Optional

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.lofig import Config
from app.db import cfg, engine, Base
from app.stock.history import Khistory as khis
from app.stock.h5 import KLineStorage
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

async def check_table_exists(cursor, dbname: str, tablename: str) -> bool:
    """检查表是否存在"""
    await cursor.execute(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (dbname, tablename)
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

async def migrate_kline_data(
    conn,
    from_dbname: str,
    code: str,
    columns_mapping: Dict[str, str],
    unique_keys: List[int],
    transform_func: Optional[Callable] = None
):
    """迁移 kline 数据的示例函数"""
    kltype_table_map = {
        101: f's_k_his_{code}',
        15: f's_k15_his_{code}',
        102: f's_kw_his_{code}',
        103: f's_km_his_{code}'
    }
    async with conn.cursor() as cursor:
        # 检查源数据库是否存在
        if not await check_database_exists(cursor, from_dbname):
            print(f"源数据库 '{from_dbname}' 不存在，无法进行数据迁移。")
            return

        KLineStorage.delete_dataset('sh688588')
        for kltype, table_name in kltype_table_map.items():
            if not await check_table_exists(cursor, from_dbname, table_name):
                print(f"源表 '{table_name}' 不存在，跳过迁移。")
                continue
            # 查询源数据
            source_columns = ', '.join(columns_mapping.values())
            await cursor.execute(f"SELECT {source_columns} FROM `{from_dbname}`.`{table_name}`")
            rows = await cursor.fetchall()

            if not rows:
                print(f"源表 '{table_name}' 中没有数据可迁移。")
                continue

            # 转换和去重数据
            unique_rows = transform_rows(rows, unique_keys, transform_func)

            if not unique_rows:
                print(f"表 '{table_name}' 去重后没有数据可迁移。")
                continue

            dtypes = [(col, 'U20' if col == 'time' else 'float64') for col in columns_mapping.keys()]
            odata = np.array([tuple(r) for r in unique_rows], dtype=dtypes)
            khis.save_kline(code, kltype, odata)

async def migrate_stock_klines(code: str):
    """迁移指定股票的 K 线数据"""
    conn = await get_connection()

    from_dbname = 'history_db'

    columns_mapping = {
        'time': 'date',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'volume': 'volume',
        'amount': 'amount',
        'change': 'p_change',
        'change_px': 'price_change',
    }

    # 定义转换函数：将 time 字段格式化为统一格式
    def transform_func(row):
        time_str = row[0]
        if len(time_str) == 10:
            time_str += ' 00:00:00'
        elif len(time_str) == 16:
            time_str += ':00'
        volume = float(row[5]) * 1e4
        amount = float(row[6]) * 1e4
        change = float(row[7].strip('%'))/100 if row[7].endswith('%') else float(row[7])/100
        return (time_str,) + row[1:5] + (volume, amount, change, row[8])

    try:
        await migrate_kline_data(
            conn=conn,
            from_dbname=from_dbname,
            code=code,
            columns_mapping=columns_mapping,
            unique_keys=[0],  # 使用 time 字段去重
            transform_func=transform_func
        )
    finally:
        conn.close()


async def setup_holidays():
    from datetime import datetime, timedelta
    from app.stock.models import MdlHolidays
    from app.db import insert_many
    trading_dates = [ str(d) for d in KLineStorage.read_kline_data('sh000001')['time']]
    date = datetime.strptime(trading_dates[0], '%Y-%m-%d')
    holidays = []
    while date < datetime.now():
        if date.weekday() < 5 and date.strftime('%Y-%m-%d') not in trading_dates:
            holidays.append(date.strftime('%Y-%m-%d'))
        date += timedelta(days=1)

    await insert_many(MdlHolidays, [{'date': d} for d in holidays] )

if __name__ == '__main__':
    asyncio.run(migrate_stock_klines('sh688588'))
