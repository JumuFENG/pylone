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
from app.users.models import *

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
        if not transformed_row:
            continue

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
    from_table_name: str,
    to_table_name: str,
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
        from_table_name: 表名
        to_table_name: 目标表名
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
        source_columns = ', '.join([c for c in columns_mapping.values() if c])
        await cursor.execute(f"SELECT {source_columns} FROM `{from_dbname}`.`{from_table_name}`")
        rows = await cursor.fetchall()

        if not rows:
            print(f"源表 '{from_table_name}' 中没有数据可迁移。")
            return

        # 转换和去重数据
        unique_rows = transform_rows(rows, unique_keys, transform_func)

        # 构建插入语句
        target_columns = ', '.join(columns_mapping.keys())
        await cursor.execute(f"SELECT {target_columns} FROM `{to_dbname}`.`{to_table_name}`")
        ex_rows = await cursor.fetchall()
        ex_rows = [tuple(r[i] for i in unique_keys) for r in ex_rows]
        unique_rows = [row for row in unique_rows if tuple(row[i] for i in unique_keys) not in ex_rows]
        if not unique_rows:
            print(f"表 '{from_table_name}' 去重后没有数据可迁移。")
            return

        placeholders = ', '.join(['%s'] * len(columns_mapping))
        insert_sql = f"INSERT INTO `{to_dbname}`.`{to_table_name}` ({target_columns}) VALUES ({placeholders})"

        # 批量插入数据
        batch_size = 20000
        for i in range(0, len(unique_rows), batch_size):
            batch_rows = unique_rows[i:i + batch_size]
            await cursor.executemany(insert_sql, batch_rows)
            await conn.commit()

        print(f"表 '{from_table_name}' -> '{to_table_name}' 数据迁移完成，共迁移 {len(unique_rows)} 条记录（去重前 {len(rows)} 条）。")


async def migrate_table_as_is(
    from_dbname: str,
    to_dbname: str,
    from_table_name: str,
    to_table_name: str = None,
    unique_keys: List[str] = [],
    skip_id = True
):
    conn = await get_connection()
    async with conn.cursor() as cursor:
        # 检查源数据库是否存在
        if not await check_database_exists(cursor, from_dbname):
            print(f"源数据库 '{from_dbname}' 不存在，无法进行数据迁移。")
            return

        # 查询源数据
        await cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{from_dbname}' AND TABLE_NAME = '{from_table_name}'")
        result = await cursor.fetchall()
        columns = [row[0] for row in result]
        if skip_id and 'id' in columns:
            columns.remove('id')

        source_columns = ', '.join(columns)
        await cursor.execute(f"SELECT {source_columns} FROM `{from_dbname}`.`{from_table_name}`")
        rows = await cursor.fetchall()

        if not rows:
            print(f"源表 '{from_table_name}' 中没有数据可迁移。")
            return

        code_idx = columns.index('code') if 'code' in columns else None
        def transform_func(row):
            if code_idx is not None:
                return row[:code_idx] + (row[code_idx].lower(),) + row[code_idx + 1:]
            else:
                return row
        rows = [transform_func(row) for row in rows]

        # 构建插入语句
        target_columns = source_columns
        placeholders = ', '.join(['%s'] * len(columns))
        if to_table_name is None:
            to_table_name = from_table_name

        if unique_keys:
            update_columns = [f"{column} = VALUES({column})" for column in columns if column not in unique_keys]
            update_clause = ', '.join(update_columns)
            if update_clause:
                insert_sql = f"INSERT INTO `{to_dbname}`.`{to_table_name}` ({target_columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"
            else:
                insert_sql = f"INSERT INTO `{to_dbname}`.`{to_table_name}` ({target_columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {columns[0]} = VALUES({columns[0]})"
        else:
            insert_sql = f"INSERT INTO `{to_dbname}`.`{to_table_name}` ({target_columns}) VALUES ({placeholders})"

        # 批量插入数据
        await cursor.executemany(insert_sql, rows)
        await conn.commit()

        print(f"表 '{from_table_name}' -> '{to_table_name}' 数据迁移完成，共迁移 {len(rows)} 条记录（去重前 {len(rows)} 条）。")

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
            from_table_name=table_name,
            to_table_name=table_name,
            columns_mapping=columns_mapping,
            unique_keys=[0, 1],  # 使用 code 和 report_date 去重
            transform_func=transform_func
        )
    finally:
        conn.close()

async def migrate_stock_bkmap(from_tbl: str = 'stock_bk_map'):
    """迁移 stock_bkmap 表"""
    conn = await get_connection()

    from_dbname = 'stock_center'
    to_dbname = 'pylone'
    table_name = 'stock_bk_map'

    columns_mapping = {
        'bk': 'bk',
        'stock': 'stock'
    }

    # 定义转换函数：将 stock 字段转换为小写
    def transform_func(row):
        return None if row[1].startswith(('HB', 'SB')) else (row[0], row[1].lower(),)

    try:
        await migrate_table(
            conn=conn,
            from_dbname=from_dbname,
            to_dbname=to_dbname,
            from_table_name=from_tbl,
            to_table_name=table_name,
            columns_mapping=columns_mapping,
            unique_keys=[0, 1],  # 使用 code 和 bk_code 去重
            transform_func=transform_func
        )
    finally:
        conn.close()


async def migrate_stock_bks(from_tbl: str = 'stock_embks'):
    """迁移 stock_bks 表"""
    conn = await get_connection()

    from_dbname = 'stock_center'
    to_dbname = 'pylone'
    table_name = 'stock_bks'

    columns_mapping = {
        'code': 'code',
        'name': 'name',
        'chgignore': ''
    }

    def transform_func(row):
        return row[:] + (0,)

    try:
        await migrate_table(
            conn=conn,
            from_dbname=from_dbname,
            to_dbname=to_dbname,
            from_table_name=from_tbl,
            to_table_name=table_name,
            columns_mapping=columns_mapping,
            unique_keys=[0],  # 使用 code 去重
            transform_func=transform_func
        )
    finally:
        conn.close()

async def migrate_change_ignored():
    """迁移 stock_bks 表的 chgignore 字段"""
    conn = await get_connection()

    from_dbname = 'stock_center'
    to_dbname = 'pylone'
    ignore_table = 'stock_embks_chg_ignored'
    table_name = 'stock_bks'

    columns_mapping = {
        'code': 'code'
    }

    try:
        async with conn.cursor() as cursor:
            # 检查源数据库是否存在
            if not await check_database_exists(cursor, from_dbname):
                print(f"源数据库 '{from_dbname}' 不存在，无法进行数据迁移。")
                return

            # 查询源数据
            source_columns = ', '.join(columns_mapping.values())
            await cursor.execute(f"SELECT {source_columns} FROM `{from_dbname}`.`{ignore_table}`")
            rows = await cursor.fetchall()

            if not rows:
                print(f"源表 '{ignore_table}' 中没有数据可迁移。")
                return

            codes_to_ignore = {row[0] for row in rows}

            # 更新目标表的 chgignore 字段
            update_sql = f"UPDATE `{to_dbname}`.`{table_name}` SET chgignore = 1 WHERE code = %s"
            for code in codes_to_ignore:
                await cursor.execute(update_sql, (code,))
            await conn.commit()

            print(f"表 '{table_name}' 的 chgignore 字段更新完成，共更新 {len(codes_to_ignore)} 条记录。")
    finally:
        conn.close()

async def migrate_stock_changes():
    """迁移 stock_changes 表"""
    assert migrate_stock_changes.__name__ != 'migrate_stock_changes', "'migrate_stock_changes' not allowed as takes too long time"
    conn = await get_connection()

    from_dbname = 'history_db'
    to_dbname = 'pylone'
    from_table_name = 'stock_changes_history'
    table_name = 'stock_changes'

    columns_mapping = {
        'code': 'code',
        'time': 'date',
        'chgtype': 'type',
        'info': 'info',
    }

    # 定义转换函数：将 time 字段格式化为统一格式
    def transform_func(row):
        return (row[0].lower(),) + row[1:]

    try:
        await migrate_table(
            conn=conn,
            from_dbname=from_dbname,
            to_dbname=to_dbname,
            from_table_name=from_table_name,
            to_table_name=table_name,
            columns_mapping=columns_mapping,
            unique_keys=[0, 1, 2],  # 使用 code 和 time 去重
            transform_func=transform_func
        )
    finally:
        conn.close()

async def migrate_day_zt_stocks(from_tbl: str = 'day_zt_stocks', mkt=0):
    """迁移 day_zt_stocks 表"""
    conn = await get_connection()

    from_dbname = 'history_db'
    to_dbname = 'pylone'
    table_name = 'day_zt_stocks'

    columns_mapping = {
        'code': 'code',
        'time': 'date',
        'fund': '涨停封单',
        'hsl': '换手率',
        'lbc': '连板数',
        'days': '总天数',
        'zbc': '炸板数',
        'bk': '板块',
        'cpt': '概念',
        'mkt': ''
    }

    # 定义转换函数：将 time 字段格式化为统一格式
    def transform_func(row):
        return (row[0].lower(),row[1],row[2] if row[2] and row[2] != '' else 0,float(row[3])/100) + row[4:8] + (row[8] or '', mkt,)

    try:
        await migrate_table(
            conn=conn,
            from_dbname=from_dbname,
            to_dbname=to_dbname,
            from_table_name=from_tbl,
            to_table_name=table_name,
            columns_mapping=columns_mapping,
            unique_keys=[0, 1],  # 使用 code 和 time 去重
            transform_func=transform_func
        )
    finally:
        conn.close()

async def migrate_day_zt_concepts():
    """迁移 day_zt_concepts 表"""
    conn = await get_connection()

    from_dbname = 'history_db'
    to_dbname = 'pylone'
    table_name = 'day_zt_concepts'

    columns_mapping = {
        'time': 'date',
        'cpt': '概念',
        'ztcnt': '涨停数',
    }

    def transform_func(row):
        return (row[0],row[1],row[2]) if row[2] and row[2] > 1 else None

    try:
        await migrate_table(
            conn=conn,
            from_dbname=from_dbname,
            to_dbname=to_dbname,
            from_table_name=table_name,
            to_table_name=table_name,
            columns_mapping=columns_mapping,
            unique_keys=[0, 1],  # 使用 time 和 cpt 去重
            transform_func=transform_func
        )
    finally:
        conn.close()


async def migrate_day_dt_stocks():
    """迁移 day_dt_stocks 表"""
    conn = await get_connection()

    from_dbname = 'history_db'
    to_dbname = 'pylone'
    table_name = 'day_dt_stocks'

    columns_mapping = {
        'code': 'code',
        'time': 'date',
        'fund': '封单资金',
        'fba': '板上成交额',
        'hsl': '换手率',
        'lbc': '连板数',
        'zbc': '开板数',
        'bk': '板块',
        'mkt': ''
    }

    # 定义转换函数：将 time 字段格式化为统一格式
    def transform_func(row):
        return (row[0].lower(),row[1],row[2] if row[2] and row[2] != '' else 0,row[3],float(row[4])/100) + row[5:8] + (0 if row[0].startswith(('SH60', 'SZ00')) else 2 if row[0].startswith('BJ') else 1,)

    try:
        await migrate_table(
            conn=conn,
            from_dbname=from_dbname,
            to_dbname=to_dbname,
            from_table_name=table_name,
            to_table_name=table_name,
            columns_mapping=columns_mapping,
            unique_keys=[0, 1],  # 使用 code 和 time 去重
            transform_func=transform_func
        )
    finally:
        conn.close()

async def migrate_stock_dt_map():
    """迁移 stock_dt_map 表"""
    conn = await get_connection()

    from_dbname = 'history_db'
    to_dbname = 'pylone'
    table_name = 'day_dt_maps'

    columns_mapping = {
        'time': 'date',
        'code': 'code',
        'step': 'step',
        'success': 'success',
    }

    # 定义转换函数：将 code 字段转换为小写
    def transform_func(row):
        return (row[0], row[1].lower(), row[2], row[3],)

    try:
        await migrate_table(
            conn=conn,
            from_dbname=from_dbname,
            to_dbname=to_dbname,
            from_table_name=table_name,
            to_table_name=table_name,
            columns_mapping=columns_mapping,
            unique_keys=[0, 1],  # 使用 time 和 code 去重
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
        date_str = date.strftime('%Y-%m-%d')
        if date.weekday() < 5 and date_str not in trading_dates and date_str < trading_dates[-1]:
            holidays.append(date_str)
        date += timedelta(days=1)

    await insert_many(MdlHolidays, [{'date': d} for d in holidays] )


async def migrate_user_data():
    tables = [
        'user_stocks', 'user_strategy', 'user_orders', 'user_full_orders', 'user_earned', 'user_earning',
        'user_deals_unknown', 'user_deals_archived', 'user_stock_buy', 'user_stock_sell',
    ]
    table_abc = {
        'user_stocks': {
            'from_table': 'u%s_stocks',
        },
        'user_strategy': {
            'from_table': 'u%s_strategy',
        },
        'user_orders': {
            'from_table': 'u%s_orders',
            'cols': {'time': 'date', 'portion': 'count', 'typebs': 'type'}
        },
        'user_full_orders': {
            'from_table': 'u%s_fullorders',
            'cols': {'time': 'date', 'portion': 'count', 'typebs': 'type'}
        },
        'user_earned': {
            'from_table': 'u%s_earned',
        },
        'user_earning': {
            'from_table': 'u%s_earning',
            'cols': {'amount': '市值'}
        },
        'user_deals_unknown': {
            'from_table': 'u%s_unknown_deals',
            'cols': {'time': 'date', 'typebs': 'type', 'sid': '委托编号', 'fee': '手续费', 'feeYh': '印花税', 'feeGh': '过户费'}
        },
        'user_deals_archived': {
            'from_table': 'u%s_archived_deals',
            'cols': {'time': 'date', 'typebs': 'type', 'sid': '委托编号', 'fee': '手续费', 'feeYh': '印花税', 'feeGh': '过户费'}
        },
        'user_stock_buy': {
            'from_table': 'u%s_buy',
            'cols': {'time': 'date', 'sid': '委托编号', 'fee': '手续费', 'feeYh': '印花税', 'feeGh': '过户费'}
        },
        'user_stock_sell': {
            'from_table': 'u%s_sell',
            'cols': {'time': 'date', 'sid': '委托编号', 'fee': '手续费', 'feeYh': '印花税', 'feeGh': '过户费'}
        },
        # 'user_costdog': {
        #     'from_table': 'u%s_costdog',
        # },
        # 'ucostdog_urque': {
        #     'from_table': 'u%s_cdurque',
        # },
    }
    users = {11: 11, 14: 12, 15:13, 16:14}
    conn = await get_connection()
    from_dbname = 'stock_center'
    to_dbname = 'pylone'

    for tbl, tbl_info in table_abc.items():
        col_mapping = {column.name: column.name for column in Base.metadata.tables.get(tbl).columns}
        pkcols = [c.name for c in Base.metadata.tables.get(tbl).primary_key.columns]
        col_mapping['user_id'] = ''
        if 'cols' in tbl_info:
            for k, v in tbl_info['cols'].items():
                col_mapping[k] = v
        code_idx = None
        cols = list(col_mapping.keys())
        if 'code' in col_mapping:
            code_idx = cols.index('code') - 1
        ukeys = [cols.index(k) for k in pkcols]
        for uid_old, uid_new in users.items():
            from_tbl = tbl_info['from_table'] % uid_old
            def transform_func(row):
                if code_idx is not None:
                    return (uid_new,) + row[:code_idx] + (row[code_idx].lower(),) + row[code_idx + 1:]
                else:
                    return (uid_new,) + row
            await migrate_table(conn, from_dbname, to_dbname, from_tbl, tbl, col_mapping, unique_keys=ukeys, transform_func=transform_func)


async def migrate_selectors():
    good_tables = ['stock_zt_lead_pickup', 'stock_tripple_bull_pickup', 'stock_day_zdtemotion']
    # 'stock_dt3_pickup', 'stock_zt1wb_pickup', 'stock_day_hotstks_retry_zt0'
    sel_tables = ['stock_day_hotstks_open']
    for tbl in sel_tables:
        await migrate_table_as_is('stock_center', 'pylone', tbl, unique_keys=['date', 'code'])

async def migrate_trackdeals():
    import stockrt
    tables = ['track_hsrzt0', 'track_hslead', 'track_zt1wb']
    to_table = 'user_track_deals'
    conn = await get_connection()
    columns_mapping = {
        'code': 'code',
        'time': 'date',
        'typebs': 'type',
        'sid': '委托编号',
        'price': 'price',
        'portion': 'portion',
        'tkey': '',
        'user_id': '',
    }
    for tbl in tables:
        def transform_func(row):
            return (stockrt.get_fullcode(row[0]),) + row[1:] + (tbl, 10)
        await migrate_table(conn, 'stock_center', 'pylone', tbl, to_table, columns_mapping, unique_keys=[0, 1, 2, 3], transform_func=transform_func)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # loop.run_until_complete(migrate_table_as_is('testdb', 'pylone', 'stock_bks', unique_keys=['code']))
    # loop.run_until_complete(migrate_stock_bkmap())
    # loop.run_until_complete(migrate_stock_bkmap('stock_bkcls_map'))
    # loop.run_until_complete(migrate_stock_bks())
    # loop.run_until_complete(migrate_stock_bks('stock_clsbks'))
    # loop.run_until_complete(migrate_change_ignored())
    # loop.run_until_complete(migrate_stock_changes())
    # loop.run_until_complete(migrate_day_zt_stocks('day_zt_stocks', mkt=0))
    # loop.run_until_complete(migrate_day_zt_stocks('day_zt_stocks_kccy', mkt=1))
    # loop.run_until_complete(migrate_day_zt_stocks('day_zt_stocks_bj', mkt=2))
    # loop.run_until_complete(migrate_day_zt_stocks('day_zt_stocks_st', mkt=3))
    # loop.run_until_complete(migrate_day_zt_concepts())
    # loop.run_until_complete(migrate_day_dt_stocks())
    # loop.run_until_complete(migrate_stock_dt_map())
    # loop.run_until_complete(migrate_user_data())
    # loop.run_until_complete(migrate_selectors())
    loop.run_until_complete(migrate_trackdeals())
