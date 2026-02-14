import os
from typing import Union, List, Dict, Any, Optional
from datetime import datetime, timedelta
from functools import lru_cache
import asyncio
import sqlite3
from sqlalchemy import select, delete, func, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.lofig import Config, logger
from app.stock.storage.models import create_kline_table, create_fflow_table, create_transaction_table
import stockrt as srt


class SQLiteStorage:
    """SQLite存储基类"""

    def __init__(self, db_name: str = None):
        """
        初始化SQLite存储

        Args:
            db_name: 数据库文件名，不带路径和扩展名
        """
        self.db_name = db_name
        self._db_path = None
        self._engine = None
        self._session_maker = None
        self.create_table_func = None

    @property
    def db_path(self) -> str:
        """获取数据库文件路径"""
        if self._db_path is None:
            history_dir = Config.h5_history_dir()
            self._db_path = f"{history_dir}/{self.db_name}.db"
        return self._db_path

    @property
    def engine(self):
        """获取SQLAlchemy异步引擎"""
        if self._engine is None:
            # 确保目录存在
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            database_url = f"sqlite+aiosqlite:///{self.db_path}"
            self._engine = create_async_engine(
                database_url, 
                echo=False,
                connect_args={
                    "check_same_thread": False,
                    "timeout": 20
                }
            )
        return self._engine

    @property
    def sync_engine(self):
        """获取SQLAlchemy同步引擎（用于创建表）"""
        if not hasattr(self, '_sync_engine'):
            database_url = f"sqlite:///{self.db_path}"
            from sqlalchemy import create_engine
            self._sync_engine = create_engine(
                database_url,
                echo=False,
                connect_args={
                    "check_same_thread": False,
                    "timeout": 20
                }
            )
        return self._sync_engine

    @property
    def session_maker(self):
        """获取会话工厂"""
        if self._session_maker is None:
            self._session_maker = sessionmaker(
                self.engine, 
                class_=AsyncSession, 
                expire_on_commit=False
            )
        return self._session_maker

    def get_session(self):
        """获取异步数据库会话"""
        return self.session_maker()

    async def create_table(self, table_name: str):
        """创建表（如果不存在）"""
        if callable(self.create_table_func):
            table = self.create_table_func(table_name)
            table.create(self.sync_engine, checkfirst=True)

    async def insert_data(self, fcode: str, kline_type: int=101, data: List[Dict[str, Any]]=None,
                         conflict_strategy: str = "REPLACE") -> int:
        """
        插入数据到表中

        Args:
            table_name: 表名
            data: 数据列表，每个元素是字典
            conflict_strategy: 冲突处理策略（REPLACE或IGNORE）

        Returns:
            插入的记录数
        """
        if not data:
            return 0

        table_name = self.get_table_name(fcode, kline_type)
        # 确保表存在
        if not await self.table_exists(table_name):
            await self.create_table(table_name)

        columns = list(data[0].keys())
        placeholders = ", ".join([f":{col}" for col in columns])
        columns_str = ", ".join(columns)

        sql = f"INSERT OR {conflict_strategy} INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        async with self.get_session() as session:
            result = await session.execute(text(sql), data)
            await session.commit()
            return result.rowcount

    async def query_data(self, table_name: str, where_clause: str = None,
                        params: dict = None, order_by: str = None,
                        limit: int = None) -> List[Dict[str, Any]]:
        """
        查询数据

        Args:
            table_name: 表名
            where_clause: WHERE条件
            params: 参数字典
            order_by: 排序字段
            limit: 限制数量

        Returns:
            查询结果列表
        """
        sql = f"SELECT * FROM {table_name}"

        if where_clause:
            sql += f" WHERE {where_clause}"

        if not order_by:
            sql += " ORDER BY time DESC"
        else:
            sql += f" ORDER BY {order_by}"

        if limit:
            sql += f" LIMIT {limit}"

        async with self.get_session() as session:
            result = await session.execute(text(sql), params or {})
            rows = result.fetchall()
            if not order_by:
                return [dict(row._mapping) for row in reversed(rows)]
            return [dict(row._mapping) for row in rows]

    async def get_latest_time(self, fcode, kline_type=101, time_column: str = "time") -> Optional[str]:
        """获取表中最新的时间"""
        table_name = self.get_table_name(fcode, kline_type)
        if not await self.table_exists(table_name):
            return None
        sql = f"SELECT MAX({time_column}) as max_time FROM {table_name}"
        async with self.get_session() as session:
            result = await session.execute(text(sql))
            row = result.fetchone()
            return row[0] if row and row[0] else None

    async def get_earliest_time(self, fcode, kline_type=101, time_column: str = "time") -> Optional[str]:
        """获取表中最早的时间"""
        table_name = self.get_table_name(fcode, kline_type)
        if not await self.table_exists(table_name):
            return None
        sql = f"SELECT MIN({time_column}) as min_time FROM {table_name}"
        async with self.get_session() as session:
            result = await session.execute(text(sql))
            row = result.fetchone()
            return row[0] if row and row[0] else None

    async def delete_old_data(self, table_name: str, time_column: str = "time", cutoff_time: str = None) -> int:
        """
        删除旧数据

        Args:
            table_name: 表名
            time_column: 时间列名
            cutoff_time: 截止时间，此时间之前的数据将被删除

        Returns:
            删除的记录数
        """
        if cutoff_time is None:
            return 0

        sql = f"DELETE FROM {table_name} WHERE {time_column} :cutoff_time"
        async with self.get_session() as session:
            result = await session.execute(text(sql), {"cutoff_time": cutoff_time})
            await session.commit()
            return result.rowcount

    async def count_records(self, table_name: str, where_clause: str = None,
                           params: dict = None) -> int:
        """统计记录数"""
        sql = f"SELECT COUNT(*) as count FROM {table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"

        async with self.get_session() as session:
            result = await session.execute(text(sql), params or {})
            row = result.fetchone()
            return row[0] if row else 0

    async def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name"
        async with self.get_session() as session:
            result = await session.execute(text(sql), {"table_name": table_name})
            row = result.fetchone()
            return row is not None

    async def all_tables(self) -> List[str]:
        """获取所有表名"""
        sql = "SELECT name FROM sqlite_master WHERE type='table'"
        async with self.get_session() as session:
            result = await session.execute(text(sql))
            rows = result.fetchall()
            return [row[0] for row in rows]

    def get_table_name(self, *args) -> str:
        """生成表名的抽象方法，由子类实现"""
        raise NotImplementedError("子类必须实现 get_table_name 方法")

    async def cleanup_old_data_by_days(self, fcode: str, kline_type: int = 101, max_days: int=100,
                                     keep_ratio: float = 0.5) -> int:
        """
        按天数清理旧数据，保留最新数据的一部分

        Args:
            table_name: 表名
            max_days: 最大保留天数
            keep_ratio: 保留比例（默认0.5，即保留一半）

        Returns:
            删除的记录数
        """
        table_name = self.get_table_name(fcode, kline_type)
        max_days = int(max_days * keep_ratio)
        sql = f"SELECT time FROM {table_name}"
        async with self.get_session() as session:
            result = await session.execute(text(sql))
            rows = result.fetchall()
            if not rows:
                return 0

            dates = [r if ' ' not in r else r.split(' ')[0] for r, in rows]
            dates = list(set(dates))
            dates.sort()

            if len(dates) > max_days:
                oldest_keep_time = dates[-max_days]
                delete_sql = f"DELETE FROM {table_name} WHERE time < :oldest_keep_time"
                delete_result = await session.execute(text(delete_sql), {"oldest_keep_time": oldest_keep_time})
                await session.commit()
                logger.warning(f"清理表 {table_name}: 删除了 {delete_result.rowcount} 条记录，保留 {len(rows) - delete_result.rowcount} 条旧记录")
                return delete_result.rowcount

            return 0

    async def vacuum(self) -> bool:
        """
        Run SQLite VACUUM to rebuild and compact the database file.

        Returns:
            True if vacuum succeeded, False otherwise.
        """
        # Ensure DB file exists before attempting vacuum
        if not os.path.exists(self.db_path):
            logger.warning(f"Database file not found, skipping VACUUM: {self.db_path}")
            return False

        try:
            async with self.get_session() as session:
                await session.execute(text("VACUUM"))
                await session.commit()
            return True
        except Exception as e:
            logger.error(f"VACUUM failed for {self.db_path}: {e}")
            return False


class KLineSQLiteStorage(SQLiteStorage):
    """K线数据SQLite存储类"""

    def __init__(self):
        super().__init__("klines")
        self.create_table_func = create_kline_table

        # K线数据类型定义
        self.saved_dtype = {
            'time': 'str',           # 时间字符串
            'open': 'float',         # 开盘价
            'close': 'float',        # 收盘价
            'high': 'float',         # 最高价
            'low': 'float',          # 最低价
            'volume': 'int',         # 成交量
            'amount': 'float',       # 成交额
            'change': 'float',       # 涨跌额
            'change_px': 'float',     # 涨跌幅
            'amplitude': 'float',    # 振幅
            'turnover': 'float'      # 换手率
        }

        # 支持的K线类型
        self.saved_kline_types = [1, 5, 15, 101, 102, 103, 104, 105, 106]

    def get_table_name(self, fcode: str, kline_type: int) -> str:
        """生成K线数据表名"""
        return f"klines_{fcode}_{kline_type}"

    async def save_kline_data(self, fcode: str, data: List[Dict[str, Any]], kline_type: int = 101) -> int:
        """
        保存K线数据

        Args:
            fcode: 股票代码
            data: K线数据列表
            kline_type: K线类型

        Returns:
            保存的记录数
        """
        kline_type = srt.to_int_kltype(kline_type)
        if kline_type not in self.saved_kline_types:
            logger.error(f'不支持的K线类型 {kline_type}')
            return 0

        if not data:
            return 0

        prepared_data = []

        for item in data:
            row = {}
            for col_name in self.saved_dtype.keys():
                if col_name in item:
                    row[col_name] = item[col_name]
                else:
                    # 设置默认值
                    if col_name in ['open', 'close', 'high', 'low', 'change', 'change_px', 'amplitude', 'turnover']:
                        row[col_name] = 0.0
                    elif col_name in ['volume', 'amount']:
                        row[col_name] = 0
                    else:
                        row[col_name] = None
            prepared_data.append(row)

        return await self.insert_data(fcode, kline_type, prepared_data)

    async def read_kline_data(self, fcode: str, kline_type: int = 101, length: int = 0) -> List[Dict[str, Any]]:
        """
        读取K线数据

        Args:
            fcode: 股票代码
            kline_type: K线类型
            length: 读取长度，0表示全部

        Returns:
            K线数据列表
        """
        kline_type = srt.to_int_kltype(kline_type)
        if kline_type not in self.saved_kline_types:
            logger.error(f'不支持的K线类型 {kline_type}')
            return []

        table_name = self.get_table_name(fcode, kline_type)

        if not await self.table_exists(table_name):
            return []

        if length > 0:
            return await self.query_data(table_name, limit=length)
        else:
            return await self.query_data(table_name)

    async def read_kline_data_by_date_range(
            self, fcode: str, kline_type: int = 101,
            start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        """
        按日期范围读取K线数据

        Args:
            fcode: 股票代码
            kline_type: K线类型
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            K线数据列表
        """
        kline_type = srt.to_int_kltype(kline_type)
        table_name = self.get_table_name(fcode, kline_type)

        if not await self.table_exists(table_name):
            return []

        where_clause = ""
        params = {}

        if start_date:
            where_clause += "time >= :start_date"
            params["start_date"] = start_date

        if end_date:
            if where_clause:
                where_clause += " AND time <= :end_date"
            else:
                where_clause += "time <= :end_date"
            params["end_date"] = end_date

        if where_clause:
            return await self.query_data(table_name, where_clause, params)
        else:
            return await self.query_data(table_name)

    async def delete_kline_data(self, fcode: str, kline_type: int = 101) -> int:
        """删除K线数据"""
        kline_type = srt.to_int_kltype(kline_type)
        table_name = self.get_table_name(fcode, kline_type)
        async with self.get_session() as session:
            result = await session.execute(text(f"DELETE FROM {table_name}"))
            await session.commit()
            return result.rowcount


class FflowSQLiteStorage(SQLiteStorage):
    """资金流数据SQLite存储类"""

    def __init__(self):
        super().__init__("fflow")
        self.create_table_func = create_fflow_table

        # 资金流数据类型定义
        self.saved_dtype = {
            'time': 'str',      # 时间字符串
            'main': 'int',      # 主力净流入
            'small': 'int',     # 小单净流入
            'middle': 'int',    # 中单净流入
            'big': 'int',       # 大单净流入
            'super': 'int',     # 超大单净流入
            'mainp': 'float',   # 主力净流入占比
            'smallp': 'float',  # 小单净流入占比
            'middlep': 'float', # 中单净流入占比
            'bigp': 'float',    # 大单净流入占比
            'superp': 'float'   # 超大单净流入占比
        }

        # 支持的K线类型
        self.saved_kline_types = [101]

    def get_table_name(self, fcode: str, *args) -> str:
        """生成资金流数据表名"""
        return f"fflow_{fcode}"

    async def save_fflow(self, fcode: str, data: List[Dict[str, Any]]) -> int:
        """
        保存资金流数据

        Args:
            fcode: 股票代码
            data: 资金流数据列表

        Returns:
            保存的记录数
        """
        if not data:
            return 0

        # 准备数据
        prepared_data = []

        for item in data:
            row = {}
            for col_name in self.saved_dtype.keys():
                if col_name in item:
                    row[col_name] = item[col_name]
                else:
                    # 设置默认值
                    if col_name in ['main', 'small', 'middle', 'big', 'super']:
                        row[col_name] = 0
                    elif col_name.endswith('p'):  # 占比字段
                        row[col_name] = 0.0
                    else:
                        row[col_name] = None
            prepared_data.append(row)

        return await self.insert_data(fcode, data=prepared_data)

    async def read_fflow(self, fcode: str, start_date: str = None,
                             end_date: str = None) -> List[Dict[str, Any]]:
        """
        读取资金流数据

        Args:
            fcode: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            资金流数据列表
        """
        table_name = self.get_table_name(fcode)

        if not await self.table_exists(table_name):
            return []

        where_clause = ""
        params = {}

        if start_date:
            where_clause += "time >= :start_date"
            params["start_date"] = start_date

        if end_date:
            if where_clause:
                where_clause += " AND time <= :end_date"
            else:
                where_clause += "time <= :end_date"
            params["end_date"] = end_date

        if where_clause:
            return await self.query_data(table_name, where_clause, params)
        else:
            return await self.query_data(table_name)

    async def delete_fflow_data(self, fcode: str) -> int:
        """删除资金流数据"""
        table_name = self.get_table_name(fcode)
        async with self.get_session() as session:
            result = await session.execute(text(f"DELETE FROM {table_name}"))
            await session.commit()
            return result.rowcount


class TransactionSQLiteStorage(SQLiteStorage):
    """交易数据SQLite存储类"""

    def __init__(self):
        super().__init__("trans")
        self.create_table_func = create_transaction_table

        # 交易数据类型定义
        self.saved_dtype = {
            'time': 'str',      # 成交时间
            'price': 'float',    # 成交价
            'volume': 'int',     # 成交量
            'num': 'int',       # 成交笔数
            'bs': 'int'         # 买卖方向：1:buy, 2:sell, 0:中性/不明, 8:集合竞价
        }

    def get_table_name(self, fcode: str, *args) -> str:
        """生成交易数据表名（使用完整股票代码）"""
        return f"trans_{fcode}"

    async def save_transaction(self, fcode: str, data: List[Dict[str, Any]]) -> int:
        """
        保存交易数据

        Args:
            fcode: 股票代码
            data: 交易数据列表

        Returns:
            保存的记录数
        """
        if not data:
            return 0

        prepared_data = []

        for item in data:
            row = {}
            for col_name in self.saved_dtype.keys():
                if col_name in item:
                    row[col_name] = item[col_name]
                else:
                    # 设置默认值
                    if col_name in ['volume', 'num', 'bs']:
                        row[col_name] = 0
                    elif col_name == 'price':
                        row[col_name] = 0.0
                    else:
                        row[col_name] = None
            prepared_data.append(row)

        # 交易数据使用INSERT IGNORE策略，避免重复
        return await self.insert_data(fcode, data=prepared_data, conflict_strategy="IGNORE")

    async def read_transaction(self, fcode: str, start_time: str = None,
                                 end_time: str = None, limit: int = None) -> List[Dict[str, Any]]:
        """
        读取交易数据

        Args:
            fcode: 股票代码
            start_time: 开始时间
            end_time: 结束时间
            limit: 限制数量

        Returns:
            交易数据列表
        """
        table_name = self.get_table_name(fcode)

        if not await self.table_exists(table_name):
            return []

        where_clause = ""
        params = {}

        if start_time:
            where_clause += "time >= :start_time"
            params["start_time"] = start_time

        if end_time:
            if where_clause:
                where_clause += " AND time <= :end_time"
            else:
                where_clause += "time <= :end_time"
            params["end_time"] = end_time

        if where_clause:
            return await self.query_data(table_name, where_clause, params, limit=limit)
        else:
            return await self.query_data(table_name, limit=limit)

    async def delete_transaction_data(self, fcode: str) -> int:
        """删除交易数据"""
        table_name = self.get_table_name(fcode)
        async with self.get_session() as session:
            result = await session.execute(text(f"DELETE FROM {table_name}"))
            await session.commit()
            return result.rowcount

    async def get_transaction_count(self, fcode: str, start_time: str = None,
                                 end_time: str = None) -> int:
        """统计交易记录数"""
        table_name = self.get_table_name(fcode)

        if not await self.table_exists(table_name):
            return 0

        where_clause = ""
        params = {}

        if start_time:
            where_clause += "time >= :start_time"
            params["start_time"] = start_time

        if end_time:
            if where_clause:
                where_clause += " AND time <= :end_time"
            else:
                where_clause += "time <= :end_time"
            params["end_time"] = end_time

        return await self.count_records(table_name, where_clause, params)


kls = KLineSQLiteStorage()
fls = FflowSQLiteStorage()
tss = TransactionSQLiteStorage()
