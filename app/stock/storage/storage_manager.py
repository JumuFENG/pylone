from app.lofig import logger
import importlib.util

if importlib.util.find_spec("h5py") is None:
    class DataSyncManager(object):
        def __getattribute__(self, name):
            """重写所有属性/方法访问"""
            attr = object.__getattribute__(self, name)
            return attr

        def __getattr__(self, name):
            """只捕获方法调用"""
            def method(*args, **kwargs):
                logger.warning(f"H5和SQLite之间的数据同步需安装h5py")
                return None
            return method

else:
    from traceback import format_exc
    from typing import List, Dict, Any, Optional, Union
    from datetime import datetime, timedelta
    from .sqlite import KLineSQLiteStorage, FflowSQLiteStorage, TransactionSQLiteStorage
    from .h5 import KLineStorage, FflowStorage, TransactionStorage
    import stockrt as srt


    class DataSyncManager:
        """数据同步管理器，用于H5和SQLite之间的数据同步"""

        def __init__(self):
            # 创建适配器实例
            self.sqlite_kline = KLineSQLiteStorage()
            self.sqlite_fflow = FflowSQLiteStorage()
            self.sqlite_trans = TransactionSQLiteStorage()

            # 创建H5存储实例用于读取
            self.h5_kline = KLineStorage()
            self.h5_fflow = FflowStorage()
            self.h5_trans = TransactionStorage()

        async def h5_to_sqlite_klines(self, fcode: str, kline_type: int = 101, limit: int = 100) -> int:
            """
            将H5中的K线数据同步到SQLite

            Args:
                fcode: 股票代码
                kline_type: K线类型
                limit: 同步数量限制，默认100条

            Returns:
                同步的记录数
            """
            try:
                # 获取SQLite中的最新时间
                sqlite_latest = await self.sqlite_kline.get_latest_time(fcode, kline_type)

                # 从H5读取数据
                if sqlite_latest:
                    # 读取比SQLite更新的数据
                    h5_data = self.h5_kline.read_kline_data(fcode, kline_type, 0)
                    if h5_data is None:
                        return 0

                    # 转换为时间列表进行比较
                    new_data = [row for row in h5_data if row['time'] > sqlite_latest]

                    # 限制同步数量
                    if limit > 0:
                        new_data = new_data[-limit:] if len(new_data) > limit else new_data

                    if new_data:
                        saved_count = await self.sqlite_kline.save_kline_data(fcode, new_data, kline_type)
                        logger.debug(f"H5→SQLite K线同步 {fcode} {kline_type}: {saved_count}条记录")
                        return saved_count
                else:
                    # SQLite中没有数据，读取最新的limit条
                    h5_data = self.h5_kline.read_kline_data(fcode, kline_type, limit)
                    if h5_data is None:
                        return 0

                    saved_count = await self.sqlite_kline.save_kline_data(fcode, h5_data, kline_type)
                    logger.debug(f"H5→SQLite K线同步 {fcode} {kline_type}: {saved_count}条记录")
                    return saved_count

                return 0

            except Exception as e:
                logger.error(f"H5→SQLite K线同步失败 {fcode} {kline_type}: {str(e)}")
                logger.debug(format_exc())
                return 0

        async def h5_to_sqlite_fflow(self, fcode: str, limit: int = 100) -> int:
            """
            将H5中的资金流数据同步到SQLite

            Args:
                fcode: 股票代码
                limit: 同步数量限制，默认100条

            Returns:
                同步的记录数
            """
            try:
                # 获取SQLite中的最新时间
                sqlite_latest = await self.sqlite_fflow.get_latest_time(fcode)

                # 从H5读取数据
                h5_data = self.h5_fflow.read_fflow(fcode)
                if not h5_data:
                    return 0

                if sqlite_latest:
                    # 读取比SQLite更新的数据
                    new_data = [row for row in h5_data if row['time'] > sqlite_latest]

                    # 限制同步数量
                    if limit > 0:
                        new_data = new_data[-limit:] if len(new_data) > limit else new_data
                else:
                    # SQLite中没有数据，读取最新的limit条
                    new_data = h5_data[-limit:] if limit > 0 and len(h5_data) > limit else h5_data

                if new_data:
                    saved_count = await self.sqlite_fflow.save_fflow(fcode, new_data)
                    logger.debug(f"H5→SQLite 资金流同步 {fcode}: {saved_count}条记录")
                    return saved_count

                return 0

            except Exception as e:
                logger.error(f"H5→SQLite 资金流同步失败 {fcode}: {str(e)}")
                logger.debug(format_exc())
                return 0

        async def h5_to_sqlite_transactions(self, fcode: str, limit: int = 10) -> int:
            """
            将H5中的交易数据同步到SQLite

            Args:
                fcode: 股票代码
                limit: 同步天数数量限制，默认10天

            Returns:
                同步的记录数
            """
            try:
                # 获取SQLite中的最新时间
                sqlite_latest = await self.sqlite_trans.get_latest_time(fcode)

                # 从H5读取数据
                h5_data = self.h5_trans.read_transaction(fcode, limit=0)
                if h5_data is None:
                    return 0
                if sqlite_latest:
                    # 读取比SQLite更新的数据
                    new_data = [row for row in h5_data if row['time'] > sqlite_latest]

                    # 限制同步数量
                    if limit > 0:
                        start_time = datetime.strftime(datetime.strptime(new_data[-1]['time'].split()[0], '%Y-%m-%d') - timedelta(days=limit), '%Y-%m-%d')
                        new_data = [row for row in new_data if row['time'] >= start_time]
                else:
                    # SQLite中没有数据，读取最新的limit条
                    if limit > 0:
                        start_time = datetime.strftime(datetime.strptime(h5_data[-1]['time'].split()[0], '%Y-%m-%d') - timedelta(days=limit), '%Y-%m-%d')
                        new_data = [row for row in h5_data if row['time'] >= start_time]
                    else:
                        new_data = h5_data

                if new_data:
                    saved_count = await self.sqlite_trans.save_transaction(fcode, new_data)
                    logger.debug(f"H5→SQLite 交易数据同步 {fcode}: {saved_count}条记录")
                    return saved_count

                return 0

            except Exception as e:
                logger.error(f"H5→SQLite 交易数据同步失败 {fcode}: {str(e)}")
                logger.debug(format_exc())
                return 0

        async def sqlite_to_h5_klines(self, fcode: str, kline_type: int = 101, limit: int = 100) -> int:
            """
            将SQLite中的K线数据同步到H5，并清理旧数据

            Args:
                fcode: 股票代码
                kline_type: K线类型

            Returns:
                同步的记录数
            """
            try:
                # 获取H5中的最新时间
                h5_latest = self.h5_kline.max_date(fcode, kline_type)

                # 从SQLite读取数据
                if h5_latest:
                    sqlite_data = await self.sqlite_kline.read_kline_data_by_date_range(fcode, kline_type, start_date=h5_latest)
                    new_data = [row for row in sqlite_data if row['time'] > h5_latest]
                else:
                    sqlite_data = await self.sqlite_kline.read_kline_data(fcode, kline_type)
                    new_data = sqlite_data

                if new_data:
                    # 保存到H5
                    self.h5_kline.save_dataset(fcode, new_data, kline_type)
                    saved_count = len(new_data)

                    # 清理SQLite中的旧数据
                    await self.sqlite_kline.cleanup_old_data_by_days(fcode, kline_type, limit)

                    logger.debug(f"SQLite→H5 K线同步 {fcode} {kline_type}: {saved_count}条记录")
                    return saved_count

                return 0

            except Exception as e:
                logger.error(f"SQLite→H5 K线同步失败 {fcode} {kline_type}: {str(e)}")
                logger.debug(format_exc())
                return 0

        async def sqlite_to_h5_fflow(self, fcode: str, limit: int = 100) -> int:
            """
            将SQLite中的资金流数据同步到H5，并清理旧数据

            Args:
                fcode: 股票代码

            Returns:
                同步的记录数
            """
            try:
                # 获取H5中的最新时间
                h5_latest = self.h5_fflow.max_date(fcode)

                # 从SQLite读取数据
                if h5_latest:
                    sqlite_data = await self.sqlite_fflow.read_fflow(fcode, start_date=h5_latest)
                    new_data = [row for row in sqlite_data if row['time'] > h5_latest]
                else:
                    sqlite_data = await self.sqlite_fflow.read_fflow(fcode)
                    new_data = sqlite_data

                if new_data:
                    # 保存到H5
                    self.h5_fflow.save_dataset(fcode, new_data)
                    saved_count = len(new_data)

                    # 清理SQLite中的旧数据
                    await self.sqlite_fflow.cleanup_old_data_by_days(fcode, max_days=limit)

                    logger.debug(f"SQLite→H5 资金流同步 {fcode}: {saved_count}条记录")
                    return saved_count

                return 0

            except Exception as e:
                logger.error(f"SQLite→H5 资金流同步失败 {fcode}: {str(e)}")
                logger.debug(format_exc())
                return 0

        async def sqlite_to_h5_transactions(self, fcode: str, limit: int = 10) -> int:
            """
            将SQLite中的交易数据同步到H5，并清理旧数据

            Args:
                fcode: 股票代码

            Returns:
                同步的记录数
            """
            try:
                # 获取H5中的最新时间
                h5_latest = self.h5_trans.max_date(fcode)

                # 从SQLite读取数据
                if h5_latest:
                    sqlite_data = await self.sqlite_trans.read_transaction(fcode, start_time=h5_latest)
                    new_data = [row for row in sqlite_data if row['time'] > h5_latest]
                else:
                    sqlite_data = await self.sqlite_trans.read_transaction(fcode)
                    new_data = sqlite_data

                if new_data:
                    # 保存到H5
                    self.h5_trans.save_dataset(fcode, new_data)
                    saved_count = len(new_data)

                    # 清理SQLite中的旧数据
                    await self.sqlite_trans.cleanup_old_data_by_days(fcode, max_days=limit)

                    logger.debug(f"SQLite→H5 交易数据同步 {fcode}: {saved_count}条记录")
                    return saved_count

                return 0

            except Exception as e:
                logger.error(f"SQLite→H5 交易数据同步失败 {fcode}: {str(e)}")
                logger.debug(format_exc())
                return 0

        async def sync_single_stock(self, fcode: str, direction: str = "h5_to_sqlite",
                                data_types: List[str] = None,
                                kline_types: List[int] = None,
                                limits: Dict[str, int] = None) -> Dict[str, int]:
            """
            同步单个股票的数据

            Args:
                fcode: 股票代码
                direction: 同步方向 ('h5_to_sqlite' 或 'sqlite_to_h5')
                data_types: 数据类型列表 ['klines', 'fflow', 'transactions']
                kline_types: K线类型列表，仅对klines有效
                limits: 各数据类型的同步数量限制

            Returns:
                同步结果统计
            """
            if data_types is None:
                data_types = ['klines', 'fflow', 'transactions']
            if kline_types is None:
                kline_types = [101]  # 默认日线
            if limits is None:
                limits = {'klines': 100, 'fflow': 100, 'transactions': 10}
            results = {}

            fcode = srt.get_fullcode(fcode)
            if direction == "h5_to_sqlite":

                if 'klines' in data_types:
                    kline_count = 0
                    for kline_type in kline_types:
                        count = await self.h5_to_sqlite_klines(fcode, kline_type, limits.get('klines', 100))
                        kline_count += count
                    results['klines'] = kline_count

                if 'fflow' in data_types:
                    count = await self.h5_to_sqlite_fflow(fcode, limits.get('fflow', 100))
                    results['fflow'] = count

                if 'transactions' in data_types:
                    count = await self.h5_to_sqlite_transactions(fcode, limits.get('transactions', 100))
                    results['transactions'] = count

            elif direction == "sqlite_to_h5":
                if 'klines' in data_types:
                    kline_count = 0
                    for kline_type in kline_types:
                        count = await self.sqlite_to_h5_klines(fcode, kline_type, limits.get('klines', 100))
                        kline_count += count
                    results['klines'] = kline_count

                if 'fflow' in data_types:
                    count = await self.sqlite_to_h5_fflow(fcode, limits.get('fflow', 100))
                    results['fflow'] = count

                if 'transactions' in data_types:
                    count = await self.sqlite_to_h5_transactions(fcode, limits.get('transactions', 10))
                    results['transactions'] = count

            return results

        async def sync_sqlite_to_h5(self) -> Dict[str, Dict[str, int]]:
            """
            同步多个股票的数据

            Args:
                fcodes: 股票代码列表

            Returns:
                同步结果统计
            """
            results = {}

            alltbls = await self.sqlite_kline.all_tables()
            for tbl in alltbls:
                _, c, t = tbl.split('_')
                kcnt = await self.sqlite_to_h5_klines(c, int(t), 100 if int(t) == 101 else 10)
                if c not in results:
                    results[c] = {}
                results[c].update({f'klines_{t}': kcnt})

            alltbls = await self.sqlite_fflow.all_tables()
            for tbl in alltbls:
                _, c = tbl.split('_')
                cnt = await self.sqlite_to_h5_fflow(c, 100)
                if c not in results:
                    results[c] = {}
                results[c].update({'fflow': cnt})

            alltbls = await self.sqlite_trans.all_tables()
            for tbl in alltbls:
                _, c = tbl.split('_')
                cnt = await self.sqlite_to_h5_transactions(c, 10)
                if c not in results:
                    results[c] = {}
                results[c].update({'transactions': cnt})

            return results

        async def compact_sqlite(self):
            await self.sqlite_kline.vacuum()
            await self.sqlite_fflow.vacuum()
            await self.sqlite_trans.vacuum()