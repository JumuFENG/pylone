import asyncio
import numpy as np
from typing import Optional
from datetime import datetime
from traceback import format_exc
from app.lofig import logger
from app.stock.models import MdlAllStock
from app.stock.date import TradingDate
from app.stock.history import Khistory
from app.stock.schemas import KNode
from app.db import query_values, query_aggregate, insert_many, or_, array_to_dict_list


class StockBaseSelector():
    def __init__(self, max_workers: int = 2) -> None:
        self.max_workers = max_workers
        self.wkstocks = []
        self.wkselected = []

    @property
    def db(self):
        pass

    async def task_prepare(self, date: Optional[str] = None) -> None:
        """准备任务"""
        if date is None:
            self.wkstocks = await query_values(
                MdlAllStock, [MdlAllStock.code, MdlAllStock.setup_date],
                or_(MdlAllStock.typekind == 'ABStock',MdlAllStock.typekind == 'TSStock', MdlAllStock.typekind == 'BJStock'))
            self.wkstocks = [[c, d] for c, d in self.wkstocks]
        else:
            stks = await query_values(MdlAllStock, [MdlAllStock.code], or_(MdlAllStock.typekind == 'ABStock', MdlAllStock.typekind == 'BJStock'))
            self.wkstocks = [[c, date] for c, in stks]
        self.wkselected = []

    async def post_process(self) -> None:
        """后处理"""
        if len(self.wkselected) > 0:
            uniq_fields = []
            if hasattr(self.db, 'code'):
                uniq_fields.append('code')
            if hasattr(self.db, 'time'):
                uniq_fields.append('time')
            elif hasattr(self.db, 'date'):
                uniq_fields.append('date')
            await insert_many(self.db, array_to_dict_list(self.db, self.wkselected), uniq_fields)

    async def task_processing(self, item) -> None:
        """任务处理逻辑"""
        pass

    async def start_multi_task(self, date: Optional[str] = None) -> None:
        """
        启动异步任务

        Args:
            date: 可选日期参数
        """
        # 准备阶段
        await self.task_prepare(date)

        ctime = datetime.now()

        if self.max_workers <= 1:
            for item in self.wkstocks:
                await self.task_processing(item)
        else:
            q = asyncio.Queue()
            for item in self.wkstocks:
                await q.put(item)

            async def _worker():
                while True:
                    try:
                        item = await q.get()
                    except asyncio.CancelledError:
                        break
                    try:
                        await self.task_processing(item)
                    except Exception:
                        logger.error('error in task_processing %s', format_exc())
                    finally:
                        q.task_done()

            workers = [asyncio.create_task(_worker()) for _ in range(self.max_workers)]
            await q.join()
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        # 记录执行时间
        elapsed = datetime.now() - ctime
        logger.info(f'异步任务完成，工作线程数: {self.max_workers}，耗时: {elapsed}')

        # 后处理
        await self.post_process()

    async def update_pickups(self):
        if getattr(self.db, 'time', None) is not None:
            mdate = await query_aggregate('max', self.db, 'time')
        else:
            mdate = await query_aggregate('max', self.db, 'date')
        if mdate == TradingDate.max_trading_date():
            logger.info('%s update_pickups already updated to latest!', self.__class__.__name__)
            return
        await self.start_multi_task(mdate)

    async def get_kd_data(self, code:str, start:str, fqt:int=0):
        if not TradingDate.is_trading_date(start):
            start = TradingDate.next_trading_date(start)
        kd = await Khistory.read_kline(code, 'd', start=start, fqt=fqt)
        if kd is None or len(kd) == 0:
            return None
        def safe_get(record, field_name, default=0.0):
            if field_name in record.dtype.names:
                value = record[field_name]
                if value is None or (isinstance(value, float) and np.isnan(value)):
                    return default
                return float(value)
            return default

        return [KNode(
            time=kl['time'],
            open=kl['open'],
            close=kl['close'],
            high=kl['high'],
            low=kl['low'],
            volume=safe_get(kl, 'volume', 0),
            amount=safe_get(kl, 'amount', 0),
            change=safe_get(kl, 'change', 0),
            change_px=safe_get(kl, 'change_px', 0),
            amplitude=safe_get(kl, 'amplitude', 0),
            turnover=safe_get(kl, 'turnover', 0)
        ) for kl in kd]
