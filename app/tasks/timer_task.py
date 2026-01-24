# Python 3
# -*- coding:utf-8 -*-
import asyncio
import inspect
from datetime import datetime
from traceback import format_exc
from typing import Optional, List, Callable, Union, Tuple
from app.lofig import Config, logging
from app.hu import delay_seconds
from app.admin.system_settings import SystemSettings
from app.stock.date import TradingDate
from app.stock.manager import AllBlocks, StockMarketStats
from .trade_opening import bk_changes_prepare_task
from .trade_closed import (
    update_daily_trade_closed_history,
    save_earning_task, update_stock_transactions,
    update_bkchanges_history)
from .daily_update import DailyUpdater as du
from .weekly_update import WeeklyUpdater as wu
from .monthly_update import MonthlyUpdater as mu


logger = logging.getLogger(f'{Config.app_name}.{__package__}')


class TimerTask:
    """异步定时任务类，支持异步任务调度"""
    def __init__(self, interval: float,
        btime: str,
        etime: str,
        brks: Optional[List[Union[str, Tuple[str, str]]]] = None,
        function: Optional[Callable] = None,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None):
        """
        初始化定时任务

        Args:
            interval: 执行间隔(秒)
            btime: 开始时间字符串，格式如 "09:30"
            etime: 结束时间字符串，格式如 "15:00"
            brks: 休息时间列表，可以是字符串列表或元组列表
                  字符串格式: "11:30" 表示休息时间点
                  元组格式: ("11:30", "13:00") 表示休息时间段
            function: 要执行的函数（如果不提供，则需子类覆盖execute_task方法）
            args: 函数位置参数
            kwargs: 函数关键字参数
        """
        self.interval = interval
        self.btime = btime
        self.etime = etime
        self.brk_times = self._parse_break_times(brks or [])
        self.function = function
        self.args = args or ()
        self.kwargs = kwargs or {}
        self._task = None
        self._cancelled = False

    def _parse_break_times(self, brks: List) -> List[Tuple[str, str]]:
        """解析休息时间，统一转换为(开始时间, 结束时间)格式"""
        parsed_brks = []
        for brk in brks:
            if isinstance(brk, str):
                # 单个时间点，休息时间段设为该时间点（持续时间为0）
                parsed_brks.append((brk, brk))
            elif isinstance(brk, (tuple, list)) and len(brk) == 2:
                # 时间段
                parsed_brks.append((str(brk[0]), str(brk[1])))
        return parsed_brks

    async def _call_execute_task(self):
        """统一调度异步任务"""
        try:
            result = self.execute_task()
            if inspect.iscoroutine(result):
                await result
        except Exception as e:
            logger.error(f"定时任务 {self.function.__name__ if self.function else self.__class__.__name__} 执行失败：{e}")
            logger.debug(format_exc())
        # 如果是同步函数，直接返回结果

    def execute_task(self):
        """如果子类重写本方法，则忽略 function。"""
        if callable(self.function):
            return self.function(*self.args, **self.kwargs)

    async def run(self):
        """异步运行任务"""
        try:
            if self.interval == 0:
                # 单次执行
                if delay_seconds(self.btime) > 0:
                    await asyncio.sleep(delay_seconds(self.btime))
                    if self._cancelled:
                        return
                    await self._call_execute_task()
                    return

                # 已经过了 btime，则判断 etime
                if not self.etime or delay_seconds(self.etime) > 0:
                    await self._call_execute_task()
                else:
                    logger.info(f"定时任务 {self.function.__name__ if self.function else self.__class__.__name__} 结束")
                return

            # 先等待首次触发时间
            sleep_duration = self._calculate_next_sleep_duration()
            await asyncio.sleep(sleep_duration)

            while not self._cancelled:
                # 结束条件
                if delay_seconds(self.etime) < 0:
                    logger.info(f"定时任务 {self.function.__name__ if self.function else self.__class__.__name__} 结束")
                    break

                # 执行任务
                if self.interval >= 60:
                    logger.info(f"定时任务 {self.function.__name__ if self.function else self.__class__.__name__} 执行")
                await self._call_execute_task()

                # 下次执行前等待
                sleep_duration = self._calculate_next_sleep_duration()
                if self.interval >= 60:
                    logger.info(f"定时任务 {self.function.__name__ if self.function else self.__class__.__name__} sleep {sleep_duration}")
                await asyncio.sleep(sleep_duration)

        except asyncio.CancelledError:
            logger.info(f"定时任务 {self.function.__name__ if self.function else self.__class__.__name__} 被取消")
            raise

    def _calculate_next_sleep_duration(self) -> float:
        if delay_seconds(self.btime) > 0:
            return delay_seconds(self.btime)

        if delay_seconds(self.etime) < 0:
            return 0

        for brk_start, brk_end in self.brk_times:
            if delay_seconds(brk_start) < 0 < delay_seconds(brk_end):
                return delay_seconds(brk_end)

        return self.interval

    def start(self):
        """启动异步任务"""
        if self._task is None or self._task.done():
            self._cancelled = False
            self._task = asyncio.create_task(self.run())
        return self._task

    def cancel(self):
        """取消任务"""
        self._cancelled = True
        if self._task and not self._task.done():
            self._task.cancel()


class BkChangesTask(TimerTask):
    def __init__(self):
        super().__init__(600, '9:30:40', '15:1:6', [('11:30:01', '13:0:40')])

    async def execute_task(self):
        bkchanges = await AllBlocks.bkchanges.getLatestChanges()
        if len(bkchanges) > 0:
            logger.info('em bks to load %d: %s %s', len(bkchanges), bkchanges[0], bkchanges[-1])
            await AllBlocks.bkchanges.saveChanges(bkchanges)

        clsbkchanges = await AllBlocks.clsbkchanges.getLatestChanges()
        if len(clsbkchanges) > 0:
            logger.info('cls bks to load %d: %s %s', len(clsbkchanges), clsbkchanges[0], clsbkchanges[-1])
            await AllBlocks.clsbkchanges.saveChanges(clsbkchanges)


class StockMarketStatsTask(TimerTask):
    '''股市概况, 早盘竞价结束自动执行一次, 早上9:40自动执行一次, 收盘执行一次'''
    # ['9:25:05', '9:40', '15:01']
    def __init__(self, btime: str, etime: str):
        super().__init__(0, btime, etime)

    async def execute_task(self):
        await StockMarketStats.execute()


class Timers:
    timers = []
    last_tid = 0

    @classmethod
    def add_timer_task(cls, callback, target_time, end_time=None) -> int:
        timer = TimerTask(0, target_time, end_time, function=callback)
        timer.start()
        cls.last_tid += 1
        cls.timers.append({'id': cls.last_tid, 'timer': timer})
        logger.info(f"已设置定时任务{callback.__name__}，将在 {target_time if delay_seconds(target_time) > 1 else '现在'} 执行")
        return cls.last_tid

    @classmethod
    def cancel_task(cls, tid):
        t = next((t for t in cls.timers if t['id'] == tid), None)
        if t:
            t['timer'].cancel()
            cls.timers.remove(t)

    @classmethod
    def daily_should_run(cls, lastrun, now):
        if lastrun is None or lastrun == '':
            return True

        lt = datetime.strptime(lastrun, f"%Y-%m-%d %H:%M")
        if now.day != lt.day:
            return True

        if now.day == lt.day and now.hour > 15 and lt.hour <= 9:
            return True

        return False

    @classmethod
    def weekly_should_run(cls, lastrun, now):
        if not TradingDate.is_holiday() and not TradingDate.trading_ended():
            return False

        if lastrun is None or lastrun == '':
            return True

        lt = datetime.strptime(lastrun, "%Y-%m-%d %H:%M")
        days_difference = (now - lt).days

        if days_difference >= 7:
            return True

        if now.weekday() == 5 and days_difference > 1:
            return True

        return False

    @classmethod
    def monthly_should_run(cls, lastrun, now):
        if not TradingDate.is_holiday() and not TradingDate.trading_ended():
            return False

        if lastrun is None or lastrun == '':
            return True

        lt = datetime.strptime(lastrun, f"%Y-%m-%d %H:%M")
        days_difference = (now - lt).days
        if days_difference >= 31:
            return True

        if now.day == 1 and days_difference > 1:
            return True

        return False

    @classmethod
    async def run_regular_tasks(cls):
        dnow = datetime.now()
        try:
            lastdaily_run_at = await SystemSettings.get('lastdaily_run_at', '')
            if cls.daily_should_run(lastdaily_run_at, dnow):
                await du.update_all()
                await SystemSettings.set('lastdaily_run_at', dnow.strftime(f"%Y-%m-%d %H:%M"))

            lastweekly_run_at = await SystemSettings.get('lastweekly_run_at', '')
            if cls.weekly_should_run(lastweekly_run_at, dnow):
                await wu.update_all()
                await SystemSettings.set('lastweekly_run_at', dnow.strftime(f"%Y-%m-%d %H:%M"))

            lastmonthly_run_at = await SystemSettings.get('lastmonthly_run_at', '')
            if cls.monthly_should_run(lastmonthly_run_at, dnow):
                await mu.update_all()
                await SystemSettings.set('lastmonthly_run_at', dnow.strftime(f"%Y-%m-%d %H:%M"))

            logger.info(f'run_regular_tasks done, time used: {datetime.now() - dnow}')
        except Exception as e:
            logger.error(f'Error running regular tasks: {e}')
            logger.debug(format_exc())

    @classmethod
    def schedule_trading_tasks(cls):
        if delay_seconds('15:00:00') < 0:
            logger.info(f'run_trading_tasks time passed.')
            return

        try:
            if SystemSettings.settings.get('bkchanges_update_realtime') == '1':
                timer = BkChangesTask()
                timer.start()
            for b,e in [('9:25:05', '9:30'), ('9:40', '9:50'), ('15:01', '15:15')]:
                timer = StockMarketStatsTask(b, e)
                timer.start()
        except Exception as e:
            logger.error(f'Error running trading tasks: {e}')
            logger.debug(format_exc())

    @classmethod
    def setup(cls):
        if Config.client_config().get('disable_timers', False):
            return
        logger.info(f"设置定时任务")
        if TradingDate.is_trading_date(TradingDate.today()):
            cls.schedule_trading_tasks()
            cls.add_timer_task(bk_changes_prepare_task, '9:16:00', '9:30:00')
            cls.add_timer_task(update_bkchanges_history, '15:01:04')
            cls.add_timer_task(update_stock_transactions, '15:33:05')
            cls.add_timer_task(save_earning_task, '15:02:03')
            cls.add_timer_task(update_daily_trade_closed_history, '15:01:00')
        cls.add_timer_task(cls.run_regular_tasks, '8:47:00', '9:11:00')
        cls.add_timer_task(cls.run_regular_tasks, '16:58:00', '23:59:00')
