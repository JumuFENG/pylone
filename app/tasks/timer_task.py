# Python 3
# -*- coding:utf-8 -*-
import asyncio
import inspect
from threading import Timer
from datetime import datetime
from traceback import format_exc
from app.lofig import Config, logging
from app.hu import delay_seconds
from app.stock.models import MdlSysSettings
from app.db import query_one_value, upsert_one
from .trade_opening import (
    stock_market_opening_task,
    bk_changes_prepare_task,)
from .trade_closed import (
    update_daily_trade_closed_history,
    save_earning_task,
    update_bkchanges_history)
from .daily_update import DailyUpdater as du
from .weekly_update import WeeklyUpdater as wu
from .monthly_update import MonthlyUpdater as mu


logger = logging.getLogger(f'{Config.app_name}.{__package__}')

class Timers:
    timers = []
    last_tid = 0

    @classmethod
    def add_timer_task(cls, callback, target_time, end_time=None) -> int:
        seconds_until = delay_seconds(target_time)
        if seconds_until < 0:
            if end_time is None or delay_seconds(end_time) < 0:
                return
            seconds_until = 0.1

        # 检查callback是否为异步函数
        if inspect.iscoroutinefunction(callback):
            # 为异步函数创建包装器
            def async_wrapper():
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(callback())
                    else:
                        loop.run_until_complete(callback())
                except RuntimeError:
                    asyncio.run(callback())

            timer = Timer(seconds_until, async_wrapper)
        else:
            timer = Timer(seconds_until, callback)

        timer.daemon = True
        timer.start()
        tid = cls.last_tid + 1
        cls.timers.append({'id': tid, 'timer': timer})
        logger.info(f"已设置定时任务{callback.__name__}，将在 {target_time if seconds_until > 1 else '现在'} 执行")
        return tid

    @classmethod
    def cancel_task(cls, tid):
        t = next((t for t in cls.timers if t['id'] == tid), None)
        if t:
            t['timer'].cancel()

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
            lastdaily_run_at = await query_one_value(MdlSysSettings, 'value', MdlSysSettings.key == 'lastdaily_run_at')
            if cls.daily_should_run(lastdaily_run_at, dnow):
                await du.update_all()
                await upsert_one(MdlSysSettings, {'key': 'lastdaily_run_at', 'value': dnow.strftime(f"%Y-%m-%d %H:%M")}, ['key'])

            lastweekly_run_at = await query_one_value(MdlSysSettings, 'value', MdlSysSettings.key == 'lastweekly_run_at')
            if cls.weekly_should_run(lastweekly_run_at, dnow):
                await wu.update_all()
                await upsert_one(MdlSysSettings, {'key': 'lastweekly_run_at', 'value': dnow.strftime(f"%Y-%m-%d %H:%M")}, ['key'])

            lastmonthly_run_at = await query_one_value(MdlSysSettings, 'value', MdlSysSettings.key == 'lastmonthly_run_at')
            if cls.monthly_should_run(lastmonthly_run_at, dnow):
                await mu.update_all()
                await upsert_one(MdlSysSettings, {'key': 'lastmonthly_run_at', 'value': dnow.strftime(f"%Y-%m-%d %H:%M")}, ['key'])

            logger.info(f'run_regular_tasks done, time used: {datetime.now() - dnow}')
        except Exception as e:
            logger.error(f'Error running regular tasks: {e}')
            logger.debug(format_exc())

    @classmethod
    def setup(cls):
        logger.info(f"设置定时任务")
        cls.add_timer_task(cls.run_regular_tasks, '8:47:00', '9:11:00')
        cls.add_timer_task(stock_market_opening_task, '9:25:00')
        cls.add_timer_task(bk_changes_prepare_task, '9:16:00')
        cls.add_timer_task(cls.run_regular_tasks, '16:58:00', '23:59:00')
        cls.add_timer_task(update_daily_trade_closed_history, '15:01:00')
        cls.add_timer_task(update_bkchanges_history, '15:01:04')
        cls.add_timer_task(save_earning_task, '15:02:03')
