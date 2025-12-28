# Python 3
# -*- coding:utf-8 -*-

import sys
import os
import asyncio
sys.path.insert(0, os.path.realpath(os.path.dirname(__file__) + '/../..'))

from app.lofig import Config, logging
from app.admin.router import admin_user_list
from app.users.manager import UserStockManager as usm
from app.stock.manager import AllStocks, khis
from app.stock.date import TradingDate

logger = logging.getLogger(f'{Config.app_name}.{__package__}')


class WeeklyUpdater():
    """for weekly update"""
    @classmethod
    async def update_all(cls):
        logger.info('Start weekly update.')

        await AllStocks.update_kline_data('w', sectype='Index')
        await AllStocks.update_kline_data('w')
        await AllStocks.update_stock_fflow()

        all_users = await admin_user_list(None)
        allcodes = []
        for u in all_users:
            if u.realcash == 0:
                usm.forget_stocks(u)
            ustks = await usm.watching_stocks(u)
            for c in ustks:
                if not await AllStocks.is_quited(c):
                    allcodes.append(c)
                else:
                    await usm.forget_stock(u, c)
        stocks = list(set(allcodes))
        AllStocks.update_klines_by_code(stocks, 'w')
        await cls.update_holidays()
        await cls.update_quit_stocks()
        logger.info('Weekly update finished.')

    @classmethod
    async def update_holidays(cls):
        try:
            await TradingDate.update_holiday()
        except Exception as e:
            logger.error(e)

    @classmethod
    async def update_quit_stocks(cls):
        qstocks = await AllStocks.read_all()
        quit_stocks = []
        for s in qstocks:
            if s.typekind == 'TSStock' and not s.quit_date:
                quit_stocks.append(s.code)
        AllStocks.update_klines_by_code(quit_stocks, 'd')
        for c in quit_stocks:
            date = khis.max_date(c, 'd')
            await AllStocks.update_stock({'code': c, 'quit_date': date})

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(WeeklyUpdater.update_all())
