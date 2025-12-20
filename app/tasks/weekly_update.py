# Python 3
# -*- coding:utf-8 -*-

import sys
import os
import asyncio
sys.path.insert(0, os.path.realpath(os.path.dirname(__file__) + '/../..'))

from app.lofig import Config, logging
from app.stock.manager import AllStocks
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

        # all_users = User.all_users()
        stocks = []
        # for u in all_users:
        #     if u.id <= 10:
        #         continue
        #     ustks = u.all_interest_stocks()
        #     if ustks:
        #         stocks = stocks + ustks

        # stocks = [s for s in set(stocks) if not AllStocks.is_quited(s)]
        AllStocks.update_klines_by_code(stocks, 'w')
        await cls.update_holidays()

    @classmethod
    async def update_holidays(cls):
        try:
            await TradingDate.update_holiday()
        except Exception as e:
            logger.error(e)

if __name__ == '__main__':
    asyncio.run(WeeklyUpdater.update_all())
