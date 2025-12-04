# Python 3
# -*- coding:utf-8 -*-

import sys
import os
import asyncio
sys.path.insert(0, os.path.realpath(os.path.dirname(__file__) + '/../..'))

from app.lofig import Config, logging
from app.stock.manager import AllStocks
logger = logging.getLogger(f'{Config.app_name}.{__package__}')


class WeeklyUpdater():
    """for weekly update"""
    @staticmethod
    async def update_all():
        logger.info('Start weekly update.')

        await AllStocks.update_kline_data('w', sectype='Index')
        await AllStocks.update_kline_data('w')
        AllStocks.update_stock_fflow()

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

if __name__ == '__main__':
    asyncio.run(WeeklyUpdater.update_all())
