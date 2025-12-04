# Python 3
# -*- coding:utf-8 -*-

import sys
import os
import traceback
import asyncio
from datetime import datetime, timedelta
sys.path.insert(0, os.path.realpath(os.path.dirname(__file__) + '/../..'))
# from utils import *
# from history import StockEmBk, StockDfsorg
# from phon.data.history import AllIndexes, AllStocks
# from phon.data.misc import PureLost4Up
from app.lofig import Config, logging
from app.stock.manager import AllStocks
logger = logging.getLogger(f'{Config.app_name}.{__package__}')


class MonthlyUpdater():
    """for monthly update"""
    @staticmethod
    async def update_all():
        print('')
        print('Start monthly update.', datetime.now())

        try:
            await AllStocks.update_kline_data('m', sectype='Index')
            await AllStocks.update_kline_data('m')
            # PureLost4Up.update_em()

            logger.info('update dfsorg details')
            # dfsorg = StockDfsorg()
            # dfsorg.updateDetails()
        except Exception as e:
            logger.error(e)
            logger.debug(traceback.format_exc())


if __name__ == '__main__':
    asyncio.run(MonthlyUpdater.update_all())
