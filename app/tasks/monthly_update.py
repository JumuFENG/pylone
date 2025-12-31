# Python 3
# -*- coding:utf-8 -*-

import sys
import os
import traceback
import asyncio
from datetime import datetime
sys.path.insert(0, os.path.realpath(os.path.dirname(__file__) + '/../..'))

from app.lofig import Config, logging
from app.stock.manager import AllStocks
logger = logging.getLogger(f'{Config.app_name}.{__package__}')


class MonthlyUpdater():
    """for monthly update"""
    @staticmethod
    async def update_all():
        logger.info('Start monthly update. %s', datetime.now())

        try:
            await AllStocks.update_kline_data('m', sectype='Index')
            await AllStocks.update_kline_data('m')
            await AllStocks.update_purelost4up()
            await AllStocks.load_all_funds()
        except Exception as e:
            logger.error(e)
            logger.debug(traceback.format_exc())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(MonthlyUpdater.update_all())
