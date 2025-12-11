# Python 3
# -*- coding:utf-8 -*-

import sys
import os
import asyncio
from traceback import format_exc
sys.path.insert(0, os.path.realpath(os.path.dirname(__file__) + '/../..'))

# from phon.data.user import User
# from utils import Utils, datetime, shared_cloud_foler
from datetime import datetime
# from tasks import StockMarket_Stats_Task
from app.stock.date import TradingDate
from app.stock.manager import AllStocks, AllBlocks
from app.admin.system_settings import SystemSettings
from app.lofig import Config, logging
logger = logging.getLogger(f'{Config.app_name}.{__package__}')


async def save_earning_task():
    if TradingDate.today() != TradingDate.max_trading_date():
        logger.warning(f'today is not trading day!')
        return

    # mxretry = 3
    # retry = 0
    # while retry < mxretry:
    #     try:
    #         logger.info(f'save_earning_task!')
    #         User.save_stocks_eaning_html(shared_cloud_foler, [11, 14])
    #         break
    #     except Exception as e:
    #         retry += 1
    #         logger.error(f'Error saving earnings: {e}')
    #         logger.error(format_exc())
    #         sleep(10)
    #         if retry < mxretry:
    #             continue

    # dnow = datetime.now()
    # if dnow.weekday() == 4:
    #     for uid in [11, 14]:
    #         user = User.user_by_id(uid)
    #         user.archive_deals(f'{dnow.year + 1}')


async def update_bkchanges_history():
    try:
        await AllBlocks.updateBkChangedIn5Days()
    except Exception as e:
        logger.error(f'Error updating bk changes history: {e}')
        logger.error(format_exc())



async def update_daily_trade_closed_history():
    try:
        await AllStocks.update_kline_data('d', sectype='Index')
        TradingDate.clear_cache()
        await AllStocks.update_kline_data('d')
        if await SystemSettings.get('daily_15min', '0') == '1':
            await AllStocks.update_kline_data(15)
        if await SystemSettings.get('daily_5min', '0') == '1':
            await AllStocks.update_kline_data(5)
        if await SystemSettings.get('daily_1min', '0') == '1':
            await AllStocks.update_kline_data(1)
    except Exception as e:
        logger.error(f'Error updating daily history data: {e}')
        logger.error(format_exc())



# class SmStatsTask1501(TimerTask):
#     def __init__(self) -> None:
#         super().__init__('15:01:10', StockMarket_Stats_Task.execute_simple_task)


if __name__ == '__main__':
    async def run_me():
        await save_earning_task()
        await update_bkchanges_history()
        await update_daily_trade_closed_history()
    asyncio.run(run_me())
