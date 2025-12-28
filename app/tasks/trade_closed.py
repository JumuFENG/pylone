# Python 3
# -*- coding:utf-8 -*-

import sys
import os
import asyncio
from traceback import format_exc
sys.path.insert(0, os.path.realpath(os.path.dirname(__file__) + '/../..'))

from datetime import datetime
from app.lofig import Config, logging
from app.stock.date import TradingDate
from app.stock.manager import AllStocks, AllBlocks
from app.admin.router import admin_user_list
from app.admin.system_settings import SystemSettings
from app.users.manager import UserStockManager as usm


logger = logging.getLogger(f'{Config.app_name}.{__package__}')

async def save_earning_task():
    if TradingDate.today() != TradingDate.max_trading_date():
        logger.warning(f'today is not trading day!')
        return

    dnow = datetime.now()
    if dnow.weekday() == 4:
        users = await admin_user_list()
        for u in users:
            if u.realcash == 0:
                continue
            await usm.archive_deals(u, f'{dnow.year + 1}')

async def update_bkchanges_history():
    try:
        await AllBlocks.update_bk_changed_in5days()
    except Exception as e:
        logger.error(f'Error updating bk changes history: {e}')
        logger.debug(format_exc())

async def update_stock_transactions():
    try:
        await AllStocks.update_stock_transactions()
        logger.info('stock transactions updated!')
    except Exception as e:
        logger.error(f'Error updating stock transactions: {e}')
        logger.debug(format_exc())

async def update_daily_trade_closed_history():
    try:
        await AllStocks.update_kline_data('d', sectype='Index')
        logger.info('index history updated!')
        TradingDate.clear_cache()
        await AllStocks.update_kline_data('d')
        logger.info('stock history updated!')
        if await SystemSettings.get('daily_15min', '0') == '1':
            logger.info('update 15min history')
            await AllStocks.update_kline_data(15)
        if await SystemSettings.get('daily_5min', '0') == '1':
            logger.info('update 5min history')
            await AllStocks.update_kline_data(5)
        if await SystemSettings.get('daily_1min', '0') == '1':
            logger.info('update 1min history')
            await AllStocks.update_kline_data(1)
    except Exception as e:
        logger.error(f'Error updating daily history data: {e}')
        logger.debug(format_exc())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(update_daily_trade_closed_history())
    loop.run_until_complete(update_bkchanges_history())
    loop.run_until_complete(update_stock_transactions())
    loop.run_until_complete(save_earning_task())
