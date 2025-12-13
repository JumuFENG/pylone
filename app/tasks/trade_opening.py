# Python 3
# -*- coding:utf-8 -*-

import sys
import os
import asyncio
sys.path.insert(0, os.path.realpath(os.path.dirname(os.path.realpath(__file__)) + '/../..'))

from app.lofig import Config, logging
from app.stock.date import TradingDate
from app.stock.manager import AllStocks, AllBlocks
logger = logging.getLogger(f'{Config.app_name}.{__package__}')


def stock_market_opening_task():
    if TradingDate.today() != TradingDate.max_trading_date():
        logger.warning(f'today is not trading day!')
        return
    # sauc = StockAuction()
    # sauc.update_daily_auctions()
    # shr = StockHotRank()
    # shr.getNext()


async def bk_changes_prepare_task():
    bks = await AllBlocks.bkchanges.dumpDataByDate()
    clsbks = await AllBlocks.clsbkchanges.dumpDataByDate()
    for bk in bks + clsbks:
        await AllBlocks.load_info(bk)


if __name__ == '__main__':
    async def run_me():
        await bk_changes_prepare_task()
        await stock_market_opening_task()
    asyncio.run(run_me())
