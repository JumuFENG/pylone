# Python 3
# -*- coding:utf-8 -*-

import sys
import os
sys.path.insert(0, os.path.realpath(os.path.dirname(os.path.realpath(__file__)) + '/../..'))

from app.lofig import Config, logging
from app.stock.date import TradingDate
from app.stock.manager import AllStocks
from app.stock.history import FflowHistory
logger = logging.getLogger(f'{Config.app_name}.{__package__}')

# from tasks import StockMarket_Stats_Task

def stock_market_opening_task():
    if TradingDate.today() != TradingDate.max_trading_date():
        logger.warning(f'today is not trading day!')
        return
    # sauc = StockAuction()
    # sauc.update_daily_auctions()
    # shr = StockHotRank()
    # shr.getNext()


def bk_changes_prepare_task():
    pass
    # bkchghis = StockBkChangesHistory()
    # for bk in bkchghis.dumpDataByDate():
    #     sbk = StockEmBk(bk)
    #     sbk.getNext()
    # clsbkhis = StockClsBkChangesHistory()
    # for bk in clsbkhis.dumpDataByDate():
    #     sbk = StockClsBk(bk)
    #     sbk.getNext()


# class UpdateBkTask(TimerTask):
#     def __init__(self) -> None:
#         super().__init__('9:16', bk_changes_prepare_task)

# class SmStatsTask925(TimerTask):
#     def __init__(self) -> None:
#         super().__init__('9:25:05', StockMarket_Stats_Task.execute_simple_task)

# class SmStatsTask940(TimerTask):
#     def __init__(self) -> None:
#         super().__init__('9:40', StockMarket_Stats_Task.execute_simple_task)


if __name__ == '__main__':
    bk_changes_prepare_task()
    stock_market_opening_task()
    # tasks = [AuctionTask(), UpdateBkTask(), SmStatsTask925(), SmStatsTask940()]
