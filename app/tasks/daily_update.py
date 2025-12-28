# Python 3
# -*- coding:utf-8 -*-

import sys
import os
import traceback
import asyncio
from datetime import datetime
sys.path.insert(0, os.path.realpath(os.path.dirname(__file__) + '/../..'))
from app.admin.router import admin_user_list
from app.users.manager import UserStockManager as usm
from app.lofig import Config, logging
from app.stock.date import TradingDate
from app.stock.manager import AllStocks, AllBlocks
from app.stock.history import Khistory as khis
from app.stock.history import StockShareBonus, StockChanges, StockZtConcepts, StockDtInfo
from app.selectors import SelectorsFactory as sfac

logger = logging.getLogger(f'{Config.app_name}.{__package__}')


class DailyUpdater():
    """for daily update"""
    @classmethod
    async def update_all(cls):
        logger.info(f"START UPDATING....")
        datetoday = datetime.now()
        if TradingDate.is_holiday():
            logger.info(f"Today is holiday, no data to update.")
            return

        morningOnetime = False
        if datetoday.hour < 12:
            morningOnetime = True
            logger.info(f"update in the morning at {datetoday.hour}")

        await cls.download_all_index_history()
        await cls.update_new_stocks()
        if morningOnetime:
            # 只在早上执行的任务
            logger.info("update in the morning...")
            # 分红派息，每天更新一次即可
            await cls.download_newly_noticed_bonuses()
        else:
            # 只在晚上执行的任务
            logger.info("update in the afternoon")
            # 更新所有股票都日k数据
            await cls.download_all_stocks_khistory()
            # 涨跌停数据，可以间隔，早晚都合适
            await cls.fetch_zdt_stocks()
            # 盘口异动数据, 每个交易日收盘后更新, 错过无法补录
            await cls.update_stock_changes()
            #
            await cls.update_selectors()
        # 早上也执行的任务，以防前一晚上没执行
        await cls.update_twice_selectors()

    @classmethod
    async def download_all_index_history(cls):
        await AllStocks.update_kline_data('d', sectype='Index')
        TradingDate.clear_cache()
        logger.info('index history updated!')

    @classmethod
    async def download_all_stocks_khistory(cls):
        logger.info('start download_all_stocks_khistory')
        all_users = await admin_user_list(None)
        allcodes = []
        for u in all_users:
            if u.realcash == 0:
                usm.forget_stocks(u)
            ustks = await usm.watching_stocks(u)
            for c in ustks:
                if not AllStocks.is_quited(c):
                    allcodes.append(c)
                else:
                    await usm.forget_stock(u, c)

        allcodes = list(set(allcodes))
        AllStocks.update_klines_by_code(allcodes, 'd')

        upfailed = []
        for c in allcodes:
            date = khis.max_date(c, 'd')
            if TradingDate.calc_trading_days(date, TradingDate.max_trading_date()) > 20:
                upfailed.append(c)
        if upfailed:
            logger.info(f'stocks update failed: {upfailed}')
            await AllStocks.check_stock_quit(upfailed)

        logger.info('download_all_stocks_khistory done! %d' % len(allcodes))

    @classmethod
    async def update_new_stocks(cls):
        # 新股信息，可以间隔几天更新一次
        logger.info('update new stocks info')
        try:
            await AllStocks.load_new_stocks()
        except Exception as e:
            logger.info(e)

    @classmethod
    async def download_newly_noticed_bonuses(cls):
        logger.info("update noticed bonuses")
        try:
            dbns = StockShareBonus()
            await dbns.getNext()
        except Exception as e:
            logger.info(e)

    @classmethod
    async def fetch_zdt_stocks(cls):
        logger.info('update ST bk stocks')
        try:
            await AllBlocks.load_info('BK0511')
        except Exception as e:
            logger.info(e)
            logger.debug(traceback.format_exc())

        logger.info('update zt info')
        ztinfo = sfac.get('StockZtDaily')
        await ztinfo.update_pickups()

        logger.info('update zt concepts')
        ztcpt = StockZtConcepts()
        await ztcpt.getNext()

        logger.info('update dt info')
        dtinfo = StockDtInfo()
        await dtinfo.getNext()

    @classmethod
    async def update_selectors(cls):
        selectors = ['StockDtMap', 'StockDt3Selector',
        #     'StockDztSelector', 'StockZt1Selector',
        'StockZt1WbSelector',
        # 'StockCentsSelector',
        #     'StockMaConvergenceSelector', 'StockZdfRanks', 'StockZtLeadingSelector', 'StockZtLeadingStepsSelector',
        #     'StockZtLeadingSelectorST', 'StockDztStSelector', 'StockDztBoardSelector', 'StockDztStBoardSelector',
        'StockZdtEmotion', 'StockHotStocksRetryZt0Selector',
        #     'StockZt1BreakupSelector', 'StockZt1j2Selector', 'StockLShapeSelector', 'StockDfsorgSelector',
        'StockTrippleBullSelector',
        #     'StockEndVolumeSelector'
        ]
        for s in selectors:
            sel = sfac.get(s)
            logger.info(f'update {s}')
            await sel.update_pickups()

    @classmethod
    async def update_twice_selectors(cls):
        pass

    @classmethod
    async def update_stock_changes(cls):
        sch = StockChanges()
        await sch.updateDaily()

    @classmethod
    async def update_fixzdt(self):
        await self.fetch_zdt_stocks()
        await self.update_stock_changes()
        await self.update_selectors()
        await self.update_twice_selectors()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(DailyUpdater.update_all())
    # loop.run_until_complete(DailyUpdater.update_selectors())
