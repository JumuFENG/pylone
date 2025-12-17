# Python 3
# -*- coding:utf-8 -*-

import sys
import os
import traceback
import asyncio
from threading import Thread
from datetime import datetime
sys.path.insert(0, os.path.realpath(os.path.dirname(__file__) + '/../..'))
# from utils import Utils
# from history import *
# from pickup import *
# from phon.data.user import User
from app.lofig import Config, logging
from app.stock.date import TradingDate
from app.stock.manager import AllStocks, AllBlocks
from app.stock.history import StockShareBonus, StockChanges, StockBkMap, StockZtDaily, StockZtConcepts, StockDtInfo, StockDtMap
logger = logging.getLogger(f'{Config.app_name}.{__package__}')


class DailyUpdater():
    """for daily update"""
    @classmethod
    async def update_all(cls):
        logger.info(f"START UPDATING....")
        datetoday = datetime.now()
        if datetoday.weekday() >= 5:
            logger.info(f'it is weekend, no data to update. {datetoday.strftime("%Y-%m-%d")}')
            return

        strtoday = TradingDate.today()
        if TradingDate.is_holiday(strtoday):
            logger.info(f"it is holiday, no data to update. {strtoday}")
            return

        morningOnetime = False
        if datetoday.hour < 12:
            morningOnetime = True
            logger.info(f"update in the morning at {datetoday.hour}")

        await cls.download_all_index_history()
        cls.update_stock_hotrank()
        await cls.update_new_stocks()
        if morningOnetime:
            # 只在早上执行的任务
            logger.info("update in the morning...")
            # 分红派息，每天更新一次即可
            await cls.download_newly_noticed_bonuses()
        else:
            # 只在晚上执行的任务
            logger.info("update in the afternoon")
            # 机构游资龙虎榜，可以间隔，首选晚上更新
            cls.fetch_dfsorg_stocks()
            # 更新所有股票都日k数据
            cls.download_all_stocks_khistory()
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
    def download_all_stocks_khistory(cls):
        logger.info('start download_all_stocks_khistory')
        # all_users = User.all_users()
        allcodes = []
        # for u in all_users:
        #     if u.id <= 10:
        #         continue
        #     if u.realcash == 0:
        #         u.forget_stocks()
        #     ustks = u.all_interest_stocks()
        #     if ustks:
        #        allcodes = allcodes + ustks

        # allcodes = [s for s in set(allcodes) if not AllStocks.is_quited(s)]
        # upfailed = AllStocks.update_klines_by_code(allcodes, 'd')
        # if not upfailed:
        #     logger.info('all stocks kline data updated!')
        #     return

        # upfailed = [s.upper() for s in upfailed]
        # if upfailed:
        #     sa = StockAnnoucements()
        #     logger.info(f'stocks update failed: {upfailed}')
        #     sa.check_stock_quit(upfailed)
        #     sa.check_fund_quit(upfailed)

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
        # logger.info('update announcements')
        # try:
        #     ann = StockAnnoucements()
        #     ann.getNext()
        # except Exception as e:
        #     logger.info(e)

    @classmethod
    async def fetch_zdt_stocks(cls):
        logger.info('update ST bk stocks')
        try:
            await AllBlocks.load_info('BK0511')
        except Exception as e:
            logger.info(e)

        logger.info('update zt info')
        ztinfo = StockZtDaily()
        await ztinfo.update_pickups()

        logger.info('update zt concepts')
        ztcpt = StockZtConcepts()
        await ztcpt.getNext()

        logger.info('update dt info')
        dtinfo = StockDtInfo()
        await dtinfo.getNext()

    @classmethod
    def fetch_dfsorg_stocks(cls):
        # dfsorg = StockDfsorg()
        # try:
        #     dfsorg.updateDfsorg()
        # except Exception as e:
        #     logger.error(e)
        #     logger.debug(traceback.format_exc())
        pass

    @classmethod
    async def update_selectors(cls):
        logger.info('update dtmap info')
        sdm = StockDtMap()
        await sdm.update_pickups()

        # logger.info('update dt3')
        # dts = StockDt3Selector()
        # dts.updateDt3()

        # selectors = [
        #     StockDztSelector(), StockZt1Selector(), StockZt1WbSelector(), StockCentsSelector(),
        #     StockMaConvergenceSelector(), StockZdfRanks(), StockZtLeadingSelector(), StockZtLeadingStepsSelector(),
        #     StockZtLeadingSelectorST(), StockDztStSelector(), StockDztBoardSelector(), StockDztStBoardSelector(),
        #     StockZdtEmotion(), StockHotStocksRetryZt0Selector(),
        #     StockZt1BreakupSelector(), StockZt1j2Selector(), StockLShapeSelector(), StockDfsorgSelector(),
        #     StockTrippleBullSelector(), StockEndVolumeSelector()]
        # for sel in selectors:
        #     logger.info(f'update { sel.__class__.__name__}')
        #     sel.updatePickUps()

    @classmethod
    async def update_twice_selectors(cls):
        # selectors = [
        #     StockUstSelector()]
        # for sel in selectors:
        #     logger.info(f'update {sel.__class__.__name__}')
        #     sel.updatePickUps()
        pass

    @classmethod
    def update_stock_hotrank(cls):
        # shr = StockHotRank()
        # shr.getNext()
        pass

    @classmethod
    async def update_stock_changes(cls):
        sch = StockChanges()
        await sch.updateDaily()

    @classmethod
    async def update_fixzdt(self):
        # await self.fetch_zdt_stocks()
        await self.update_stock_changes()
        await self.update_selectors()
        await self.update_twice_selectors()


if __name__ == '__main__':
    asyncio.run(DailyUpdater.update_selectors())
