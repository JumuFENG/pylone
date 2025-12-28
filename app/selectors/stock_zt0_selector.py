from typing import Optional
from app.lofig import logger
from app.stock.models import MdlAllStock
from app.stock.date import TradingDate
from app.db import query_one_record, query_values, query_aggregate, upsert_one
from .models import MdlZt0hrst0
from .stock_base_selector import StockBaseSelector
from .stock_ztlead_selector import StockHotStocksOpenSelector


class StockHotStocksRetryZt0Selector(StockBaseSelector):
    '''
    高标/人气股 涨停回调(>3个交易日)之后首板打板买入,
    入选之后66个交易日不涨停则剔除，再次涨停后22个交易日不涨停剔除
    首板后若连板高度>3则重新计算
    卖出条件，
    次日如果涨停则开板卖出，如果浮盈>5% 则回撤5%卖出, 如果不涨停，尾盘卖出，
    次日如果深水开盘则盘中冲高卖出，如果盘中横盘则跌破横盘区需止损卖出，
    如果尾盘跌停，且股价处于近期低位则转波段策略
    '''
    @property
    def db(self):
        return MdlZt0hrst0

    async def task_prepare(self, date=None):
        self.wkselected = []
        shso = StockHotStocksOpenSelector()
        hsos = await shso.dumpDataLaterThanDate(date)
        ssel = {}
        for d,c,zd,days,step,*_ in hsos:
            if step < 4:
                continue
            if c not in ssel:
                stock = await query_one_record(MdlAllStock, MdlAllStock.code == c)
                if not stock or stock.setup_date is None:
                    continue
                if TradingDate.calc_trading_days(stock.setup_date, zd) < 2*days:
                    continue
                ssel[c] = [(zd,days,step)]
                continue
            ld,ldays,lstep = ssel[c][-1]
            if ld == zd:
                continue
            if TradingDate.calc_trading_days(ld, zd) > 4:
                ssel[c].append((zd,days,step))
                continue
            ssel[c][-1] = (zd,days,step)

        self.wkstocks = []
        if date is None:
            for c in ssel:
                for zd,days,step in ssel[c]:
                    self.wkstocks.append((c,zd,days,step,66))
        else:
            orecs = await query_values(self.db, ['date', 'code', 'days', 'step', 'remdays'], self.db.remdays > 0)
            for d,c,days,step,rd in orecs:
                if c in ssel:
                    del ssel[c]
                allkl = await self.get_kd_data(c, TradingDate.prev_trading_date(d, days*2), fqt=1)
                lbc, fdate, ldate = self.check_lbc(allkl)
                if lbc >= step and ldate != d and fdate < d:
                    days = len([d for x in allkl if x.date >= fdate and x.date <= ldate])
                    await upsert_one(self.db, {'code': c, 'date': ldate, 'days': days, 'step': lbc, 'remdays': 66}, ['code', 'date'])
                    self.wkstocks.append((c,ldate,days,lbc,66))
                    continue
                if lbc >= 3 and ldate != d:
                    await upsert_one(self.db, {'code': c, 'date': d, 'remdays': 0, 'dropdate': fdate}, ['code', 'date'])
                    days = len([d for x in allkl if x.date >= fdate and x.date <= ldate])
                    self.wkstocks.append((c,ldate,days,lbc,66))
                    logger.info(f'lbc >= 3, drop old record and add new record {c} {lbc} {fdate} {ldate}')
                    continue
                self.wkstocks.append((c,d,days,step,rd))
            for c in ssel:
                for zd,days,step in ssel[c]:
                    self.wkstocks.append((c,zd,days,step, 66))
        self.wkstocks = sorted(self.wkstocks, key=lambda x: (x[0], x[1]))

    def check_lbc(self, allkl):
        lbc, fid, lid = 0, 0, 0
        mxlbc, mxfid, mxlid = 0, 0, 0
        for i in range(0, len(allkl)):
            if round(allkl[i].pchange) >= 10 and allkl[i].high == allkl[i].close:
                if lbc == 0:
                    fid = i
                lbc += 1
                lid = i
            if i - lid >= 3:
                if lbc > mxlbc:
                    mxlbc = lbc
                    mxfid = fid
                    mxlid = lid
                lbc = 0
        if lbc > mxlbc:
            mxlbc = lbc
            mxfid = fid
            mxlid = lid
        return mxlbc, allkl[mxfid].date, allkl[mxlid].date

    async def task_processing(self, item):
        c,d,days,step,rdays = item
        allkl = await self.get_kd_data(c, TradingDate.prev_trading_date(d, days), fqt=1)
        post_days = len([x for x in allkl if x.date > d])
        if post_days < 66:
            self.wkselected.append([d,c,days,step,66-post_days,''])
            return
        i = 0
        while i < len(allkl) and allkl[i].date < d:
            i += 1
        if not any([round(x.pchange) >= 10 and x.high == x.close for x in allkl if x.date > d and allkl.index(x) - i < 66]):
            self.wkselected.append([d,c,days,step,0,allkl[i+66].date])
            return
        last_zid = i + 66
        while last_zid > i:
            if round(allkl[last_zid].pchange) >= 10 and allkl[last_zid].high == allkl[last_zid].close:
                break
            last_zid -= 1
        fianal_zid = max(last_zid + 22, i + 66)
        while fianal_zid < len(allkl):
            while fianal_zid > last_zid:
                if round(allkl[fianal_zid].pchange) >= 10 and allkl[fianal_zid].high == allkl[fianal_zid].close:
                    break
                fianal_zid -= 1
            if fianal_zid > last_zid:
                last_zid = fianal_zid
                fianal_zid += 22
            else:
                break
        fianal_zid = max(fianal_zid, i + 66)
        if fianal_zid >= len(allkl):
            self.wkselected.append([d,c,days,step,fianal_zid - len(allkl) + 1, ''])
        else:
            self.wkselected.append([d,c,days,step,0,allkl[fianal_zid].date])

    async def dumpDataByDate(self, date=None):
        if date is None:
            date = await query_aggregate('max', self.db, 'date')
        ldate = TradingDate.prev_trading_date(date, 2)
        return await query_values(self.db, ['date', 'code', 'days', 'step'], self.db.date < ldate, self.db.remdays > 0)

