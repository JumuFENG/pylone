from typing import Optional
from datetime import datetime, timedelta
from app.stock.models import MdlStockBkMap
from app.stock.date import TradingDate
from app.db import query_values, upsert_one, upsert_many, delete_records
from .models import Mdl3Bull
from .stock_base_selector import StockBaseSelector


class StockTrippleBullSelector(StockBaseSelector):
    '''三阳买入
    选股条件; 连续3根阳线价升量涨 以突破此3根阳线的最高价为买入点 以第一根阳线到买入日期之间的最低价为止损价 止盈设置5%
    '''
    def __init__(self):
        super().__init__()
        self.erate = 0.05
        self.crate = 0.05

    @property
    def db(self):
        return Mdl3Bull

    async def task_prepare(self, date=None):
        self.nonefdates = {}
        self.maxfdates = {}
        self.upstocks = []
        self.blockedst = await query_values(MdlStockBkMap, [MdlStockBkMap.stock], MdlStockBkMap.bk == 'BK0511')
        self.blockedst = [s for s, in self.blockedst]
        dfdates = await query_values(self.db, ['code', 'date', 'fdate'])
        if dfdates is None or len(dfdates) == 0:
            await super().task_prepare()
            return
        ncodes = {c: d for c,d,f in dfdates if f is None}
        xcodes = {}
        for c,d,f in dfdates:
            if f is None:
                continue
            if c not in xcodes:
                xcodes[c] = []
            xcodes[c].append(f)
        xcodes = {c:max(d) for c, d in xcodes.items()}
        pops = []
        for c in ncodes.keys():
            if c in xcodes and xcodes[c] > ncodes[c]:
                await delete_records(self.db, {'code': c, 'date': ncodes[c]})
                pops.append(c)
        [ncodes.pop(c) for c in pops]
        self.nonefdates = ncodes
        await super().task_prepare(max(xcodes.values()))
        wkstocks = []
        for c,d in self.wkstocks:
            if c in ncodes:
                fdate = await self.check_nonfinished(c, ncodes[c])
                if fdate is not None:
                    wkstocks.append([c, fdate])
                    xcodes[c] = fdate
                else:
                    wkstocks.append([c, ncodes[c]])
            elif c in xcodes:
                wkstocks.append([c, xcodes[c]])
            else:
                wkstocks.append([c, d])
        self.maxfdates = xcodes
        self.wkstocks = wkstocks

    async def check_nonfinished(self, code, date):
        if code not in self.nonefdates:
            return
        kdate = (datetime.strptime(self.nonefdates[code], r'%Y-%m-%d') + timedelta(days=-12)).strftime(r"%Y-%m-%d")
        allkl = await self.get_kd_data(code, start=kdate)
        if allkl is None or len(allkl) == 0:
            return
        i = 0
        while i < len(allkl) and allkl[i].time <= self.nonefdates[code]:
            i += 1
        if i >= len(allkl):
            return

        updatefdate = False
        if i >= 3 and allkl[i-1].time == self.nonefdates[code]:
            uprice = max(allkl[i-1].high, allkl[i-2].high, allkl[i-3].high)
            support = min(allkl[i-1].low, allkl[i-2].low, allkl[i-3].low)
            j = i
            while j < len(allkl) and allkl[j].time < date:
                j += 1
            while j < len(allkl):
                if allkl[j].high > uprice:
                    updatefdate = True
                    break
                if allkl[j].close < support and j - i > 10:
                    updatefdate = True
                    break
                j += 1
        if updatefdate:
            await upsert_one(self.db, {'code': code, 'date': self.nonefdates[code], 'fdate': allkl[j].time}, ['code', 'date'])
            self.nonefdates.pop(code)
            if j > i:
                return allkl[j].time
            while j < len(allkl):
                if not (allkl[j].close > allkl[j-1].close and allkl[j].volume > allkl[j-1].volume and allkl[j].close > allkl[j].open):
                    break
                j += 1
            return allkl[j if j < len(allkl) else -1].time

    async def task_processing(self, item):
        c, sdate = item
        if c in self.blockedst:
            return
        kdate = (datetime.strptime(sdate, r'%Y-%m-%d') + timedelta(days=-12)).strftime(r"%Y-%m-%d")
        allkl = await self.get_kd_data(c, start=kdate)
        if allkl is None or len(allkl) == 0:
            return

        i = 0
        while i < len(allkl) and allkl[i].time < sdate:
            i += 1

        if i >= len(allkl):
            return

        while i < len(allkl):
            if i < 2:
                i += 1
                continue
            if allkl[i].close < allkl[i].open or allkl[i-1].close < allkl[i-1].open or allkl[i-2].close < allkl[i-2].open:
                i += 1
                continue
            if allkl[i].close < allkl[i - 1].close or allkl[i-1].close < allkl[i - 2].close:
                i += 1
                continue
            if allkl[i].volume < allkl[i-1].volume or allkl[i-1].volume < allkl[i-2].volume:
                i += 1
                continue
            if allkl[i].change > 0.08 or allkl[i-1].change > 0.08 or allkl[i-2].change > 0.08:
                i += 1
                continue
            j = i + 1
            uprice = max(allkl[i].high, allkl[i-1].high, allkl[i-2].high)
            support = min(allkl[i].low, allkl[i-1].low, allkl[i-2].low)
            fdate = None
            while j < len(allkl):
                if allkl[j].high > uprice or allkl[j].change < -0.05:
                    fdate = allkl[j].time
                    break
                lowest = min([kl.low for kl in allkl[i-2 : j+1]])
                if (uprice - lowest) / uprice > 0.1:
                    fdate = allkl[j].time
                    break
                if allkl[j].close < support and j - i > 10:
                    fdate = allkl[j].time
                    break
                j += 1

            if fdate is not None:
                if c in self.nonefdates:
                    if self.nonefdates[c] == allkl[i].time:
                        self.upstocks.append([fdate, c, self.nonefdates[c]])
                    else:
                        self.upstocks.append([allkl[i-2].time, c, self.nonefdates[c]])
                        self.wkselected.append([allkl[i].time, c, 1, allkl[i-2].time, fdate])
                    self.maxfdates[c] = fdate
                    i = j
                    continue
            if c not in self.maxfdates or allkl[i-2].time > self.maxfdates[c]:
                if fdate is not None and (c not in self.maxfdates or self.maxfdates[c] < fdate):
                    self.maxfdates[c] = fdate
                if c in self.nonefdates and self.nonefdates[c] != allkl[i].time:
                    self.upstocks.append([allkl[i-2].time, c, self.nonefdates[c]])
                self.wkselected.append([allkl[i].time, c, 1, allkl[i-2].time, fdate])
            i = j

    async def post_process(self):
        await upsert_many(self.db, [dict(zip(['fdate', 'code', 'date'], x)) for x in self.upstocks], ['code', 'date'])
        await super().post_process(update=True)

    async def dumpDataByDate(self, date: Optional[str] = None):
        cond = [self.db.prepk == 1, self.db.fdate == None]
        if date is not None:
            cond.append(self.db.date == date)
        rows = await query_values(self.db, ['code', 'bdate', 'date'], *cond)
        return rows

    async def getLatestCandidatesHighLow(self, fullcode=False):
        cdb = await query_values(self.db, ['code', 'date', 'bdate'], self.db.prepk == 1, self.db.fdate == None)
        chl = []
        for c, d, b in cdb:
            allkl = await self.get_kd_data(c, b, fqt=1)
            high = allkl[0].high
            i = 1
            while i < len(allkl) and allkl[i].time <= d:
                if allkl[i].high > high:
                    high = allkl[i].high
                i += 1
            low = min([kl.low for kl in allkl])
            chl.append([c if fullcode else c[2:], high, low])
        return chl

    async def getDaysCandidatesHighLow(self, days = 5, fullcode=False):
        date = TradingDate.max_trading_date()
        date = TradingDate.prev_trading_date(date, days)
        cdb = await query_values(self.db, ['code', 'date', 'bdate'], self.db.date < date, self.db.prepk == 1, self.db.fdate == None)
        chl = []
        for c, d, b in cdb:
            allkl = await self.get_kd_data(c, b, fqt=1)
            high = allkl[0].high
            i = 1
            while i < len(allkl) and allkl[i].time <= d:
                if allkl[i].high > high:
                    high = allkl[i].high
                i += 1
            low = min([kl.low for kl in allkl])
            chl.append([c if fullcode else c[2:], high, low])
        return chl
