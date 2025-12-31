from typing import Optional
from app.lofig import logger
from app.stock import async_lru, zdf_from_code, zt_priceby
from app.stock.history import srt, StockZtInfo, StockZtInfo10jqka
from app.stock.models import MdlAllStock, MdlDayZtStocks, MdlStockBkMap, MdlStockBk, MdlDayDtStocks
from app.stock.date import TradingDate
from app.db import query_values, query_one_value, query_aggregate, query_group_counts, upsert_many, or_
from .models import MdlHotstksOpen, MdlDayZdtEmotion
from .stock_base_selector import StockBaseSelector


class StockZtDaily(StockBaseSelector):
    def __init__(self):
        super().__init__(1)

    @property
    def db(self):
        return MdlDayZtStocks

    @property
    def ztinfo(self):
        return StockZtInfo()

    @property
    def jqkinfo(self):
        return StockZtInfo10jqka()

    @async_lru()
    async def st_stocks(self):
        sts = await query_values(MdlStockBkMap, MdlStockBkMap.stock, MdlStockBkMap.bk == 'BK0511')
        return [s for s, in sts]

    async def is_st_stock(self, code):
        sts = await self.st_stocks()
        return code in sts

    async def task_prepare(self, date: Optional[str] = None):
        onlycalc = date is None
        if date is None:
            await super().task_prepare(date)
        else:
            date = TradingDate.next_trading_date(date)
            stks = await query_values(MdlAllStock, [MdlAllStock.code], or_(MdlAllStock.typekind == 'ABStock', MdlAllStock.typekind == 'BJStock'))
            self.wkstocks = [[c, date] for c, in stks]
            self.wkselected = []

        for i, w in enumerate(self.wkstocks):
            sdate = await query_aggregate('max', self.db, 'time', MdlDayZtStocks.code == w[0], MdlDayZtStocks.lbc == 1)
            if sdate is None or sdate == 0:
                sdate = ''
            else:
                sdate = TradingDate.prev_trading_date(sdate)
            self.wkstocks[i].append(sdate)

        while not onlycalc:
            ztdata = self.ztinfo.getNext(date)
            jqkztdata = self.jqkinfo.getNext(date)
            for ztobj in jqkztdata:
                zdata = next((t for t in ztdata if t[0] == ztobj[0] and t[1] == ztobj[1]), None)
                if zdata:
                    zdata[-2] = ztobj[-2]
                else:
                    ztdata.append(ztobj)
            self.wkselected.extend(ztdata)
            if date == TradingDate.max_trading_date():
                break
            date = TradingDate.next_trading_date(date)

    async def get_embk(self, code):
        bks = await query_values(MdlStockBkMap, 'bk', MdlStockBkMap.stock == code)
        if bks is None or len(bks) == 0:
            return ''
        bk, = bks[0]
        return await query_one_value(MdlStockBk, 'name', MdlStockBk.code == bk)

    @async_lru()
    async def get_bk(self, code):
        bks = await query_values(self.db, 'bk', self.db.code == code)
        if bks is None or len(bks) == 0:
            return await self.get_embk(code)

        for i in range(len(bks) - 1, 0, -1):
            bk, = bks[i]
            if bk != '':
                return bk
        return await self.get_embk(code)

    @async_lru()
    async def get_concept(self, code):
        cpts = await query_values(self.db, 'cpt', self.db.code == code)
        if cpts is None or len(cpts) == 0:
            return ''
        for i in range(len(cpts) - 1, 0, -1):
            cpt, = cpts[i]
            if cpt != '':
                return cpt
        return ''

    async def merge_selected(self, sel):
        osel = next((s for s in self.wkselected if s[0] == sel[0] and s[1] == sel[1]), None)
        if not osel:
            sel[7] = await self.get_bk(sel[0])
            sel[8] = await self.get_concept(sel[0])
            self.wkselected.append(sel)
            return
        if osel[7] == '':
            osel[7] = await self.get_bk(sel[0])
        if osel[8] == '':
            osel[8] = await self.get_concept(sel[0])
        if osel[4] != sel[4]:
            osel[4] = sel[4]
        if osel[5] != sel[5]:
            osel[5] = sel[5]

    async def task_processing(self, item):
        c, zdate, sdate = item
        allkl = await self.get_kd_data(c, start=sdate)
        if allkl is None or len(allkl) == 0:
            return

        zdf = zdf_from_code(c)
        if zdf == 10 and await self.is_st_stock(c):
            zdf = 5
        mkt = [10, 20, 30, 5].index(zdf)

        i = 0
        while i < len(allkl):
            if allkl[i].time < zdate:
                i += 1
                continue
            if i == 0 and allkl[i].close == allkl[i].high and allkl[i].change * 100 >= zdf - 0.1:
                await self.merge_selected([c, allkl[i].time, 0, 0, 1, 1, 0, "", "", mkt])
                i += 1
                continue

            c0 = allkl[i-1].close
            if allkl[i].close == allkl[i].high and allkl[i].close >= zt_priceby(c0, zdf=zdf):
                days = 1
                lbc = 1
                t = i
                j = i - 1
                while j >= 0:
                    if j == 0:
                        if allkl[j].change * 100 >= zdf - 0.1:
                            lbc += 1
                            days += t
                        break
                    c0 = allkl[j-1].close
                    if allkl[j].close == allkl[j].high and allkl[j].close >= zt_priceby(c0, zdf=zdf):
                        lbc += 1
                        days += t - j
                        t = j
                        j -= 1
                        continue
                    if t - j >= 3:
                        break
                    j -= 1
                if lbc == 1:
                    days = 1
                await self.merge_selected([c, allkl[i].time, 0, 0, lbc, days, 0, "", "", mkt])
            i += 1

    async def get_hot_stocks(self, date):
        zts = await query_values(self.db, ['code', 'time', 'days', 'lbc'], self.db.time == date)
        ztdate = {}
        for c, d, days, lbc in zts:
            if c not in ztdate:
                ztdate[c] = d
            elif d > ztdate[c]:
                ztdate[c] = d

        return [[c, d, days, lbc] for c, d, days, lbc in zts if d == ztdate[c]]

    async def dump_main_stocks_zt0(self, date=None):
        if date is None:
            date = await query_aggregate('max', self.db, 'time')

        return await query_values(self.db, ['code', 'bk', 'cpt'], self.db.time == date, self.db.lbc == 1, self.db.mkt == 0)

    async def dump_by_concept(self, date, concept):
        if date is None:
            return []

        zts = await query_values(self.db, ['code', 'lbc', 'bk', 'cpt'], self.db.time == date)

        def unify_concepts(zts):
            return [[c, n, bk if con == '' else con] for c, n, bk, con in zts]

        if concept is not None:
            ztcpt = []
            for c, n, bk, con in zts:
                cons = [bk]
                if '+' in con:
                    cons = con.split('+')
                elif con != '':
                    cons = [con]
                if concept in cons:
                    ztcpt.append([c, n, bk, con])
            return unify_concepts(ztcpt)
        return unify_concepts(zts)

    async def dumpDataByDate(self, date=None):
        return await self.dump_main_stocks_zt0(date)

    async def dumpZtStocksInDays(self, n=3, fullcode=True):
        date = await query_aggregate('max', self.db, 'time')
        date = TradingDate.prev_trading_date(date, n)
        zts = await query_values(self.db, ['code'], self.db.time >= date)
        ret = [c for c, in zts] if fullcode else [c[2:] for c, in zts]
        return list(set(ret))

    async def dumpDailySteps(self, minstep=4, days=3):
        '''
        minstep: 最小连板数
        days: 查询天数
        '''
        sdate = TradingDate.prev_trading_date(TradingDate.max_traded_date(), days)
        dsteps = await query_group_counts(self.db, 'time', self.db.time > sdate, self.db.lbc >= minstep, self.db.mkt == 0)
        return [[t, c] for t, c in dsteps.items()]


class StockZdtEmotion(StockBaseSelector):
    @property
    def db(cls):
        return MdlDayZdtEmotion

    async def start_multi_task(self, date=None):
        if date is None:
            date = await query_aggregate('max', self.db, 'date')
            if date is None:
                date = ''
            else:
                date = TradingDate.next_trading_date(date)

        self.dayztcnt = await query_group_counts(MdlDayZtStocks, MdlDayZtStocks.time, MdlDayZtStocks.time >= date, MdlDayZtStocks.mkt.in_([0,1]))
        self.dayzt1cnt = await query_group_counts(MdlDayZtStocks, MdlDayZtStocks.time, MdlDayZtStocks.time >= date, MdlDayZtStocks.lbc == 1, MdlDayZtStocks.mkt.in_([0,1]))
        ztinfo = await query_values(MdlDayZtStocks, ['time', 'code'], MdlDayZtStocks.time >= date, MdlDayZtStocks.mkt.in_([0,1]))
        self.dayztinfo = {}
        for d, c in ztinfo:
            if d in self.dayztinfo:
                self.dayztinfo[d].append(c)
            else:
                self.dayztinfo[d] = [c]
        self.daydtcnt = await query_group_counts(MdlDayDtStocks, MdlDayDtStocks.time, MdlDayDtStocks.time >= date)

        sdate = min(self.dayztcnt.keys()) if len(self.dayztcnt.keys()) > 0 else date
        if len(self.daydtcnt.keys()) > 0:
            sdate = min(sdate, min(self.daydtcnt.keys()))
        values = []
        while True:
            row = [sdate]
            row.append(self.dayztcnt[sdate] if sdate in self.dayztcnt else 0)
            row.append(self.dayzt1cnt[sdate] if sdate in self.dayzt1cnt else 0)
            row.append(self.daydtcnt[sdate] if sdate in self.daydtcnt else 0)
            amt = 0
            if sdate in self.dayztinfo:
                for c in self.dayztinfo[sdate]:
                    allkl = await self.get_kd_data(c, sdate)
                    if not allkl or len(allkl) == 0:
                        logger.info(f'no kl data for {c}, {sdate}')
                        break
                    if allkl[0].time != sdate:
                        logger.info(f'invalid kl data for {c}, {sdate}')
                        break
                    amt += allkl[0].amount
            row.append(amt)
            values.append(row)
            ndate = TradingDate.next_trading_date(sdate)
            if ndate == sdate:
                break
            sdate = ndate

        self.wkselected = values
        await self.post_process()

    async def dumpDataByDate(self, date):
        if date is None:
            date = await query_aggregate('max', self.db, 'date')
        rows = await query_values(self.db, None, self.db.date >= date)
        return [list(row) for row in rows]

    async def dumpDataInDays(self, days):
        return await self.dumpDataByDate(TradingDate.prev_trading_date(TradingDate.max_traded_date(), days-1))


class StockHotStocksOpenSelector(StockBaseSelector):
    @property
    def db(self):
        return MdlHotstksOpen

    async def saveDailyHotStocksOpen(self, hstks):
        values = [{
            'date': h[0], 'code': srt.get_fullcode(h[1]), 'zdate': h[2], 'days': h[3], 'step': h[4], 'emrk': h[5]
        } for h in hstks]
        await upsert_many(self.db, values, ['date', 'code'])

    def get_top_ztstocks(self, zt_stocks):
        zt_stocks = sum(zt_stocks, [])
        step = max([x[3] for x in zt_stocks])
        zstepdic = {}
        for z in zt_stocks:
            if z[0] not in zstepdic:
                zstepdic[z[0]] = z[3]
                continue
            if z[3] > zstepdic[z[0]]:
                zstepdic[z[0]] = z[3]
        zt_stocks = [z for z in zt_stocks if z[3] == zstepdic[z[0]]]
        top_zt_stocks = []
        while step > 2:
            if len([x for x in zt_stocks if x[3] == step]) > len(top_zt_stocks) and len(top_zt_stocks) >= 8:
                break
            top_zt_stocks = [x for x in zt_stocks if x[3] >= step]
            if len(top_zt_stocks) >= 10:
                break
            step -= 1
        return top_zt_stocks

    async def start_multi_task(self):
        stks = await query_values(MdlDayZtStocks, ['code', 'time', 'days', 'lbc'], MdlDayZtStocks.mkt == 0)
        saved_dates = await query_values(self.db, ['date'])
        saved_dates = list(set([x for x, in saved_dates]))
        self.wkselected = []
        from collections import defaultdict
        grouped = defaultdict(list)
        for s in stks:
            grouped[s[1]].append(s)

        stks = list(grouped.values())
        for i in range(3, len(stks)):
            date = stks[i][0][1]
            if date in saved_dates:
                continue
            zs = self.get_top_ztstocks(stks[i-3: i])
            for z in zs:
                self.wkselected.append((date,) + z + (0,))
        self.post_process()

    async def dumpDataByDate(self, date: Optional[str] = None):
        cond = []
        if date is not None:
            cond.append(self.db.date == date)
        rows = await query_values(self.db, ['date', 'code', 'zdate', 'days', 'step', 'emrk'], *cond)
        return rows

    async def dumpDataLaterThanDate(self, date: Optional[str] = None):
        cond = []
        if date is not None:
            cond.append(self.db.date >= date)
        rows = await query_values(self.db, ['date', 'code', 'zdate', 'days', 'step', 'emrk'], *cond)
        return rows
