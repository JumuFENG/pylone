from typing import Optional
from app.lofig import logger
from app.stock.history import StockDtInfo
from app.stock.models import MdlAllStock, MdlDayDtMap
from app.stock.date import TradingDate
from app.db import query_values, query_one_value, query_aggregate, upsert_one
from .stock_base_selector import StockBaseSelector


class StockDtMap(StockBaseSelector):
    '''跌停进度表
    '''
    def __init__(self) -> None:
        self.dtdtl = {}

    @property
    def db(self):
        return MdlDayDtMap
    
    @property
    def dtinfo(self):
        return StockDtInfo()

    async def getdtl(self, code, step):
        dtl = []
        for i in range(1, step + 1):
            detail = await query_values(self.db, ['time'], self.db.code == code, self.db.step == i, self.db.success == 1)
            if len(detail) > 0:
                ctdate, = detail[-1]
                if len(dtl) == 0 or ctdate > dtl[0]['date']:
                    dtl.append({'ct': i, 'date': ctdate})
        return dtl

    async def start_multi_task(self, mpdate):
        mxdate = TradingDate.max_trading_date()
        premap = await self.dumpDataByDate(mpdate)
        if mpdate is None:
            mpdate = await query_aggregate('min', self.dtinfo.db, 'time')
        if premap and premap['date'] != mpdate:
            logger.info('data invalid %s', premap)
            return

        if premap is None:
            premap = []
        else:
            premap = premap['data']
        while mpdate < mxdate:
            nxdate = TradingDate.next_trading_date(mpdate)
            nxdt = await self.dtinfo.dumpDataByDate(nxdate)
            if nxdt is None or nxdate != nxdt['date']:
                logger.info('dt data for %s not invalid %s', nxdate, nxdt)
                break

            nmap = []
            dt1 = [c for c,*_ in nxdt['pool']] if nxdt is not None and 'pool' in nxdt else []
            for c in dt1:
                oldmp = False
                premapbk = []
                for code, step, suc in premap:
                    if code == c:
                        oldmp = True
                        nmap.append([nxdate, (step + 1 if suc==1 else step), c, 1])
                    else:
                        premapbk.append([code, step, suc])
                if not oldmp:
                    nmap.append([nxdate, 1, c, 1])
                premap = premapbk

            for code, step, suc in premap:
                if code not in self.dtdtl:
                    self.dtdtl[code] = await self.getdtl(code, step)

                dtl = self.dtdtl[code]
                kd = await self.get_kd_data(code, start=dtl[-1]['date'])
                if not kd:
                    continue
                lkl = [k for k in kd if k.time == nxdate]
                if len(lkl) != 1:
                    if lkl[-1].time < nxdate:
                        ts = await query_one_value(MdlAllStock, MdlAllStock.quit_date, MdlAllStock.code == code, MdlAllStock.typekind == 'TSStock')
                        if ts is None:
                            nmap.append([nxdate, (step + 1 if suc==1 else step), code, 0])
                else:
                    lkl = lkl[0]
                    if lkl.time != nxdate:
                        nmap.append([nxdate, (step + 1 if suc==1 else step), code, 0])
                    else:
                        dkl = kd[0]
                        if lkl.low - dkl.close * 0.9 <= 0:
                            nmap.append([nxdate, (step + 1 if suc==1 else step), code, 1])
                        elif lkl.close - dkl.close * 1.08 <= 0 and len(kd) <= 4:
                            nmap.append([nxdate, (step + 1 if suc==1 else step), code, 0])

            self.wkselected = []
            premap = []
            for d, step, c, suc in nmap:
                premap.append([c, step, suc])
                mp = await query_aggregate('count', self.db, 'code', self.db.time == d, self.db.code == c)
                if mp == 1:
                    await upsert_one(self.db, {'time': d, 'code': c, 'step': step, 'success': suc}, ['time', 'code'])
                else:
                    self.wkselected.append([d, c, step, suc])

            await self.post_process()
            mpdate = nxdate

    async def dumpDataByDate(self, date = None):
        if date is None:
            date = await query_aggregate('max', self.db, 'time')

        if date is None:
            return None

        while date <= TradingDate.max_trading_date():
            mp = await query_values(self.db, ['code', 'step', 'success'], self.db.time == date)
            if mp is not None and len(mp) > 0:
                data = {'date': date}
                dtmap = []
                for code, step, suc in mp:
                    dtmap.append([code, step, suc])
                data['data'] = dtmap
                return data
            date = TradingDate.next_trading_date(date)

        return await self.dumpDataByDate()
