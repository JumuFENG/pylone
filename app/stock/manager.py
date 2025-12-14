import sys
import json
import gzip
import numpy as np
from typing import List, Tuple
from datetime import datetime
import stockrt as srt
from stockrt.sources.eastmoney import Em
from emxg import search_emxg
from app.hu import classproperty, to_cls_secucode
from app.lofig import logger
from app.db import upsert_one, upsert_many, query_one_value, query_aggregate, query_values, delete_records
from .models import MdlAllStock, MdlStockBk, MdlSMStats
from .schemas import PmStock
from .history import (
    Network, array_to_dict_list,
    Khistory as khis, FflowHistory as fhis, StockBkMap, StockBkChanges, StockClsBkChanges, StockZtDaily,
    StockList)
from .date import TradingDate


class AllStocks:
    @classproperty
    def db(cls):
        return MdlAllStock

    @classmethod
    async def read_all(cls):
        return await query_values(cls.db)

    @classmethod
    async def load_info(cls, stock: PmStock):
        code = srt.get_fullcode(stock.code)
        qt = srt.quotes(code).get(code, {})

        update_data = {
            "code": code,
            "name": qt["name"]
        }
        if stock.typekind:
            update_data["typekind"] = stock.typekind
        if stock.setup_date:
            update_data["setup_date"] = stock.setup_date
        if stock.quit_date:
            update_data["quit_date"] = stock.quit_date

        await upsert_one(cls.db, update_data, ["code"])

    @classmethod
    async def remove(cls, code):
        await delete_records(cls.db, cls.db.code == code)

    @classmethod
    async def stock_name(cls, code):
        await query_one_value(cls.db, 'name', cls.db.code == code)

    @classmethod
    async def update_kline_data(cls, kltype='d', sectype: str = None):
        '''
        更新表中所有指数的K线数据
        Args:
            kltype: K线类型 (d, w, m) 指数只保存这几种K线数据
        '''
        rows = await query_values(cls.db)
        target_types = ['Index', 'ETF', 'LOF', 'ABStock', 'BJStock', 'TSStock']
        if sectype in target_types:
            target_code = [row.code for row in rows if row.typekind == sectype]
        else:
            if kltype == 'd':
                stock_cns = {r.code: r.name for r in rows if r.code.startswith(('sh', 'sz', 'bj')) and r.typekind in ('ABStock', 'BJStock')}
                target_code = await cls.update_stock_daily_kline_and_fflow(stock_cns)
            else:
                target_code = [row.code for row in rows if row.typekind == 'ABStock' or row.typekind == 'BJStock']

        if not target_code:
            return

        cls.update_klines_by_code(target_code, kltype)

    @classmethod
    def update_klines_by_code(cls, stocks, kltype: str='d') -> List[str]:
        """
        Updates the K-line data for a list of stock codes based on the specified K-line type.

        Args:
            stocks: A list of stock codes for which the K-line data needs to be updated.
            kltype: A string representing the K-line type (e.g., 'd' for daily).

        Returns:
            A list of stock codes for which the K-line data was not updated.

        """
        uplens = {c: khis.count_bars_to_updated(c, kltype) for c in stocks}
        fixlens = {}
        for c,l in uplens.items():
            if l == 0:
                continue
            if l not in fixlens:
                fixlens[l] = []
            fixlens[l].append(c)
        if not fixlens:
            return
        if 1 in fixlens and len(fixlens[1]) > 100:
            logger.warning('too many stocks to update for 1 day, please call update_kline_data("d") first!')
            return

        ofmt = srt.set_array_format('np')
        # srt.set_default_sources('dklines', 'dklines', ('xueqiu', 'ths', 'eastmoney', 'tdx', 'sina'), True)
        srt.set_default_sources('dklines', 'dklines', ('xueqiu', 'ths', 'tdx', 'sina'), True)
        klines = {}
        for l, codes in fixlens.items():
            if l == sys.maxsize:
                klines.update(srt.fklines(codes, kltype, 0))
            else:
                klines.update(srt.klines(codes, kltype, l+2, 0))
        for c in klines:
            if c in stocks:
                khis.save_kline(c, kltype, klines[c])
        srt.set_array_format(ofmt)
        return [c for c in sum(fixlens.values(), []) if c not in klines or len(klines[c]) == 0 or TradingDate.calc_trading_days(klines[c][-1]['time'], TradingDate.max_trading_date()) > 20]

    @classmethod
    async def update_stock_daily_kline_and_fflow(cls, cns: dict = {}):
        '''
        通过涨幅榜更新当日K线数据和资金流数据，必须盘后执行，盘中获取的收盘价为当时的最新价

        :return: 有最新价但是与数据库中保存的数据不连续的股票列表，需单独获取股票K线
        '''
        # TODO: Testing, remove later
        is_test = True
        if is_test:
            logger.warning('testing, skip!')
            return cns.keys()
        srt.set_default_sources('stock_list', 'stocklistapi', ('eastmoney', 'sina'), False)
        stock_list = srt.stock_list()
        if 'all' not in stock_list:
            return

        stock_list = stock_list['all']
        result = {}
        for stock in stock_list:
            c = stock['code'][-6:]
            result[c] = {** stock}
            if 'time' not in stock:
                result[c]['time'] = TradingDate.max_trading_date()
            if 'amplitude' not in stock:
                if stock['lclose'] == 0:
                    result[c]['amplitude'] = 0
                    continue
                result[c]['amplitude'] = (stock['high'] - stock['low']) / stock['lclose']
        unconfirmed = []
        for c, kl in result.items():
            mxdate = khis.max_date(c, 'd')
            if TradingDate.prev_trading_date(TradingDate.max_trading_date()) == mxdate:
                dtypes = [('time', 'U10'), ('open', 'float'), ('high', 'float'), ('low', 'float'), ('close', 'float'),
                          ('volume', 'int64'), ('amount', 'int64'), ('change', 'float'), ('change_px', 'float'), ('amplitude', 'float')]
                npkl = np.array([(kl['time'], kl['open'], kl['high'], kl['low'], kl['close'], kl['volume'], kl['amount'], kl['change'], kl['change_px'], kl['amplitude'])], dtype=dtypes)
                khis.save_kline(c, 'd', npkl)
            else:
                unconfirmed.append(c)
            mxfdate = fhis.max_date(c)
            if 'main' in kl and TradingDate.prev_trading_date(TradingDate.max_trading_date()) == mxfdate:
                fhis.save_fflow(c, [[kl['time'], kl['main'], kl['small'], kl['middle'], kl['big'], kl['super'], kl['mainp'], kl['smallp'], kl['middlep'], kl['bigp'], kl['superp']]])
            if c in cns and cns[c] != kl['name']:
                await upsert_one(cls.db, {"code": c, "name": kl['name']}, ["code"])
        return unconfirmed

    @classmethod
    async def update_stock_fflow(cls):
        rows = await query_values(cls.db)
        for r in rows:
            if r.code.startswith(('sh', 'sz', 'bj')) and r.typekind in ('ABStock', 'BJStock'):
                await fhis.update_fflow(r.code)

    @classmethod
    async def is_quited(cls, code):
        quit_date = await query_one_value(cls.db, 'quit_date', cls.db.code == srt.get_fullcode(code))
        return quit_date is not None

    @classmethod
    async def load_new_stocks(cls, sdate=None):
        if sdate is None:
            sdate = await query_aggregate('max', cls.db, 'setup_date')
        if not sdate:
            sdate = "20000101"
        sdate = int(sdate.replace('-', ''))
        clist = Em.qt_clist(
            fs='m:0+f:8,m:1+f:8,m:0+f:81+s:262144', fields='f12,f13,f14,f21,f26', fid='f26',
            qtcb=lambda data: min([int(item['f26']) for item in data]) < sdate
        )

        newstocks = []
        for nsobj in clist:
            c = nsobj['f12']
            n = nsobj['f14']
            ipodays = (datetime.now() - datetime.strptime(str(nsobj['f26']), "%Y%m%d")).days
            if ipodays > 10:
                continue
            code = srt.get_fullcode(c)
            tp = 'BJStock' if code.startswith('bj') else 'ABStock'
            d = str(nsobj['f26'])
            newstocks.append({
                'code': code, 'name': n, 'typekind': tp, 'setup_date': d[0:4] + '-' + d[4:6] + '-' + d[6:]
            })

        if newstocks:
            await upsert_many(cls.db, newstocks, ['code'])

    @classmethod
    async def load_a_stocks(cls):
        def get_stocks_of(fs, tp, pre):
            clist = Em.qt_clist(
                fs, fields='f12,f13,f14,f21,f26', fid='f26'
            )
            stocks = []
            for s in clist:
                d = str(s['f26'])
                stocks.append({
                    'code': pre + s['f12'], 'name': s['f14'], 'typekind': tp, 'setup_date': d[0:4] + '-' + d[4:6] + '-' + d[6:]
                })
            return stocks

        astocks = []
        astocks.extend(get_stocks_of('m:1+t:2+f:!2,m:1+t:23+f:!2', 'ABStock', 'sh'))
        astocks.extend(get_stocks_of('m:1+t:2+f:2,m:1+t:23+f:2', 'TSStock', 'sh'))
        astocks.extend(get_stocks_of('m:0+t:6+f:!2,m:0+t:80+f:!2', 'ABStock', 'sz'))
        astocks.extend(get_stocks_of('m:0+t:6+f:2,m:0+t:80+f:2', 'TSStock', 'sz'))
        astocks.extend(get_stocks_of('m:0+t:81+s:262144+f:!2', 'BJStock', 'bj'))
        astocks.extend(get_stocks_of('m:0+t:81+s:262144+f:2', 'TSStock', 'bj'))

        await upsert_many(cls.db, astocks, ['code'], 3000)

    @classmethod
    async def get_bkstocks(self, bks):
        if isinstance(bks, str):
            bks = [bks]
        bks = ','.join(['b:' + bk for bk in bks])
        clist = Em.qt_clist(fs=bks, fields='f12,f13,f14,f26')
        return clist

    @classmethod
    async def load_all_funds(cls):
        def get_stocks(bks, tp):
            stocks = cls.get_bkstocks(bks)
            bkstks = []
            for s in stocks:
                d = str(s['f26'])
                bkstks.append({
                    'code': srt.get_fullcode(s['f12']), 'name': s['f14'], 'typekind': tp, 'setup_date': d[0:4] + '-' + d[4:6] + '-' + d[6:]
                })
            return bkstks
        funds = []
        funds.extend(get_stocks(['MK0021', 'MK0022', 'MK0023', 'MK0024'], 'ETF'))
        funds.extend(get_stocks(['MK0404', 'MK0405', 'MK0406', 'MK0407'], 'LOF'))
        await upsert_many(cls.db, funds, ['code'])

    @classmethod
    async def update_purelost4up(cls):
        '''
        连续4个季度亏损大于1000万元
        '''

        pdata = search_emxg('连续4个季度亏损大于1000万元')
        await StockList.save_stocks('purelost4up', [srt.get_fullcode(code[:6]) for code in pdata['代码']])

    @classmethod
    async def get_purelost4up(cls):
        return await StockList.get_stocks('purelost4up')


class AllBlocks:
    @classproperty
    def db(cls):
        return MdlStockBk

    @classproperty
    def bkmap(cls) -> StockBkMap:
        return StockBkMap()

    @classproperty
    def bkchanges(cls) -> StockBkChanges:
        return StockBkChanges()

    @classproperty
    def clsbkchanges(cls) -> StockClsBkChanges:
        return StockClsBkChanges()

    @classmethod
    async def read_all(cls):
        return await query_values(cls.db)

    @classmethod
    async def read_ignored(cls):
        return await query_values(cls.db, ['code'], cls.db.chgignore == 1)

    @classmethod
    async def load_info(cls, code, name=None):
        update_data = {
            "code": code
        }
        bkname = await query_one_value(cls.db, "name", cls.db.code == code)
        if name:
            update_data["name"] = name
        if bkname is None or (name and bkname != name):
            await upsert_one(cls.db, update_data, ["code"])

        cls.bkmap.setCode(code)
        if code.startswith('BK'):
            cls.bkmap.bkstocks = await AllStocks.get_bkstocks(code)
            await cls.bkmap.saveFetched()
        else:
            await cls.bkmap.getNext()

    @classmethod
    async def ignore_bk(cls, code, ignore=1):
        await upsert_one(cls.db, {'code': code, 'chgignore': ignore}, ['code'])

    @classmethod
    async def update_bk_changed(cls):
        bk_chgs = await cls.bkchanges.getLatestChanges()
        bk_chgs += await cls.clsbkchanges.getLatestChanges()
        return bk_chgs

    @classmethod
    async def update_bk_changed_in5days(cls):
        await cls.bkchanges.updateBkChangedIn5Days()
        await cls.clsbkchanges.updateBkChangedIn5Days()

    @classmethod
    async def get_topbks(cls):
        embkkicks = await cls.bkchanges.topbks_to_date()
        clsbkkicks = await cls.clsbkchanges.topbks_to_date()
        return {**embkkicks, **clsbkkicks}


class StockMarketStats():
    topbks = None
    hotstocks = None
    lateststats = None

    @classproperty
    def db(cls):
        return MdlSMStats

    @classmethod
    async def get_topbks(self):
        self.topbks = await AllBlocks.get_topbks()
        self.bkstocklist = {}
        for bk in self.topbks:
            self.bkstocklist[bk] = [to_cls_secucode(c) for c in StockBkMap.bk_stocks(bk)]

        kickdate = TradingDate.prev_trading_date(TradingDate.max_traded_date(), 3) if len(self.topbks.values()) == 0 else min([bk['kickdate'] for bk in self.topbks.values()])
        szt = StockZtDaily()
        ztstks = await szt.get_hot_stocks(kickdate)
        mdate = TradingDate.max_trading_date()
        self.hotstocks = {to_cls_secucode(c): {'code': to_cls_secucode(c), 'date': d, 'days': days, 'lbc': lbc, 'ndays': TradingDate.calc_trading_days(d, mdate) - 1} for c,d,days,lbc in ztstks}

    @classmethod
    def zt_lbc_sort_key(self, secu):
        code = secu['secu_code']
        if code not in self.hotstocks:
            return -1, -1
        days = self.hotstocks[code]['days'] + self.hotstocks[code]['ndays']
        if days == 0:
            return -1, self.hotstocks[code]['lbc']
        return days, self.hotstocks[code]['lbc'] / days

    @classmethod
    def connect_bk_stock(self, stats):
        zt_stocks = [z['secu_code'] for z in stats['stocks']['zt_yzb'] + stats['stocks']['zt']]
        for bk in self.topbks:
            self.topbks[bk]['zt_stocks'] = [s for s in self.bkstocklist[bk] if s in zt_stocks]
        stats['plates'] = sorted(self.topbks.values(), key=lambda x: len(x['zt_stocks']), reverse=True)

        stocks = {}
        for k in ['zt_yzb', 'zt', 'up', 'down', 'dt']:
            stocks[k] = []
            pstocks = [[] for _ in range(len(stats['plates']))]
            for zs in stats['stocks'][k]:
                inplates = False
                for i in range(0, len(stats['plates'])):
                    if zs['secu_code'] in self.bkstocklist[stats['plates'][i]['code']]:
                        pstocks[i].append(zs)
                        inplates = True
                        break
                if not inplates:
                    if len(pstocks) == 0:
                        pstocks.append([])
                    pstocks[-1].append(zs)

            for i in range(len(pstocks)):
                if len(pstocks[i]) < 2:
                    continue
                pstocks[i] = sorted(pstocks[i], key=self.zt_lbc_sort_key, reverse=True)
            stocks[k] = []
            for zs in pstocks:
                stocks[k] += zs
        stats['stocks'] = stocks

        estocks = []
        for k in ['zt_yzb', 'zt', 'up', 'down', 'dt']:
            for zs in stats['stocks'][k]:
                estocks.append(zs['secu_code'])
        stockextras = {}
        for s in estocks:
            if s in self.hotstocks:
                stockextras[s] = self.hotstocks[s]
            plist = []
            for p in stats['plates']:
                if s in self.bkstocklist[p['code']]:
                    plist.append(p['code'])
            if len(plist) > 0:
                if s not in stockextras:
                    stockextras[s] = {}
                stockextras[s]['plates'] = plist
        stats['stockextras'] = stockextras
        return stats

    @classmethod
    async def execute(self):
        try:
            if self.topbks is None or self.hotstocks is None:
                await self.get_topbks()
            zdfranks = []
            fs = 'm:0+t:6+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:81+s:262144+f:!2'
            zdfranks.extend(Em.qt_clist(fs, fields='f2,f3,f12,f13,f18', fid='f3', po=1, qtcb=lambda data: any(d['f3'] < 8 for d in data)))
            zdfranks.extend(Em.qt_clist(fs, fields='f2,f3,f12,f13,f18', fid='f3', po=0, qtcb=lambda data: any(d['f3'] > -8 for d in data)))
            up_down_stocks = []
            for rkobj in zdfranks:
                c = rkobj['f2']   # 最新价
                zd = rkobj['f3']  # 涨跌幅
                if c == '-' or zd == '-':
                    continue
                cd = rkobj['f12'] # 代码
                if zd >= 8 or zd <= -8:
                    code = srt.get_fullcode(cd)
                    up_down_stocks.append(code)

            sm_statistics = {'time': datetime.now().strftime('%Y-%m-%d %H:%M'), 'stocks': {'zt_yzb':[], 'zt':[], 'dt':[], 'up':[], 'down':[]}}
            fields = 'open_px,av_px,high_px,low_px,change,change_px,down_price,cmc,business_amount,business_balance,secu_name,secu_code,trade_status,secu_type,preclose_px,up_price,last_px'
            for i in range(0,len(up_down_stocks),200):
                ccodes = ','.join([to_cls_secucode(c) for c in up_down_stocks[i: i+200]])
                bUrl = f'https://x-quote.cls.cn/quote/stocks/basic?app=CailianpressWeb&fields={fields}&os=web&secu_codes={ccodes}&sv=8.4.6'
                sbasics = json.loads(Network.fetch_url(bUrl, Network.get_headers({'Host': 'x-quote.cls.cn'})))
                if 'data' in sbasics:
                    for secu in sbasics['data']:
                        sbasic = sbasics['data'][secu]
                        o,h,l,c = sbasic['open_px'], sbasic['high_px'], sbasic['low_px'], sbasic['last_px']
                        u,d,lc = sbasic['up_price'], sbasic['down_price'], sbasic['preclose_px']
                        if c == u:
                            if h == l:
                                # 一字
                                sm_statistics['stocks']['zt_yzb'].append(sbasic)
                            else:
                                # 涨停
                                sm_statistics['stocks']['zt'].append(sbasic)
                        elif c == d:
                            sm_statistics['stocks']['dt'].append(sbasic)
                            # 跌停
                        elif sbasic['change'] >= 0.08:
                            sm_statistics['stocks']['up'].append(sbasic)
                            # 大涨
                        elif sbasic['change'] <= -0.08:
                            # 大跌
                            sm_statistics['stocks']['down'].append(sbasic)

            sm_statistics = self.connect_bk_stock(sm_statistics)
            dsm, tsm = sm_statistics['time'].split(' ')
            await self.save_stats([[dsm, tsm, sm_statistics]])
            if self.lateststats is None:
                self.lateststats = [sm_statistics]
            else:
                self.lateststats.append(sm_statistics)
        except Exception as e:
            logger.info(e)

    @classmethod
    async def save_stats(self, stats):
        values = []
        for d,t,s in stats:
            sstr = json.dumps(s)
            cmpsstr = gzip.compress(sstr.encode('utf-8'))
            values.append([d, t, cmpsstr])

        if len(values) > 0:
            await upsert_many(self.db, array_to_dict_list(self.db, values), ['date', 'time'])

    @classmethod
    async def read_stats(self, date=None):
        if date is None:
            date = await query_aggregate('max', self.db, 'date')
        dstats = await query_values(self.db, 'stats', self.db.date == date)
        stats = []
        for s, in dstats:
            stats.append(json.loads(gzip.decompress(s).decode('utf-8')))
        return stats

    @classmethod
    async def latest_stats(self):
        if self.lateststats is None:
            self.lateststats = await self.read_stats()
        return self.lateststats
