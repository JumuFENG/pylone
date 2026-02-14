import sys
import json
import gzip
import emxg
import stockrt as srt
from typing import List, Tuple
from traceback import format_exc
from datetime import datetime
from stockrt.sources.eastmoney import Em
from app.hu import classproperty, to_cls_secucode, time_stamp
from app.hu.network import Network as net
from app.lofig import logger
from app.db import upsert_one, upsert_many, query_one_value, query_one_record, query_aggregate, query_values, delete_records
from .models import MdlAllStock, MdlStockBk, MdlStockBkMap, MdlSMStats, MdlStockChanges
from .schemas import PmStock
from .history import (
    Network, array_to_dict_list,
    Khistory as khis, FflowHistory as fhis, StockBkMap, StockBkChanges, StockClsBkChanges,
    StockList)
from app.selectors import SelectorsFactory as sfac
from .date import TradingDate
from .storage.sqlite import tss


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

        await cls.update_stock(update_data)

    @classmethod
    async def update_stock(cls, stock: dict):
        await upsert_one(cls.db, stock, ["code"])

    @classmethod
    async def remove(cls, code):
        await delete_records(cls.db, cls.db.code == code)

    @classmethod
    async def stock_name(cls, code):
        return await query_one_value(cls.db, 'name', cls.db.code == code)

    @classmethod
    async def is_exists(cls, code):
        return code == await query_one_value(cls.db, 'code', cls.db.code == code)

    @classmethod
    async def get_stock(cls, code):
        return await query_one_record(cls.db, cls.db.code == code)

    @classmethod
    async def update_kline_data(cls, kltype='d', sectype: str = None):
        '''
        更新表中所有指数的K线数据
        Args:
            kltype: K线类型 (d, w, m) 指数只保存这几种K线数据
        '''
        rows = await query_values(cls.db)
        target_types = ['Index', 'ETF', 'LOF', 'ABStock', 'BJStock', 'TSStock']
        mxdate = TradingDate.max_trading_date()
        if sectype in target_types:
            target_code = [row.code for row in rows if row.typekind == sectype and (row.setup_date is None or row.setup_date <= mxdate)]
        else:
            if kltype == 'd':
                stock_cns = {r.code: r.name for r in rows if r.code.startswith(('sh', 'sz', 'bj')) and r.typekind in ('ABStock', 'BJStock') and (r.setup_date is None or r.setup_date <= mxdate)}
                target_code = await cls.update_stock_daily_kline_and_fflow(stock_cns)
            else:
                target_code = [row.code for row in rows if row.typekind in ('ABStock', 'BJStock') and (row.setup_date is None or row.setup_date <= mxdate)]

        if not target_code:
            return

        await cls.update_klines_by_code(target_code, kltype)
        if 'sh000001' in target_code:
            mxdate = await khis.max_date('sh000001')
            TradingDate.update_max_traded_date(mxdate)

    @classmethod
    async def update_klines_by_code(cls, stocks, kltype: str='d') -> List[str]:
        """
        Updates the K-line data for a list of stock codes based on the specified K-line type.

        Args:
            stocks: A list of stock codes for which the K-line data needs to be updated.
            kltype: A string representing the K-line type (e.g., 'd' for daily).

        Returns:
            A list of stock codes for which the K-line data was not updated.

        """
        uplens = {c: await khis.count_bars_to_updated(c, kltype) for c in stocks}
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

        async def update_and_save(codes, length, fkl=False):
            # tdx = srt.rtsource('tdx')
            # func = tdx.fklines if fkl else tdx.klines
            func = srt.fklines if fkl else srt.klines
            args = (codes, kltype, 0) if fkl else (codes, kltype, length, 0)
            klines = func(*args)
            for c in klines:
                if c in stocks:
                    await khis.save_kline(c, kltype, klines[c])

        ofmt = srt.set_array_format('json')
        # srt.set_default_sources('dklines', 'dklines', ('xueqiu', 'ths', 'eastmoney', 'tdx', 'sina'), True)
        srt.set_default_sources('dklines', 'dklines', ('xueqiu', 'ths', 'tdx', 'sina'), True)
        ksize = 500
        for l, codes in fixlens.items():
            if l == sys.maxsize:
                for i in range(0, len(codes), ksize):
                    batch = codes[i:i+ksize]
                    await update_and_save(batch, l, fkl=True)
            else:
                for i in range(0, len(codes), ksize):
                    batch = codes[i:i+ksize]
                    await update_and_save(batch, l, fkl=False)
        srt.set_array_format(ofmt)

    @classmethod
    async def update_stock_daily_kline_and_fflow(cls, cns: dict = {}):
        '''
        通过涨幅榜更新当日K线数据和资金流数据，必须盘后执行，盘中获取的收盘价为当时的最新价

        :return: 有最新价但是与数据库中保存的数据不连续的股票列表，需单独获取股票K线
        '''
        sina = srt.rtsource('sina')
        stock_list = sina.stock_list()
        if 'all' not in stock_list:
            return

        stock_list = stock_list['all']
        stock_list_bj = sina.stock_list('bjs')
        if 'bjs' in stock_list_bj:
            stock_list.extend(stock_list_bj['bjs'])
        result = {}
        for stock in stock_list:
            c = stock['code']
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
            if kl['open'] == 0 or kl['high'] == 0 or kl['low'] == 0 or kl['close'] == 0:
                logger.warning(f'invalid kline for {c}')
                continue
            mxdate = await khis.max_date(c, 'd')
            if TradingDate.prev_trading_date(TradingDate.max_trading_date()) == mxdate:
                await khis.save_kline(c, 'd', [kl])
            else:
                unconfirmed.append(c)
            mxfdate = await fhis.max_date(c)
            if 'main' in kl and TradingDate.prev_trading_date(TradingDate.max_trading_date()) == mxfdate:
                await fhis.save_fflow(c, [kl])
            if c in cns and cns[c] != kl['name']:
                await upsert_one(cls.db, {"code": c, "name": kl['name']}, ["code"])
        return unconfirmed

    @classmethod
    async def update_stock_fflow(cls):
        rows = await query_values(cls.db)
        for r in rows:
            if r.code.startswith(('sh', 'sz', 'bj')) and r.typekind in ('ABStock', 'BJStock') and (r.setup_date is None or r.setup_date <= TradingDate.max_trading_date()):
                await fhis.update_fflow(r.code)

    @classmethod
    async def update_stock_transactions(cls):
        if TradingDate.today() != TradingDate.max_trading_date():
            logger.warning(f'transactions should updated on the same day of trading day!')
            return
        rows = await query_values(cls.db)
        stocks = [r.code for r in rows if r.code.startswith(('sh', 'sz', 'bj')) and r.typekind in ('ABStock', 'BJStock') and (r.setup_date is None or r.setup_date <= TradingDate.max_trading_date())]
        await cls.update_transactions_by_code(stocks)

    @classmethod
    async def update_transactions_by_code(cls, stocks: list = None):
        # 使用trans_adapter获取最新交易时间
        stocks_with_time = []
        for s in stocks:
            max_time = await tss.get_latest_time(s)
            if max_time and max_time < TradingDate.max_trading_date():
                stocks_with_time.append(s)
        stocks = stocks_with_time
        if not stocks:
            logger.info('no stocks need to update transactions')
            return
        if TradingDate.trading_started() and not TradingDate.trading_ended():
            logger.warning(f'transactions should not updated during trading!')
            return

        tsize = 1000
        date = TradingDate.max_traded_date()
        tdx = srt.rtsource('tdx')
        ofmt = srt.set_array_format('list')
        for i in range(0, len(stocks), tsize):
            batch = stocks[i:i+tsize]
            trans = tdx.transactions(batch)
            for k, v in trans.items():
                if v is None:
                    logger.warning(f'no transactions for {k}')
                    continue
                if len(v) < 10:
                    logger.warning(f'not enough transactions for {k} {v}')
                    continue
                cols = ['time', 'price', 'volume', 'num', 'bs'] if len(v[0]) == 5 else ['time', 'price', 'volume', 'bs']
                trdata = [tuple([t[0] if ' ' in t[0] else f'{date} {t[0]}'] + t[1:]) for t in v]
                trans_dicts = []
                for t in trdata:
                    trans_dict = dict(zip(cols, t))
                    trans_dicts.append(trans_dict)

                await tss.save_transaction(k, trans_dicts)
            logger.info(f'saved transactions for stocks: {i} - {i+len(batch)} / {len(stocks)}')
        srt.set_array_format(ofmt)

    @classmethod
    async def is_quited(cls, code):
        srec = await query_one_record(cls.db, cls.db.code == srt.get_fullcode(code))
        return srec.typekind == 'TSStock' or srec.quit_date

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
            logger.info('load %d new stocks', len(newstocks))
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
    def get_bkstocks(self, bks):
        if isinstance(bks, str):
            bks = [bks]
        bks = ','.join(['b:' + bk for bk in bks])
        clist = Em.qt_clist(fs=bks, fields='f12,f13,f14,f26')
        return clist

    @classmethod
    def get_stocks_zdfrank(cls, minzdf=None):
        if minzdf is None:
            stocks = srt.stock_list()
            if stocks is None or 'all' not in stocks:
                return []
            return stocks['all']

        try:
            result = cls.get_emxg_stock_zdfrank(minzdf=minzdf)
            if not result:
                clist = Em.qt_clist(
                    fs='m:0+t:6+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:81+s:262144+f:!2',
                    fields='f1,f2,f3,f4,f5,f6,f15,f16,f17,f18,f12,f13,f14',
                    fid='f3', po=1 if minzdf > 0 else 0,
                    qtcb=lambda data: any(abs(d['f3']) < abs(minzdf) for d in data)
                )
                if not clist:
                    raise Exception('No data from Em.qt_clist')
                result = [{
                    'code': srt.get_fullcode(s['f12']),
                    'name': s['f14'],
                    'close': float(s['f2']),
                    'high': float(s['f15']) if s['f15'] != '-' else 0,
                    'low': float(s['f16']) if s['f16'] != '-' else 0,
                    'open': float(s['f17']) if s['f17'] != '-' else 0,
                    'lclose': float(s['f18']),
                    'change_px': float(s['f4']),
                    'change': float(s['f3']) / 100,
                    'volume': (int(s['f5']) if s['f5'] != '-' else 0) * 100,
                    'amount': float(s['f6']) if s['f6'] != '-' else 0
                } for s in clist if s['f2'] != '-' and s['f18'] != '-']
        except Exception as e:
            logger.warning(f'get_stocks_zdfrank error: {e}')

            clist = cls.get_stocks_zdfrank()
            minzdf /= 100
            if minzdf < 0:
                clist = [s for s in clist if s['change'] <= minzdf]
                clist = list(reversed(clist))
            elif minzdf > 0:
                clist = [s for s in clist if s['change'] >= minzdf]

            result = [{
                'code': srt.get_fullcode(s['code']),
                'name': s['name'],
                'close': float(s['close']),
                'high': float(s.get('high', 0)),
                'low': float(s.get('low', 0)),
                'open': float(s.get('open', 0)),
                'lclose': float(s.get('lclose', 0)),
                'change_px': float(s.get('change_px', 0)),
                'change': float(s.get('change', 0)),
                'volume': int(s.get('volume', 0)),
                'amount': float(s.get('amount', 0))
            } for s in clist]
        return result

    @staticmethod
    def get_emxg_stock_zdfrank(minzdf=None):
        if minzdf is None:
            return []

        pdata = emxg.search(keyword=f'涨跌幅>={minzdf}%' if minzdf > 0 else f'涨跌幅=<{minzdf}%')
        pdata = pdata.rename(columns = {
            '代码': 'code', '名称': 'name', '股票简称': 'name', '最新价': 'close', '涨跌额': 'change_px', '涨跌幅': 'change',
            '涨跌幅:前复权': 'change', '成交量(股)': 'volume', '成交量': 'volume', '开盘价:前复权': 'open', '开盘价': 'open',
            '最高价:前复权': 'high', '最高价(日线不复权)': 'high', '最低价:前复权': 'low', '最低价(日线不复权)': 'low', '成交额': 'amount'
        })
        pdata = pdata.assign(code=lambda x: x['code'].apply(srt.get_fullcode) if hasattr(x['code'], 'apply') else srt.get_fullcode(x['code']))
        if 'lclose' not in pdata.columns:
            if 'change_px' not in pdata.columns:
                pdata = pdata.assign(lclose=lambda x: x['close'] / (1 + x['change']))
                pdata = pdata.assign(change_px=lambda x: x['close'] - x['lclose'])
            else:
                pdata = pdata.assign(lclose=lambda x: x['close'] - x['change_px'])

        if 'amount' not in pdata.columns:
            pdata = pdata.assign(amount=lambda x: x['close'] * x['volume'])
        if 'open' not in pdata.columns:
            pdata = pdata.assign(open=lambda x: x['lclose'])
        result = pdata[['code', 'name', 'close', 'high', 'low', 'open', 'change', 'volume', 'amount', 'change_px', 'lclose']].to_dict('records')
        return result

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
    async def check_stock_quit(cls, codes):
        # https://xueqiu.com/query/v1/suggest_stock.json?q=SZ000046&count=5
        # https://q.stock.sohu.com/suggest/search/all?type=all&count=10&terminal=pc&callback=&keyword=
        for code in codes:
            url = f'https://q.stock.sohu.com/suggest/search/all?type=all&count=10&terminal=pc&callback=&keyword={code[-6:]}&_={time_stamp()}'
            response = net.fetch_url(url)
            data = json.loads(response)
            if data and data['code'] == 200 and 'data' in data:
                for s in data['data']:
                    if s['typeId'] == '001001' and s['id'] == f'cn_{code[-6:]}' and s['status'] == 'D':
                        logger.info('%s %s quit', code, s['name'])
                        await upsert_one(cls.db, {'code': code, 'name': s['name'], 'typekind': 'TSStock'}, ['code'])

    @classmethod
    async def update_purelost4up(cls):
        '''
        连续4个季度亏损大于1000万元
        '''

        pdata = emxg.search('连续4个季度亏损大于1000万元')
        pdata = pdata.rename(columns={'代码': 'code'})
        await StockList.save_stocks('purelost4up', [srt.get_fullcode(code[:6]) for code in pdata['code']])

    @classmethod
    async def get_purelost4up(cls):
        return await StockList.get_stocks('purelost4up')

    @classmethod
    async def get_stock_changes(cls, code, start=None):
        if start is None:
            start = await query_aggregate('max', MdlStockChanges, 'time')
            start = start.split()[0]
        cond = [MdlStockChanges.time >= start]
        if code is not None:
            cond.append(MdlStockChanges.code == code)
        return await query_values(MdlStockChanges, None, *cond)


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
            cls.bkmap.bkstocks = AllStocks.get_bkstocks(code)
            await cls.bkmap.saveFetched()
        else:
            await cls.bkmap.getNext()

    @classmethod
    async def ignore_bk(cls, code, ignore=1):
        await upsert_one(cls.db, {'code': code, 'chgignore': ignore}, ['code'])

    @classmethod
    async def update_bk_changed(cls):
        bk_chgs = await cls.bkchanges.getLatestChanges()
        cls_chgs = await cls.clsbkchanges.getLatestChanges()
        return bk_chgs + cls_chgs

    @classmethod
    async def update_bk_changed_in5days(cls):
        await cls.bkchanges.updateBkChangedIn5Days()
        await cls.clsbkchanges.updateBkChangedIn5Days()

    @classmethod
    async def get_topbks(cls):
        embkkicks = await cls.bkchanges.topbks_to_date()
        clsbkkicks = await cls.clsbkchanges.topbks_to_date()
        return {**embkkicks, **clsbkkicks}

    @classmethod
    async def bk_stocks(self, bks, union=True):
        if isinstance(bks, str):
            bks = [bks]
        stocks = await query_values(MdlStockBkMap, MdlStockBkMap.stock, MdlStockBkMap.bk.in_(bks) if union else MdlStockBkMap.bk.all_(bks))
        return [s for s, in stocks]

    @classmethod
    async def stock_bks(self, codes, union=True):
        if isinstance(codes, str):
            codes = [codes]
        bks = await query_values(MdlStockBkMap, MdlStockBkMap.bk, MdlStockBkMap.stock.in_(codes) if union else MdlStockBkMap.stock.all_(codes))
        return [s for s, in bks]

    @classmethod
    async def get_bk_name(cls, bkcode):
        return await query_one_value(MdlStockBk, 'name', MdlStockBk.code == bkcode)


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
            stocks = await AllBlocks.bk_stocks(bk)
            self.bkstocklist[bk] = [to_cls_secucode(c) for c in stocks]

        kickdate = TradingDate.prev_trading_date(TradingDate.max_traded_date(), 3) if len(self.topbks.values()) == 0 else min([bk['kickdate'] for bk in self.topbks.values()])
        szt = sfac.get('StockZtDaily')
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
            zdfranks.extend(AllStocks.get_stocks_zdfrank(8))
            zdfranks.extend(AllStocks.get_stocks_zdfrank(-8))
            up_down_stocks = []
            for rkobj in zdfranks:
                c = rkobj['close']   # 最新价
                zd = rkobj['change']  # 涨跌幅
                code = rkobj['code'] # 代码
                if zd >= 0.08 or zd <= -0.08:
                    up_down_stocks.append(code)

            if not up_down_stocks:
                logger.info('no up or down stocks')
                return

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
            logger.debug(format_exc())

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
