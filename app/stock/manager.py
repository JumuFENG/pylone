import sys
import numpy as np
from typing import List, Tuple
from datetime import datetime
import stockrt as srt
from stockrt.sources.eastmoney import Em
from app.hu import classproperty
from app.lofig import logger
from app.db import upsert_one, upsert_many, query_one_value, query_aggregate, query_values, delete_records
from .models import MdlAllStock
from .schemas import PmStock
from .history import (Khistory as khis, FflowHistory as fhis)
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
