import sys
import re
import json
import asyncio
import hashlib
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from traceback import format_exc
from typing import Optional, List, Any
from numpy.lib.recfunctions import append_fields
import stockrt as srt
from app.lofig import logger
from app.hu import classproperty, time_stamp
from app.hu.network import Network, EmRequest, EmDataCenterRequest
from app.hu.aes_decrypt import AesCBCBase64
from app.db import (
    array_to_dict_list, query_one_value, query_values, query_aggregate,
    upsert_one, upsert_many, insert_many, delete_records)
from . import dynamic_cache, lru_cache
from .h5 import (KLineStorage as kls, FflowStorage as ffs)
from .date import TradingDate
from .models import (
    MdlAllStock, MdlStockList, MdlStockShare,MdlStockBk, MdlStockBkMap, MdlStockChanges, MdlStockBkChanges, MdlStockBkClsChanges,
    MdlDayZtStocks, MdlDayDtStocks, MdlDayZtConcepts)


class Khistory:
    @classproperty
    def stock_bonus_handler(cls):
        return StockShareBonus()

    @classproperty
    def fund_bonus_handler(cls):
        return FundShareBonus()

    @lru_cache(maxsize=1024)
    @staticmethod
    def guess_bars_since(last_date, kltype='d'):
        if last_date is None or last_date == '':
            return sys.maxsize

        if kltype == 'w' or kltype == 102:
            return (datetime.now() - datetime.strptime(last_date, "%Y-%m-%d")).days // 7
        months = datetime.now().year * 100 + datetime.now().month - int(last_date.replace('-', '')[:6]) + 1
        if kltype == 'm' or kltype == 103:
            return months
        if kltype == 'q' or kltype == 104:
            return months // 3
        if kltype == 'hy' or kltype == 105:
            return months // 6
        if kltype == 'y' or kltype == 106:
            return months // 12
        if ' ' in last_date:
            last_date = last_date.split(' ')[0]
        days = TradingDate.calc_trading_days(last_date, TradingDate.max_trading_date()) - 1
        if kltype == 'd' or kltype == 101:
            return days
        if isinstance(kltype, int) or kltype.isdigit():
            if int(kltype) == 1:
                return days * 240
            if int(kltype) % 5 == 0:
                return days * 240 / int(kltype)
        return 0

    @classmethod
    def max_date(cls, code, kltype='d'):
        return kls.max_date(srt.get_fullcode(code), srt.to_int_kltype(kltype))

    @classmethod
    def count_bars_to_updated(cls, code, kltype=101):
        if srt.to_int_kltype(kltype) not in kls.saved_kline_types:
            return 0
        guessed = cls.guess_bars_since(cls.max_date(code, kltype), kltype)
        if guessed == sys.maxsize:
            return guessed
        if not TradingDate.trading_ended():
            guessed -= 1
        return max(guessed, 0)

    @classmethod
    async def read_kline(cls, code, kline_type, fqt=0, length=None, start=None):
        """从HDF5文件中读取K线数据"""
        klt = srt.to_int_kltype(kline_type)
        if start is not None:
            length = cls.guess_bars_since(start, klt)
            if length == sys.maxsize:
                length = 0
            else:
                length += 1
        def_len = kls.default_kline_cache_size(klt)
        if length is None:
            length = def_len
        code = srt.get_fullcode(code)
        kldata = kls.read_kline_data(code, klt, max(length, def_len))
        if kldata is None:
            return None

        if len(kldata) > length:
            kldata = kldata[-length:]

        if start is not None:
            kldata = kldata[kldata['time'] >= start]

        if fqt == 0:
            return kldata

        return await cls.fix_price(code, kldata, fqt)

    @classmethod
    def fix_price_pre(cls, f0data, bndata):
        """
        前复权

        Args:
            f0data: numpy 结构化数组或元组列表
            bndata: 分红数据列表

        Returns:
            与输入相同类型的复权后数据
        """
        # 判断输入类型
        is_numpy = isinstance(f0data, np.ndarray)

        def fix_single_pre(p, gx):
            for i in range(-1, -len(gx) - 1, -1):
                if gx[i][0] == 0 and gx[i][1] == 0:
                    continue
                if gx[i][0] == 0:
                    p -= gx[i][1]
                    continue
                p = (p - gx[i][1]) / (1 + gx[i][0])
            return round(p, 3)

        if is_numpy:
            # 处理 numpy 数组
            result = f0data.copy()
            fid = len(result) - 1
            gx = [(0, 0)]

            # 移除超出范围的分红数据
            while len(bndata) > 0 and bndata[-1].ex_dividend_date > result['time'][-1]:
                bndata.pop()

            for bi in range(-1, -len(bndata) - 1, -1):
                while fid >= 0:
                    if result['time'][fid] >= bndata[bi].ex_dividend_date:
                        result['open'][fid] = fix_single_pre(float(result['open'][fid]), gx)
                        result['high'][fid] = fix_single_pre(float(result['high'][fid]), gx)
                        result['low'][fid] = fix_single_pre(float(result['low'][fid]), gx)
                        result['close'][fid] = fix_single_pre(float(result['close'][fid]), gx)
                        fid -= 1
                        continue

                    if bndata[bi].total_bonus is None or bndata[bi].total_bonus == '0':
                        gx.append((0, float(bndata[bi].cash_dividend) / 10))
                    elif bndata[bi].cash_dividend is None or bndata[bi].cash_dividend == '0':
                        gx.append((float(bndata[bi].total_bonus) / 10, 0))
                    else:
                        gx.append((float(bndata[bi].total_bonus) / 10, float(bndata[bi].cash_dividend) / 10))
                    break

            while fid >= 0:
                result['open'][fid] = fix_single_pre(float(result['open'][fid]), gx)
                result['high'][fid] = fix_single_pre(float(result['high'][fid]), gx)
                result['low'][fid] = fix_single_pre(float(result['low'][fid]), gx)
                result['close'][fid] = fix_single_pre(float(result['close'][fid]), gx)
                fid -= 1

            return result
        else:
            # 处理元组列表（保持原有逻辑）
            fid = len(f0data) - 1
            gx = (0, 0),
            l0data = list(f0data)
            while len(bndata) > 0 and bndata[-1].ex_dividend_date > l0data[-1][0]:
                bndata.pop()
            for bi in range(-1, -len(bndata) - 1, -1):
                while fid >= 0:
                    if (l0data[fid][0] >= bndata[bi].ex_dividend_date):
                        fdid = list(l0data[fid])
                        fdid[1] = fix_single_pre(float(fdid[1]), gx)
                        fdid[2] = fix_single_pre(float(fdid[2]), gx)
                        fdid[3] = fix_single_pre(float(fdid[3]), gx)
                        fdid[4] = fix_single_pre(float(fdid[4]), gx)
                        l0data[fid] = tuple(fdid)
                        fid -= 1
                        continue
                    if bndata[bi].total_bonus is None or bndata[bi].total_bonus == '0':
                        gx += (0, float(bndata[bi].cash_dividend) / 10),
                    elif bndata[bi].cash_dividend is None or bndata[bi].cash_dividend == '0':
                        gx += (float(bndata[bi].total_bonus) / 10, 0),
                    else:
                        gx += (float(bndata[bi].total_bonus) / 10, float(bndata[bi].cash_dividend) / 10),
                    break
            while fid >= 0:
                fdid = list(l0data[fid])
                fdid[1] = fix_single_pre(float(fdid[1]), gx)
                fdid[2] = fix_single_pre(float(fdid[2]), gx)
                fdid[3] = fix_single_pre(float(fdid[3]), gx)
                fdid[4] = fix_single_pre(float(fdid[4]), gx)
                l0data[fid] = tuple(fdid)
                fid -= 1
            return tuple(l0data)

    @classmethod
    def fix_price_post(cls, f0data, bndata):
        """
        后复权

        Args:
            f0data: numpy 结构化数组或元组列表
            bndata: 分红数据列表

        Returns:
            与输入相同类型的复权后数据
        """
        # 判断输入类型
        is_numpy = isinstance(f0data, np.ndarray)

        def fix_single_post(p, gx):
            for i in range(0, len(gx)):
                if gx[i][0] == 0 and gx[i][1] == 0:
                    continue
                if gx[i][0] == 0:
                    p += gx[i][1]
                    continue
                p = p * (1 + gx[i][0]) + gx[i][1]
            return round(p, 3)

        if is_numpy:
            # 处理 numpy 数组
            result = f0data.copy()
            gx = [(0, 0)]
            fid = 0

            for bi in range(0, len(bndata)):
                while fid < len(result):
                    if result['time'][fid] < bndata[bi].ex_dividend_date:
                        result['open'][fid] = fix_single_post(float(result['open'][fid]), gx)
                        result['high'][fid] = fix_single_post(float(result['high'][fid]), gx)
                        result['low'][fid] = fix_single_post(float(result['low'][fid]), gx)
                        result['close'][fid] = fix_single_post(float(result['close'][fid]), gx)
                        fid += 1
                        continue

                    if bndata[bi].total_bonus is None or bndata[bi].total_bonus == 0:
                        gx.append((0, float(bndata[bi].cash_dividend) / 10))
                    elif bndata[bi].cash_dividend is None or bndata[bi].cash_dividend == 0:
                        gx.append((float(bndata[bi].total_bonus) / 10, 0))
                    else:
                        gx.append((float(bndata[bi].total_bonus) / 10, float(bndata[bi].cash_dividend) / 10))
                    break

            while fid < len(result):
                result['open'][fid] = fix_single_post(float(result['open'][fid]), gx)
                result['high'][fid] = fix_single_post(float(result['high'][fid]), gx)
                result['low'][fid] = fix_single_post(float(result['low'][fid]), gx)
                result['close'][fid] = fix_single_post(float(result['close'][fid]), gx)
                fid += 1

            return result
        else:
            # 处理元组列表（保持原有逻辑）
            l0data = list(f0data)
            gx = (0, 0),
            fid = 0
            for bi in range(0, len(bndata)):
                while fid < len(l0data):
                    if (l0data[fid][0] < bndata[bi].ex_dividend_date):
                        fdid = list(l0data[fid])
                        fdid[1] = fix_single_post(float(fdid[1]), gx)
                        fdid[2] = fix_single_post(float(fdid[2]), gx)
                        fdid[3] = fix_single_post(float(fdid[3]), gx)
                        fdid[4] = fix_single_post(float(fdid[4]), gx)
                        l0data[fid] = tuple(fdid)
                        fid += 1
                        continue
                    if bndata[bi].total_bonus is None or bndata[bi].total_bonus == 0:
                        gx += (0, float(bndata[bi].cash_dividend) / 10),
                    elif bndata[bi].cash_dividend is None or bndata[bi].cash_dividend == 0:
                        gx += (float(bndata[bi].total_bonus) / 10, 0),
                    else:
                        gx += (float(bndata[bi].total_bonus) / 10, float(bndata[bi].cash_dividend) / 10),
                    break
            while fid < len(l0data):
                fdid = list(l0data[fid])
                fdid[1] = fix_single_post(float(fdid[1]), gx)
                fdid[2] = fix_single_post(float(fdid[2]), gx)
                fdid[3] = fix_single_post(float(fdid[3]), gx)
                fdid[4] = fix_single_post(float(fdid[4]), gx)
                l0data[fid] = tuple(fdid)
                fid += 1
            return l0data

    @classmethod
    async def fix_price(cls, code, f0data, fqt):
        kind = await query_one_value(MdlAllStock, 'typekind', MdlAllStock.code == code)
        if kind is None:
            return f0data
        bn: StockShareBonus = None
        if kind == 'ABStock' or kind == 'BJStock':
            bn = cls.stock_bonus_handler
        elif kind == 'LOF' or kind == 'ETF':
            bn = cls.fund_bonus_handler
        else:
            return f0data

        bndata = await bn.getBonusHis(code)
        if bndata is None or len(bndata) == 0:
            return f0data

        if fqt == 1:
            return cls.fix_price_pre(f0data, bndata)
        if fqt == 2:
            return cls.fix_price_post(f0data, bndata)
        return f0data

    @classmethod
    def save_kline(cls, code: str, kline_type: str|int, kldata: np.ndarray):
        """保存K线数据到HDF5文件"""
        if len(kldata) == 0:
            return False
        klt = srt.to_int_kltype(kline_type)
        if klt not in kls.saved_kline_types:
            logger.error(f'kline_type {klt} not in saved_kline_types')
            return False
        if klt == 101:
            close = kldata['close']
            if 'change_px' not in kldata.dtype.names:
                change_px = np.empty_like(close, dtype=np.float64)
                if len(close) > 1:
                    change_px[1:] = close[1:] - close[:-1]
                kldata = append_fields(kldata, 'change_px', change_px)

            if 'change' not in kldata.dtype.names:
                change_px = kldata['change_px']
                change = np.empty_like(close, dtype=np.float64)
                if len(close) > 1:
                    change[1:] = change_px[1:] / close[:-1]
                kldata = append_fields(kldata, 'change', change)

            if 'amplitude' not in kldata.dtype.names:
                high = kldata['high']
                low = kldata['low']
                amplitude = np.empty_like(close, dtype=np.float64)
                if len(close) > 1:
                    amplitude[1:] = (high[1:] - low[1:]) / close[:-1]
                kldata = append_fields(kldata, 'amplitude', amplitude)

        kls.save_dataset(code, kldata, klt)


class StockList():
    @classproperty
    def db(cls):
        return MdlStockList

    @classmethod
    async def get_stocks(cls, list_key):
        stocks = await query_values(cls.db, ['code'], cls.db.lkey == list_key)
        return [c for c, in stocks]

    @classmethod
    async def save_stocks(cls, list_key, stocks):
        async def quick_update():
            await delete_records(cls.db, cls.db.lkey == list_key)
            await insert_many(cls.db, [{'lkey': list_key, 'code': c} for c in stocks])

        ostocks = await query_values(cls.db, ['code'], cls.db.lkey == list_key)
        if len(ostocks) == 0:
            await insert_many(cls.db, [{'lkey': list_key, 'code': c} for c in stocks])
            return
        ostocks = {c for c, in ostocks}
        exists = list(ostocks - set(stocks))
        news = list(set(stocks) - ostocks)
        if len(exists) == 0:
            await insert_many(cls.db, [{'lkey': list_key, 'code': c} for c in news])
            return
        if len(news) == 0:
            await delete_records(cls.db, cls.db.lkey == list_key, cls.db.code.in_(exists))
            return

        if len(ostocks) < 200 or (len(ostocks) - len(exists)) / len(ostocks) > 0.3:
            await quick_update()
            return

        if len(exists) > 0:
            await delete_records(cls.db, cls.db.code.in_(exists), cls.db.lkey == list_key)
        if len(news) > 0:
            await insert_many(cls.db, [{'lkey': list_key, 'code': c} for c in news])


class StockShareBonus(EmDataCenterRequest):
    '''get bonus share notice datacenter.eastmoney.com.
    ref: https://data.eastmoney.com/yjfp/
    '''
    @classproperty
    def db(cls):
        return MdlStockShare

    def __init__(self):
        super().__init__()
        self.code = None
        self.pageSize = 100
        self.latestBn = None

    def getUrl(self):
        return  f'''https://datacenter.eastmoney.com/api/data/v1/get?reportName=RPT_SHAREBONUS_DET&columns=ALL&quoteColumns=&pageNumber={self.page}&pageSize={self.pageSize}&sortColumns=EQUITY_RECORD_DATE&sortTypes=-1&source=WEB&client=WEB&filter={self._filter}'''

    def setCode(self, code):
        self.code = code if len(code) == 6 else code[2:]
        self.page = 1

    async def getNext(self, headers=None):
        if self.page == 1:
            if self.code is None:
                # (REPORT_DATE='2021-12-31')(EX_DIVIDEND_DAYS>0)(EX_DIVIDEND_DATE='2021-12-07')
                # self.setFilter(f'''(EX_DIVIDEND_DATE='{date}')''')
                date = TradingDate.today()
                mxdate = await query_aggregate('max', self.db, 'report_date')
                date = min(date, mxdate)
                self.setFilter(f'''(EQUITY_RECORD_DATE>='{date}')(EX_DIVIDEND_DAYS>=-10)''')
            else:
                self.setFilter(f'''(SECURITY_CODE="{self.code}")''')

        return await super().getNext(self.headers if headers is None else headers)

    async def saveFecthed(self):
        values = []
        for bn in self.fecthed:
            rptdate = bn['REPORT_DATE'].split()[0]
            rcddate = bn['EQUITY_RECORD_DATE'].split()[0] if bn['EQUITY_RECORD_DATE'] is not None else ''
            dividdate = bn['EX_DIVIDEND_DATE'].split()[0] if bn['EX_DIVIDEND_DATE'] is not None else ''
            if dividdate == '':
                continue

            secode = bn['SECUCODE'].split('.')
            secode.reverse()
            code = ''.join(secode).lower()
            if self.code is None:
                if self.latestBn is None:
                    self.latestBn = await query_values(self.db, [MdlStockShare.code, MdlStockShare.ex_dividend_date], MdlStockShare.report_date >= TradingDate.today())
                if (code, dividdate) in self.latestBn:
                    continue
            values.append({
                'code': code,
                'report_date': rptdate,
                'register_date': rcddate,
                'ex_dividend_date': dividdate,
                'progress': bn['ASSIGN_PROGRESS'],
                'total_bonus': bn['BONUS_IT_RATIO'],
                'bonus_share': bn['BONUS_RATIO'],
                'transfer_share': bn['IT_RATIO'],
                'cash_dividend': bn['PRETAX_BONUS_RMB'],
                'dividend_yield': bn['DIVIDENT_RATIO'],
                'eps': bn['BASIC_EPS'],
                'bvps': bn['BVPS'],
                'total_shares': bn['TOTAL_SHARES'],
                'bonus_details': bn['IMPL_PLAN_PROFILE']
            })
        if len(values) > 0:
            await insert_many(self.db, values, ['code', 'report_date'])

    async def dividenDateLaterThan(self, code, date=None):
        if date is None:
            date = TradingDate.today()
        brows = await query_aggregate('count', self.db, 'code', MdlStockShare.ex_dividend_date > date, MdlStockShare.code == code)
        if brows is None:
            return False
        return brows > 0

    async def dividenDetailsLaterThan(self, date=None):
        if date is None:
            date = TradingDate.today()
        ddtl = await query_values(self.db, None, MdlStockShare.register_date > date)
        return [list(row) for row in ddtl]

    async def getBonusHis(self, code):
        return await query_values(self.db, None, MdlStockShare.code == code)


class FundShareBonus(StockShareBonus):
    ''' get bonus share data for fund
    ref: https://fundf10.eastmoney.com/fhsp_510050.html
    '''
    def __init__(self) -> None:
        super().__init__()

    def setCode(self, code):
        self.code = srt.get_fullcode(code)
        self.bnData = []

    def getUrl(self):
        return f'''https://fundf10.eastmoney.com/fhsp_{self.code[2:]}.html'''

    async def getNext(self, headers=None):
        fhsp = self.getRequest(headers)
        soup = BeautifulSoup(fhsp, 'html.parser')
        fhTable = soup.find('table', {'class':'w782 comm cfxq'})
        self.fecthed = []
        if fhTable is not None:
            rows = fhTable.find_all('tr')
            for r in rows:
                tr = r.find_all('td')
                if len(tr) < 4:
                    continue
                rcddate = tr[1].get_text()
                rptdate = rcddate
                dividdate = tr[2].get_text()
                detail = tr[3].get_text()
                fh = re.findall(r'-?\d+\.?\d*', detail)[0]
                fh = 10 * float(fh)
                self.fecthed.append([self.code, rptdate, rcddate, dividdate, '', 0, 0, 0, fh, 0, 0, 0, 0, detail])

        await self.saveFetched()

    async def saveFetched(self):
        if len(self.fecthed) == 0:
            return

        cols = [c.name for c in self.db.__table__.columns]
        await upsert_many(self.db, [dict(zip(cols, row)) for row in self.fecthed], ['code', 'report_date'])
        self.fecthed = []

    async def getBonusHis(self, code):
        brows = await query_aggregate('count', self.db, 'code', MdlStockShare.code == code)
        if brows is None or brows == 0:
            self.setCode(code)
            await self.getNext()
        return await query_values(self.db, None, MdlStockShare.code == code)


class FflowRequest(EmRequest):
    def __init__(self):
        super().__init__()
        self.page = 1
        self.pageSize = 50
        self.fecthed = []
        self.headers['Host'] = 'push2his.eastmoney.com'
        self.headers['Referer'] = 'http://quote.eastmoney.com/'

    def getUrl(self):
        # 获取最新120天的资金流向
        emsec = f"{1 if self.code.startswith('sh') else 0}.{self.code[2:]}"
        return f'''https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?lmt=0&klt=101&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65&ut=b2884a393a59ad64002292a3e90d46a5&secid={emsec}&_={time_stamp()}'''

    def setCode(self, code):
        self.code = srt.get_fullcode(code)
        self.page = 1

    async def getNext(self, headers=None):
        headers = self.headers.copy()
        headers['Referer'] = f'https://data.eastmoney.com/zjlx/{self.code[2:]}.html'
        rsp = self.getRequest(headers)
        fflow = json.loads(rsp)
        if fflow is None or 'data' not in fflow or fflow['data'] is None or 'klines' not in fflow['data']:
            logger.warning(fflow)
            return

        fflow = [f.split(',') for f in fflow['data']['klines']]
        if fflow is None or len(fflow) == 0:
            return

        maxdate = ffs.max_date(self.code)
        if len(fflow) == 1 and TradingDate.prev_trading_date(fflow[0][0]) != maxdate:
            logger.info(f'Stock_Fflow_History got only 1 data {self.code}, and not continously, discarded!')
            return

        fflow = [f for f in fflow if f[0] > maxdate]
        if not fflow or len(fflow) == 0:
            return
        ffs.save_fflow(self.code, fflow)


class FflowHistory:
    @classproperty
    def fclient(cls) -> FflowRequest:
        return FflowRequest()

    @classmethod
    def max_date(self, code):
        return ffs.max_date(code)

    @classmethod
    def save_fflow(self, code, fflow):
        ffs.save_fflow(code, fflow)

    @classmethod
    async def update_fflow(cls, code):
        if cls.max_date(code) == TradingDate.max_trading_date():
            return
        cls.fclient.setCode(code)
        await cls.fclient.getNext()

    @classmethod
    def get_main_fflow(cls, code, date=None, date1=None):
        if date is None:
            date = ffs.max_date(code)
        return ffs.read_fflow(code, date, date1)


class StockBkMap(EmRequest):
    def __init__(self, bk='') -> None:
        super().__init__()
        self.bk = bk
        self.bkstocks = []
        self.headers['Host'] = 'x-quote.cls.cn'

    def getUrl(self):
        return f'https://x-quote.cls.cn/web_quote/plate/stocks?app=CailianpressWeb&os=web&rever=1&secu_code={self.bk}&sv=8.4.6&way=change'

    async def getNext(self):
        self.fetchBkStocks()
        await self.saveFetched()

    def setCode(self, bk):
        self.bk = bk
        self.bkstocks = []

    def fetchBkStocks(self):
        self.headers['Referer'] = f'https://www.cls.cn/plate?code={self.bk}'
        bkstks = json.loads(self.getRequest(self.headers))
        if 'data' in bkstks and 'stocks' in bkstks['data']:
            for stk in bkstks['data']['stocks']:
                code = ''.join(reversed(stk['secu_code'].split('.')))
                self.bkstocks.append(code)
        return self.bkstocks

    async def saveFetched(self):
        exstocks = await query_values(MdlStockBkMap, MdlStockBkMap.stock, MdlStockBkMap.bk == self.bk)
        exstocks = [s for s, in exstocks]
        allstocks = await query_values(MdlAllStock, MdlAllStock.code)
        allstocks = [s for s, in allstocks]
        self.bkstocks = list(filter(lambda x: x in allstocks, self.bkstocks))
        if len(exstocks) == 0:
            await insert_many(MdlStockBkMap, [{"bk":self.bk, "stock":s} for s in self.bkstocks])
            self.bkstocks = []
            return

        ex = list(set(exstocks) - set(self.bkstocks))
        new = list(set(self.bkstocks) - set(exstocks))
        if len(ex) > 0:
            await delete_records(MdlStockBkMap, MdlStockBkMap.bk == self.bk, MdlStockBkMap.stock.in_(ex))

        if len(new) > 0:
            await insert_many(MdlStockBkMap, [{"bk":self.bk, "stock":s} for s in new])

        self.bkstocks = []


class StockChanges(EmRequest):
    '''盘口异动
    '''
    def __init__(self):
        super().__init__()
        self.page = 0
        self.pageSize = 1000
        self.fecthed = []
        self.date = None
        self.exist_changes = set()
        self.headers['Host'] = 'push2ex.eastmoney.com'
        self.headers['Referer'] = 'http://quote.eastmoney.com/changes/'

    def getUrl(self):
        t = '8201,8202,8193,4,32,64,8207,8209,8211,8213,8215,8204,8203,8194,8,16,128,8208,8210,8212,8214,8216,8217,8218,8219,8220,8221,8222'
        return f'http://push2ex.eastmoney.com/getAllStockChanges?type={t}&ut=7eea3edcaed734bea9cbfc24409ed989&pageindex={self.page}&pagesize={self.pageSize}&dpt=wzchanges'

    async def getNext(self):
        chgs = json.loads(self.getRequest(self.headers))
        if 'data' not in chgs or chgs['data'] is None:
            if len(self.fecthed) > 0:
                await self.saveFetched()
            return

        if 'allstock' in chgs['data']:
            self.mergeFetched(chgs['data']['allstock'])

        if len(self.fecthed) == chgs['data']['tc']:
            await self.saveFetched()
        else:
            self.page += 1
            await self.getNext()

    def mergeFetched(self, changes):
        if self.date is None:
            self.date = TradingDate.max_trading_date()

        for chg in changes:
            code = chg['c']
            code = srt.get_fullcode(code)
            tm = str(chg['tm']).rjust(6, '0')
            ftm = f'{self.date} {tm[0:2]}:{tm[2:4]}:{tm[4:6]}'
            tp = chg['t']
            info = chg['i']
            if (code, ftm, tp) not in self.exist_changes:
                self.fecthed.append([code, ftm, tp, info])
                self.exist_changes.add((code, ftm, tp))

    async def saveFetched(self):
        if len(self.fecthed) == 0:
            return

        await insert_many(MdlStockChanges, array_to_dict_list(MdlStockChanges, self.fecthed))

    async def updateDaily(self):
        date = await query_aggregate('max', MdlStockChanges, 'time')
        self.date = TradingDate.max_trading_date()
        if date and date.startswith(self.date):
            logger.info(f'{self.__class__.__name__} already updated to {self.date}')
            return

        await self.getNext()


class BkChanges(EmRequest):
    def __init__(self):
        super().__init__()
        self.fecthed = []
        self.exist_changes = set()
        self.allBks = []
        self.ignoredBks = []

    @property
    def model_class(self):
        pass

    async def load_bk_cache(self):
        """加载板块缓存"""
        if not self.allBks:
            bks = await query_values(MdlStockBk, [MdlStockBk.code, MdlStockBk.chgignore])
            self.allBks = [bk for bk, _ in bks]
            self.ignoredBks = [bk for bk, _ignored in bks if _ignored == 1]

    async def saveChanges(self, changes):
        """保存异动数据到数据库"""
        if len(changes) == 0:
            return
        await upsert_many(self.model_class, array_to_dict_list(self.model_class, changes), ['code', 'time'])

    async def dumpDataByDate(self, date=None):
        if date is None:
            date = await query_aggregate('max', self.model_class, 'time')
        if date is None:
            logger.info(f'{self.__class__.__name__} no data to updated!')
            return

        pool = await query_values(self.model_class, self.model_class.code, self.model_class.time == date)
        return [c for c, in pool]

    async def dumpTopBks(self, date, min_change=2, min_amount=0, min_ztcnt=5):
        """提取符合条件的板块（参数化阈值）"""
        ndate = TradingDate.next_trading_date(date)
        conds = [self.model_class.time >= f"{date} 14:50"]
        if ndate > date:
            conds.append(self.model_class.time < ndate)

        chgs = await query_values(self.model_class,
            ['code', 'time', 'change', 'amount', 'ztcnt', 'dtcnt'],
            *conds)

        bks = []
        for c, d, p, a, zcnt, dcnt in chgs:
            if c in self.ignoredBks:
                continue
            if p >= min_change and a > min_amount and zcnt >= min_ztcnt:
                bks.append(c)
        return bks

    async def topbks_to_date(self):
        mdate = TradingDate.max_traded_date()
        sdate = mdate
        ibks = []
        bkhist = {}
        for i in range(0, 10):
            sdate = TradingDate.prev_trading_date(sdate)
            ibks += await self.dumpTopBks(sdate)

        chgs = await query_values(self.model_class, ['code', 'time', 'change', 'amount', 'ztcnt', 'dtcnt'], self.model_class.time >= f"{sdate} 14:50")
        for c, d, ch, amt, zcnt, dcnt in chgs:
            dt, tm = d.split()
            if tm < '14:50':
                continue
            if c not in ibks:
                continue
            if c not in bkhist:
                bkhist[c] = {}
            bkhist[c][dt] = [c, dt, ch, amt, zcnt, dcnt]

        faded = []
        for c, hist in bkhist.items():
            if max(hist.keys()) < mdate:
                faded.append(c)
        for c in faded:
            bkhist.pop(c)

        bkhistarr = {}
        for c, hist in bkhist.items():
            if c not in bkhistarr:
                bkhistarr[c] = []
            for d, h in hist.items():
                bkhistarr[c].append(h)
            shist = sorted(bkhistarr[c], key=lambda x: x[1], reverse=True)
            bkhistarr[c] = [shist[0]]
            for i in range(1, len(shist)):
                if shist[i][1] != TradingDate.prev_trading_date(bkhistarr[c][0][1]):
                    break
                bkhistarr[c].insert(0, shist[i])
        bkhist = bkhistarr

        bkkickdetail = {}
        topbktetail = {}
        for c, hist in bkhist.items():
            mxch = hist[-1][2]
            kickid = len(hist) - 1
            for i in range(len(hist) - 1, -1, -1):
                if hist[i][2] > mxch:
                    mxch = hist[i][2]
                    kickid = i
            for i in range(kickid, -1, -1):
                if hist[i][2] < 0.3 * mxch:
                    break
                kickid = i
            tch = 0
            for i in range(kickid, len(hist)):
                tch += hist[i][2]
            tch = round(tch, 2)
            name = await query_one_value(MdlStockBk, 'name', MdlStockBk.code == c)
            kick_days = TradingDate.calc_trading_days(hist[kickid][1], min(hist[-1][1], TradingDate.max_trading_date()))
            if len(topbktetail.keys()) == 0 or tch > list(topbktetail.values())[0]['change_to_date']:
                topbktetail[c] = {'code':c, 'name':name, 'kickdate': hist[kickid][1], 'change_to_date': tch, 'kick_days': kick_days}
            if tch / kick_days < 0.8:
                continue
            bkkickdetail[c] = {'code':c, 'name':name, 'kickdate': hist[kickid][1], 'change_to_date': tch, 'kick_days': kick_days}
        return bkkickdetail if len(bkkickdetail.keys()) > 0 else topbktetail


class StockBkChanges(BkChanges):
    '''板块异动
    '''
    ydtypes = [4,8,16,32,64,128,8193,8194,8201,8202,8203,8204,8207,8208,8209,8210,8211,8212,8213,8214,8215,8216,8217,8218,8219,8220,8221,8222]
    ydpos_types = [4,32,64,8193,8201,8202,8207,8209,8211,8213,8215,8217,8219,8221]
    def __init__(self):
        super().__init__()
        self.page = 0
        self.pageSize = 1000
        self.headers['Host'] = 'push2ex.eastmoney.com'

    @property
    def model_class(self):
        return MdlStockBkChanges

    def getUrl(self):
        return f'http://push2ex.eastmoney.com/getAllBKChanges?ut=7eea3edcaed734bea9cbfc24409ed989&dpt=wzchanges&pageindex={self.page}&pagesize={self.pageSize}'

    async def getNext(self):
        chgs = json.loads(self.getRequest(self.headers))
        if 'data' not in chgs or chgs['data'] is None:
            return

        if 'allbk' in chgs['data']:
            await self.mergeFetched(chgs['data']['allbk'], str(chgs['data']['dt']))

        if len(self.fecthed) == chgs['data']['tc']:
            return

        self.page += 1
        await self.getNext()

    async def mergeFetched(self, changes, chgtime):
        await self.load_bk_cache()
        ftm = f'{chgtime[0:4]}-{chgtime[4:6]}-{chgtime[6:8]} {chgtime[8:10]}:{chgtime[10:12]}'
        for chg in changes:
            code = chg['c']
            name = chg['n']
            if code not in self.allBks:
                await upsert_one(MdlStockBk, {'code': code, 'name': name, 'chgignore': 0}, ['code'])
                self.allBks.append(code)
            pchange = float(chg['u'])
            amount = chg['zjl']
            ydct = chg['ct']
            if (code, ftm) not in self.exist_changes:
                ydrow = [code, ftm, pchange, amount, ydct]
                ydarr = [0] * len(self.ydtypes)
                ydpos = 0
                ztcnt = 0
                dtcnt = 0
                for yl in chg['ydl']:
                    ydarr[self.ydtypes.index(yl['t'])] = yl['ct']
                    if yl['t'] in self.ydpos_types:
                        ydpos += yl['ct']
                    if yl['t'] == 4:
                        ztcnt += yl['ct']
                    elif yl['t'] == 8:
                        dtcnt += yl['ct']
                    elif yl['t'] == 16:
                        ztcnt -= yl['ct']
                    elif yl['t'] == 32:
                        dtcnt -= yl['ct']
                ydrow += ydarr
                ydrow += [ydpos, 2*ydpos-ydct, ztcnt, dtcnt]
                self.fecthed.append(ydrow)
                self.exist_changes.add((code, ftm))

    @dynamic_cache(ttl=10)
    async def getLatestChanges(self):
        self.fecthed = []
        self.exist_changes = set()
        self.page = 0
        await self.getNext()
        if len(self.fecthed) == 0:
            return []
        self.fecthed = [yd for yd in self.fecthed if yd[0] not in self.ignoredBks]
        self.fecthed = sorted(self.fecthed, key=lambda x: x[3], reverse=True)
        # 净流入
        bkyd = self.fecthed[0:10]
        self.fecthed = sorted(self.fecthed, key=lambda x: x[2], reverse=True)
        # 涨跌幅
        bkyd += self.fecthed[0:10]
        self.fecthed = sorted(self.fecthed, key=lambda x: x[4], reverse=True)
        # 异动数量
        bkyd += self.fecthed[0:10]
        self.fecthed = sorted(self.fecthed, key=lambda x: x[-4], reverse=True)
        # 正异动
        bkyd += self.fecthed[0:10]
        self.fecthed = sorted(self.fecthed, key=lambda x: x[-3], reverse=True)
        # 绝对异动
        bkyd += self.fecthed[0:10]
        self.fecthed = sorted(self.fecthed, key=lambda x: x[-2], reverse=True)
        # 涨停数
        for i in range(0, 10):
            if self.fecthed[i][-2] > 0:
                bkyd.append(self.fecthed[i])
            else:
                break
        bkset = set()
        bkchanges = []
        for x in bkyd:
            if x[0] not in bkset:
                bkchanges.append(x)
                bkset.add(x[0])

        return bkchanges

    async def updateBkChangedIn5Days(self):
        mdate = await query_aggregate('max', self.model_class, 'time')
        mdate = mdate.split(' ')[0] if mdate else TradingDate.max_traded_date()
        ibks = await self.dumpTopBks(mdate)
        sdate = mdate
        for i in range(0, 5):
            sdate = TradingDate.prev_trading_date(sdate)
            ibks += await self.dumpTopBks(sdate)

        ibks = set(ibks)
        if len(self.fecthed) == 0:
            await self.getLatestChanges()

        bkchanges5 = []
        for x in self.fecthed:
            if x[0] in ibks:
                bkchanges5.append(x)

        await upsert_many(self.model_class, array_to_dict_list(self.model_class, bkchanges5), ['code', 'time'])


class StockClsBkChanges(BkChanges):
    def __init__(self):
        super().__init__()
        self.page = 1
        self.way = 'change'
        self.headers['Host'] = 'x-quote.cls.cn'
        self.headers['Referer'] = 'https://www.cls.cn/'

    @property
    def model_class(self):
        return MdlStockBkClsChanges

    def getUrl(self):
        return f'https://x-quote.cls.cn/web_quote/plate/plate_list?app=CailianpressWeb&os=web&page={self.page}&rever=1&sv=8.4.6&type=concept&way={self.way}'

    async def getNext(self):
        try:
            chgs = json.loads(self.getRequest(self.headers))
            if 'data' not in chgs or chgs['data'] is None:
                return

            if 'plate_data' in chgs['data']:
                await self.mergeFetched(chgs['data']['plate_data'])
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
        except Exception as e:
            logger.error(f"Error in getNext: {e}")
            logger.debug(format_exc())

    async def mergeFetched(self, changes, chgtime=None):
        await self.load_bk_cache()
        if chgtime is None:
            ftm = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
        else:
            ftm = f'{chgtime[0:4]}-{chgtime[4:6]}-{chgtime[6:8]} {chgtime[8:10]}:{chgtime[10:12]}'
        # ftm = '2024-07-26 15:00'
        for chg in changes:
            code = chg['secu_code']
            name = chg['secu_name']
            if code not in self.allBks:
                await upsert_one(MdlStockBk, {'code': code, 'name': name, 'chgignore': 0}, ['code'])
                self.allBks.append(code)
            pchange = round(chg['change'] * 100, 2) if chg['change'] else 0
            amount = chg['main_fund_diff']/10000
            ztcnt = chg['limit_up_num']
            dtcnt = chg['limit_down_num']
            self.fecthed.append([code, ftm, pchange, amount, ztcnt, dtcnt])
            self.exist_changes.add((code, ftm))

    @dynamic_cache(ttl=10)
    async def getLatestChanges(self):
        ways = ['change', 'main_fund_diff', 'limit_up_num']
        self.fecthed = []
        self.exist_changes = set()
        for w in ways:
            self.way = w
            await self.getNext()

        if len(self.fecthed) == 0:
            return []

        self.fecthed = [yd for yd in self.fecthed if yd[0] not in self.ignoredBks]
        self.fecthed = sorted(self.fecthed, key=lambda x: x[3], reverse=True)
        # 净流入
        bkyd = self.fecthed[0:10]
        self.fecthed = sorted(self.fecthed, key=lambda x: x[2], reverse=True)
        # 涨跌幅
        bkyd += self.fecthed[0:10]
        self.fecthed = sorted(self.fecthed, key=lambda x: x[4], reverse=True)
        # 涨停数
        i = 0
        ztnum = 0
        while i < len(self.fecthed):
            ztcnt = self.fecthed[i][4]
            if ztcnt == 0:
                break
            while i < len(self.fecthed) and self.fecthed[i][4] == ztcnt:
                bkyd.append(self.fecthed[i])
                ztnum += 1
                i += 1
            if ztnum >= 10:
                break

        bkset = set()
        self.fecthed = []
        for x in bkyd:
            if x[0] not in bkset:
                self.fecthed.append(x)
                bkset.add(x[0])

        return self.fecthed

    async def updateBkChangedIn5Days(self):
        mdate = await query_aggregate('max', self.model_class, 'time')
        mdate = mdate.split(' ')[0] if mdate else TradingDate.max_traded_date()
        ibks = await self.dumpTopBks(mdate)
        sdate = mdate
        for i in range(0, 5):
            sdate = TradingDate.prev_trading_date(sdate)
            ibks += await self.dumpTopBks(sdate)

        ibks = set(ibks)
        bkvalues = []
        ftm = mdate + ' 15:00'
        headers = self.headers.copy()
        headers['Host'] = 'x-quote.cls.cn'
        for bk in ibks:
            iurl = f'https://x-quote.cls.cn/web_quote/plate/info?app=CailianpressWeb&os=web&secu_code={bk}&sv=8.4.6'
            response = Network.fetch_url(iurl, headers=headers)
            plinfo = json.loads(response)
            if 'data' not in plinfo:
                continue
            plinfo = plinfo['data']
            code = plinfo['secu_code']
            pchange = round(plinfo['change'] * 100, 2)
            amount = plinfo['fundflow']/10000
            ztcnt = plinfo['limit_up_num']
            dtcnt = plinfo['limit_down_num']
            bkvalues.append([code, ftm, pchange, amount, ztcnt, dtcnt])

        if len(bkvalues) > 0:
            await upsert_many(self.model_class, array_to_dict_list(self.model_class, bkvalues), ['code', 'time'])


class StockZtInfo10jqka(EmRequest):
    '''涨停
    ref: http://data.10jqka.com.cn/datacenterph/limitup/limtupInfo.html
    '''
    def __init__(self) -> None:
        super().__init__()
        self.date = None
        self.pageSize = 15
        self.headers['Referer'] = 'http://data.10jqka.com.cn/datacenterph/limitup/limtupInfo.html'
        self.headers['Host'] = 'data.10jqka.com.cn'
        self.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'

    def getUrl(self):
        if self.date is None:
            self.date = TradingDate.today()
        url = f'http://data.10jqka.com.cn/dataapi/limit_up/limit_up_pool?page={self.page}&limit={self.pageSize}&field=199112,330329,9001,330325,9002,133971,133970,1968584&filter=HS,GEM2STAR,ST&order_field=199112&order_type=0&date={self.date.replace("-", "")}'
        return url

    def getNext(self, date=None):
        self.date = date
        self.page = 1
        ztdata = []
        while True:
            jqkback = json.loads(self.getRequest(self.headers))
            if jqkback is None or jqkback['status_code'] != 0 or jqkback['data'] is None:
                logger.info('StockZtInfo invalid response! %s', jqkback)
                return

            rdate = jqkback['data']['date']
            rdate = rdate[0:4] + '-' + rdate[4:6] + '-' + rdate[6:8]
            for ztobj in jqkback['data']['info']:
                mt = ztobj['market_type']
                code = ztobj['code']
                code = srt.get_fullcode(code) # code
                hsl = ztobj['turnover_rate'] / 100 # 换手率 %
                fund = ztobj['order_amount'] # 封单金额
                zbc = 0 if ztobj['open_num'] is None else ztobj['open_num'] # 炸板次数
                lbc = 1 if ztobj['high_days'] is None or ztobj['high_days'] == '首板' else int(re.findall(r'\d+', ztobj['high_days'])[-1])
                days = 1 if ztobj['high_days'] is None or ztobj['high_days'] == '首板' else int(re.findall(r'\d+', ztobj['high_days'])[0])
                zdf = ztobj['change_rate']
                cpt = ztobj['reason_type'] # 涨停原因

                rzdf = round(zdf)
                mkt = 0
                if rzdf == 30:
                    mkt = 2
                elif rzdf == 20:
                    mkt = 1
                elif rzdf == 5:
                    mkt = 3
                ztdata.append([code, rdate, fund, hsl, lbc, days, zbc, '', cpt, mkt])
            # fields:
            # 199112(change_rate涨跌幅),330329(high_days几天几板),9001(reason_type涨停原因),330325(limit_up_type涨停形态),
            # 9002(open_num开板次数),133971(order_volume封单量),133970(order_amount封单额),1968584(turnover_rate换手率),
            # 330323(first_limit_up_time首次涨停时间),330324(last_limit_up_time最后涨停时间),3475914(currency_value流通市值),
            # 10(latest最新价),9003(limit_up_suc_rate近一年涨停封板率),9004(time_preview)

            if jqkback['data']['page']['count'] == jqkback['data']['page']['page']:
                break
            self.page += 1

        return ztdata


class StockZtInfo(EmRequest):
    '''涨停
    ref: http://quote.eastmoney.com/ztb/detail#type=ztgc
    '''
    def __init__(self) -> None:
        super().__init__()
        self.urlroot = f'http://push2ex.eastmoney.com/getTopicZTPool?ut=7eea3edcaed734bea9cbfc24409ed989&sort=fbt%3Aasc&Pageindex=0&dpt=wz.ztzt&date='
        self.date = None

    @property
    def db(self):
        return MdlDayZtStocks

    def getUrl(self):
        if self.date is None:
            self.date = TradingDate.max_trading_date()
        return self.urlroot + self.date.replace('-', '')

    def getNext(self, date=None):
        self.date = date

        self.headers['Referer'] = f'http://quote.eastmoney.com/ztb/detail'
        self.headers['Host'] = 'push2ex.eastmoney.com'
        emback = json.loads(self.getRequest(self.headers))
        if emback is None or emback['data'] is None:
            logger.info('StockZtInfo invalid response! %s', emback)
            return []

        qdate = f"{emback['data']['qdate']}"
        if 'qdate' in emback['data'] and qdate != self.date.replace('-', ''):
            self.date = qdate[0:4] + '-' + qdate[4:6] + '-' + qdate[6:8]
            return []

        ztdata = []
        date = qdate[0:4] + '-' + qdate[4:6] + '-' + qdate[6:8]
        for ztobj in emback['data']['pool']:
            code = srt.get_fullcode(ztobj['c']) # code
            hsl = ztobj['hs']/100 # 换手率 %
            fund = ztobj['fund'] # 封单金额
            zbc = ztobj['zbc'] # 炸板次数
            lbc = ztobj['lbc']
            zdf = ztobj['zdp']
            mkt = 0
            if code.startswith('bj'):
                mkt = 2
            elif round(zdf) == 5:
                mkt = 3
            elif round(zdf) == 20:
                mkt = 1
            zdf = zdf / 100.0
            hybk = ztobj['hybk'] # 行业板块
            days = ztobj['zttj']['days']
            # other sections: c->code, n->name, m->market(0=SZ,1=SH), p->涨停价*1000, zdp->涨跌幅,
            # amount->成交额, ltsz->流通市值, tshare->总市值, lbc->连板次数, fbt->首次封板时间, lbt->最后封板时间
            # zttj->涨停统计 {days->天数, ct->涨停次数}
            ztdata.append([code, date, fund, hsl, lbc, days, zbc, hybk, '', mkt])

        return ztdata


class StockZtConcepts():
    @property
    def db(self):
        return MdlDayZtConcepts

    @property
    def ztdb(self):
        return MdlDayZtStocks

    async def getNext(self):
        date = await query_aggregate('max', self.db, 'time')
        if date is None:
            date = await query_aggregate('min', self.ztdb, 'time')
        if date is None:
            logger.info(f'{self.__class__.__name__} no data to updated!')
            return

        ztconceptsdata = []
        while date <= TradingDate.max_trading_date():
            pool = await query_values(self.ztdb, [MdlDayZtStocks.code, MdlDayZtStocks.bk, MdlDayZtStocks.cpt], self.ztdb.time == date)
            if not pool:
                if date == TradingDate.max_trading_date():
                    logger.info(f'{self.__class__.__name__} no data for {date}')
                    return
                date = TradingDate.next_trading_date(date)
                continue

            cdict = {}
            for c, bk, con in pool:
                if con is None:
                    con = ''
                if con == '' and bk == '':
                    raise Exception(f'no bk or con for {c} on {date}, please correct the data!')
                cons = []
                if con == '':
                    cons.append(bk)
                elif '+' in con:
                    cons = con.split('+')
                else:
                    cons.append(con)
                for k in cons:
                    if k not in cdict:
                        cdict[k] = 1
                    else:
                        cdict[k] += 1
            for k,v in cdict.items():
                if v > 1:
                    ztconceptsdata.append([date, k, v])

            ndate = TradingDate.next_trading_date(date)
            if ndate == date:
                break
            date = ndate

        unique_cols = []
        unique_data = []
        for d, c, v in ztconceptsdata:
            if (d, c.lower()) not in unique_cols:
                unique_cols.append((d, c.lower()))
                unique_data.append([d, c, v])
        if not unique_data:
            return
        await insert_many(self.db, array_to_dict_list(self.db, unique_data), ['time', 'cpt'])


class StockDtInfo(EmRequest):
    '''跌停
    ref: http://quote.eastmoney.com/ztb/detail#type=ztgc
    '''
    def __init__(self):
        super().__init__()
        self.date = None
        self.urlroot = f'http://push2ex.eastmoney.com/getTopicDTPool?ut=7eea3edcaed734bea9cbfc24409ed989&dpt=wz.ztzt&Pageindex=0&sort=fund%3Aasc&date='
        self.dtdata = []

    @property
    def db(self):
        return MdlDayDtStocks

    def getUrl(self):
        return f'{self.urlroot}{self.date.replace("-", "")}'

    async def getNext(self):
        if self.date is None:
            mxdate = await query_aggregate('max', self.db, 'time')
            if mxdate == TradingDate.max_trading_date():
                logger.info(f'{self.__class__.__name__} already updated to {mxdate}')
                return
            self.date = TradingDate.next_trading_date(mxdate) if mxdate is not None else TradingDate.max_trading_date()

        self.headers['Referer'] = f'http://quote.eastmoney.com/ztb/detail'
        self.headers['Host'] = 'push2ex.eastmoney.com'
        self.dtdata = []
        while True:
            emback = json.loads(self.getRequest(self.headers))
            if emback is None or emback['data'] is None:
                logger.info('StockDtInfo invalid response! %s', emback)
                if self.date < TradingDate.max_trading_date():
                    self.date = TradingDate.next_trading_date(self.date)
                    continue
                break

            qdate = f"{emback['data']['qdate']}"
            if 'qdate' in emback['data'] and qdate != self.date.replace('-', ''):
                self.date = qdate[0:4] + '-' + qdate[4:6] + '-' + qdate[6:8]
                continue

            date = qdate[0:4] + '-' + qdate[4:6] + '-' + qdate[6:8]
            for dtobj in emback['data']['pool']:
                code = srt.get_fullcode(dtobj['c']) # code
                hsl = dtobj['hs'] / 100 # 换手率 %
                fund = dtobj['fund'] # 封单金额
                fba = dtobj['fba'] # 板上成交额
                lbc = dtobj['days'] # 连板次数
                zbc = dtobj['oc'] # 开板次数
                hybk = dtobj['hybk'] # 行业板块
                mkt = 0
                if code.startswith('bj'):
                    mkt = 2
                elif round(abs(dtobj['zdp'])) == 20:
                    mkt = 1
                elif round(abs(dtobj['zdp'])) == 5:
                    mkt = 3
                self.dtdata.append([code, date, fund, fba, hsl, lbc, zbc, hybk, mkt])

            self.date = TradingDate.next_trading_date(self.date)
            if self.date == TradingDate.max_trading_date():
                break

        if len(self.dtdata) > 0:
            await insert_many(self.db, array_to_dict_list(self.db, self.dtdata), ['code', 'time'])

    async def dumpDataByDate(self, date = None):
        if date is None:
            date = await query_aggregate('max', self.db, 'time')

        if date is None:
            return None

        if date > TradingDate.max_traded_date():
            date = TradingDate.max_traded_date()

        if not TradingDate.is_trading_date(date):
            date = TradingDate.next_trading_date(date)

        pool = await query_values(self.db, ['code', 'lbc', 'bk'], self.db.time == date)
        if pool is not None and len(pool) > 0:
            return {'date': date, 'pool': pool}
        return {'date': date,'pool':[]}


class StockHotRank(EmRequest):
    ''' 获取人气榜
    http://guba.eastmoney.com/rank/
    '''
    def __init__(self) -> None:
        self.market = 0 # 1: hk 2: us
        super().__init__()
        self.decrypter = None
        self.page = 1
        self.headers.update({
            'Host': 'gbcdn.dfcfw.com',
            'Referer': 'http://guba.eastmoney.com/'
        })

    def getUrl(self):
        return f'''http://gbcdn.dfcfw.com/rank/popularityList.js?type={self.market}&sort=0&page={self.page}'''

    def getLatestRanks(self, page=1):
        self.page = page
        rsp = self.getRequest(self.headers)
        enrk = rsp.split("'")[1]

        if self.decrypter is None:
            k = hashlib.md5('getUtilsFromFile'.encode()).hexdigest()
            iv = 'getClassFromFile'
            self.decrypter = AesCBCBase64(k, iv)
        ranks = json.loads(self.decrypter.decrypt(enrk))
        if ranks is None or len(ranks) == 0:
            print(rsp)
            return []
        return ranks

    def getGbRanks(self, page=1):
        ''' max page = 5
        '''
        ranks = self.getLatestRanks(page)
        valranks = []
        for rk in ranks:
            valranks.append([rk['code'], rk['rankNumber'], float(rk['newFans'])])
        return valranks

    def getEmRanks(self, total=20):
        url = f'''https://data.eastmoney.com/dataapi/xuangu/list?st=POPULARITY_RANK&sr=1&ps={total}&p=1&sty=SECURITY_CODE,SECURITY_NAME_ABBR,NEW_PRICE,CHANGE_RATE,VOLUME_RATIO,HIGH_PRICE,LOW_PRICE,PRE_CLOSE_PRICE,VOLUME,DEAL_AMOUNT,TURNOVERRATE,POPULARITY_RANK,NEWFANS_RATIO&filter=(POPULARITY_RANK>0)(POPULARITY_RANK<={total})(NEWFANS_RATIO>=0.00)(NEWFANS_RATIO<=100.0)&source=SELECT_SECURITIES&client=WEB'''
        rsp = Network.fetch_url(url, Network.get_headers({'Host': 'data.eastmoney.com'}))
        jdata = json.loads(rsp)
        if jdata['code'] != 0 or 'result' not in jdata or 'data' not in jdata['result']:
            return []

        ranks = []
        for rk in jdata['result']['data']:
            ranks.append([rk['SECURITY_CODE'], rk['POPULARITY_RANK'], rk['NEWFANS_RATIO']])
        return ranks

    def get10jqkaRanks(self):
        # https://basic.10jqka.com.cn/basicph/popularityRanking.html
        url = 'https://basic.10jqka.com.cn/api/stockph/popularity/top/'
        rsp = Network.fetch_url(url, Network.get_headers({'Host': 'basic.10jqka.com.cn'}))
        jdata = json.loads(rsp)
        if jdata['status_code'] != 0 or 'data' not in jdata or 'list' not in jdata['data']:
            return []

        ranks = []
        for rk in jdata['data']['list']:
            ranks.append([rk['code'], rk['hot_rank']])
        return ranks

