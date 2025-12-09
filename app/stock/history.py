import sys
import re
import json
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from traceback import format_exc
from numpy.lib.recfunctions import append_fields
from stockrt.sources.rtbase import rtbase
from app.lofig import logger
from app.hu import classproperty, time_stamp
from app.hu.network import Network, EmRequest, EmDataCenterRequest
from app.db import query_one_value, query_values, query_aggregate, upsert_one, upsert_many, insert_many, delete_records
from . import dynamic_cache
from .h5 import (KLineStorage as kls, FflowStorage as ffs)
from .date import TradingDate
from .models import (
    MdlStockShare, MdlAllStock, MdlStockBk, MdlStockBkMap, MdlStockChanges, MdlStockBkChanges, MdlStockBkClsChanges)


class Khistory:
    @classproperty
    def stock_bonus_handler(cls):
        return StockShareBonus()

    @classproperty
    def fund_bonus_handler(cls):
        return FundShareBonus()

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
        days = TradingDate.calc_trading_days(last_date, TradingDate.max_trading_date())
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
        return kls.max_date(rtbase.get_fullcode(code), rtbase.to_int_kltype(kltype))

    @classmethod
    def count_bars_to_updated(cls, code, kltype=101):
        if rtbase.to_int_kltype(kltype) not in kls.saved_kline_types:
            return 0
        guessed = cls.guess_bars_since(cls.max_date(code, kltype), kltype)
        return guessed if guessed == sys.maxsize else max(guessed - 1, 0)

    @classmethod
    async def read_kline(cls, code, kline_type, fqt=0, length=None, start=None):
        """从HDF5文件中读取K线数据"""
        klt = rtbase.to_int_kltype(kline_type)
        if start is not None:
            length = cls.guess_bars_since(start, klt)
        def_len = kls.default_kline_cache_size(klt)
        if length is None:
            length = def_len
        code = rtbase.get_fullcode(code)
        kldata = kls.read_kline_data(code, klt, max(length, def_len))
        if len(kldata) > length:
            kldata = kldata[-length:]

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
            while len(bndata) > 0 and bndata[-1][2] > result['time'][-1]:
                bndata.pop()

            for bi in range(-1, -len(bndata) - 1, -1):
                while fid >= 0:
                    if result['time'][fid] >= bndata[bi][2]:
                        result['open'][fid] = fix_single_pre(float(result['open'][fid]), gx)
                        result['high'][fid] = fix_single_pre(float(result['high'][fid]), gx)
                        result['low'][fid] = fix_single_pre(float(result['low'][fid]), gx)
                        result['close'][fid] = fix_single_pre(float(result['close'][fid]), gx)
                        fid -= 1
                        continue

                    if bndata[bi][4] is None or bndata[bi][4] == '0':
                        gx.append((0, float(bndata[bi][7]) / 10))
                    elif bndata[bi][7] is None or bndata[bi][7] == '0':
                        gx.append((float(bndata[bi][4]) / 10, 0))
                    else:
                        gx.append((float(bndata[bi][4]) / 10, float(bndata[bi][7]) / 10))
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
            while len(bndata) > 0 and bndata[-1][2] > l0data[-1][1]:
                bndata.pop()
            for bi in range(-1, -len(bndata) - 1, -1):
                while fid >= 0:
                    if (l0data[fid][1] >= bndata[bi][2]):
                        fdid = list(l0data[fid])
                        fdid[2] = fix_single_pre(float(fdid[2]), gx)
                        fdid[3] = fix_single_pre(float(fdid[3]), gx)
                        fdid[4] = fix_single_pre(float(fdid[4]), gx)
                        fdid[5] = fix_single_pre(float(fdid[5]), gx)
                        l0data[fid] = tuple(fdid)
                        fid -= 1
                        continue
                    if bndata[bi][4] is None or bndata[bi][4] == '0':
                        gx += (0, float(bndata[bi][7]) / 10),
                    elif bndata[bi][7] is None or bndata[bi][7] == '0':
                        gx += (float(bndata[bi][4]) / 10, 0),
                    else:
                        gx += (float(bndata[bi][4]) / 10, float(bndata[bi][7]) / 10),
                    break
            while fid >= 0:
                fdid = list(l0data[fid])
                fdid[2] = fix_single_pre(float(fdid[2]), gx)
                fdid[3] = fix_single_pre(float(fdid[3]), gx)
                fdid[4] = fix_single_pre(float(fdid[4]), gx)
                fdid[5] = fix_single_pre(float(fdid[5]), gx)
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
                    if result['time'][fid] < bndata[bi][2]:
                        result['open'][fid] = fix_single_post(float(result['open'][fid]), gx)
                        result['high'][fid] = fix_single_post(float(result['high'][fid]), gx)
                        result['low'][fid] = fix_single_post(float(result['low'][fid]), gx)
                        result['close'][fid] = fix_single_post(float(result['close'][fid]), gx)
                        fid += 1
                        continue

                    if bndata[bi][4] is None or bndata[bi][4] == '0':
                        gx.append((0, float(bndata[bi][7]) / 10))
                    elif bndata[bi][7] is None or bndata[bi][7] == '0':
                        gx.append((float(bndata[bi][4]) / 10, 0))
                    else:
                        gx.append((float(bndata[bi][4]) / 10, float(bndata[bi][7]) / 10))
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
                    if (l0data[fid][1] < bndata[bi][2]):
                        fdid = list(l0data[fid])
                        fdid[2] = fix_single_post(float(fdid[2]), gx)
                        fdid[3] = fix_single_post(float(fdid[3]), gx)
                        fdid[4] = fix_single_post(float(fdid[4]), gx)
                        fdid[5] = fix_single_post(float(fdid[5]), gx)
                        l0data[fid] = tuple(fdid)
                        fid += 1
                        continue
                    if bndata[bi][4] is None or bndata[bi][4] == '0':
                        gx += (0, float(bndata[bi][7]) / 10),
                    elif bndata[bi][7] is None or bndata[bi][7] == '0':
                        gx += (float(bndata[bi][4]) / 10, 0),
                    else:
                        gx += (float(bndata[bi][4]) / 10, float(bndata[bi][7]) / 10),
                    break
            while fid < len(l0data):
                fdid = list(l0data[fid])
                fdid[2] = fix_single_post(float(fdid[2]), gx)
                fdid[3] = fix_single_post(float(fdid[3]), gx)
                fdid[4] = fix_single_post(float(fdid[4]), gx)
                fdid[5] = fix_single_post(float(fdid[5]), gx)
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
        klt = rtbase.to_int_kltype(kline_type)
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
            insert_many(self.db, values)

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
        return await query_values(self.db, None, MdlStockShare.register_date > date)

    async def getBonusHis(self, code):
        return await query_values(self.db, None, MdlStockShare.code == code)


class FundShareBonus(StockShareBonus):
    ''' get bonus share data for fund
    ref: https://fundf10.eastmoney.com/fhsp_510050.html
    '''
    def __init__(self) -> None:
        super().__init__()

    def setCode(self, code):
        self.code = rtbase.get_fullcode(code)
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
        upsert_many(self.db, [dict(zip(cols, row)) for row in self.fecthed], ['code', 'report_date'])
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
        self.code = rtbase.get_fullcode(code)
        self.page = 1

    async def getNext(self, headers=None):
        headers = self.headers.copy()
        headers['Referer'] = f'https://data.eastmoney.com/zjlx/{self.code[2:]}.html'
        rsp = self.getRequest(headers)
        fflow = json.loads(rsp)
        if fflow is None or 'data' not in fflow or fflow['data'] is None or 'klines' not in fflow['data']:
            logger.warning(rsp.url)
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
                code = ''.join(reversed(stk['secu_code'].split('.'))).upper()
                self.bkstocks.append(code)
        return self.bkstocks

    async def saveFetched(self):
        exstocks = await query_values(MdlStockBkMap, MdlStockBkMap.stock, MdlStockBkMap.bk == self.bk)
        exstocks = [s for s, in exstocks]
        allstocks = await query_values(MdlAllStock, MdlAllStock.code)
        allstocks = [s for s, in allstocks]
        self.bkstocks = list(filter(lambda x: x in allstocks, self.bkstocks))
        if len(exstocks) == 0:
            await insert_many(MdlStockBkMap, [MdlStockBkMap(bk=self.bk, stock=s) for s in self.bkstocks])
            self.bkstocks = []
            return

        ex = list(set(exstocks) - set(self.bkstocks))
        new = list(set(self.bkstocks) - set(exstocks))
        if len(ex) > 0:
            await delete_records(MdlStockBkMap, MdlStockBkMap.bk == self.bk, MdlStockBkMap.stock.in_(ex))

        if len(new) > 0:
            await insert_many(MdlStockBkMap, [MdlStockBkMap(bk=self.bk, stock=s) for s in new])

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
                self.saveFetched()
            return

        if 'allstock' in chgs['data']:
            self.mergeFetched(chgs['data']['allstock'])

        if len(self.fecthed) == chgs['data']['tc']:
            self.saveFetched()
        else:
            self.page += 1
            await self.getNext()

    def mergeFetched(self, changes):
        if self.date is None:
            self.date = TradingDate.max_trading_date()

        for chg in changes:
            code = chg['c']
            code = rtbase.get_fullcode(code)
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

        await insert_many(MdlStockChanges, self.fecthed)

    async def updateDaily(self):
        date = await query_aggregate('max', MdlStockChanges, 'date')
        self.date = TradingDate.max_trading_date()
        if date.startswith(self.date):
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

    def changesToDict(self, changes):
        """通用的转换方法"""
        if not changes:
            return []
        keys = [col.name for col in self.model_class.__table__.columns]
        return [dict(zip(keys, x)) for x in changes]

    async def saveChanges(self, changes):
        """保存异动数据到数据库"""
        if len(changes) == 0:
            return
        await upsert_many(self.model_class, self.changesToDict(changes), ['code', 'time'])

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

        await upsert_many(self.model_class, self.changesToDict(bkchanges5), ['code', 'time'])


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
            await upsert_many(self.model_class, self.changesToDict(bkvalues), ['code', 'time'])
