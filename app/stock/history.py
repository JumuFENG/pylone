import sys
import re
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from stockrt.sources.rtbase import rtbase
from app.lofig import logger
from app.hu import classproperty
from app.hu.network import EmRequest, EmDataCenterRequest
from app.db import query_one_value, query_values, query_aggregate, upsert_many, insert_many
from .h5 import KLineStorage as h5storage
from .date import TradingDate
from .models import MdlStockShare, MdlAllStock


class Khistory:
    @classproperty
    def stock_bonus_handler(cls):
        return StockShareBonus()

    @classproperty
    def fund_bonus_handler(cls):
        return FundShareBonus()

    @staticmethod
    def guess_bars_since(last_date, kltype='d'):
        if last_date is None:
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
        klt = rtbase.to_int_kltype(kltype)
        klines = h5storage.read_kline_data(rtbase.get_fullcode(code), klt, h5storage.default_kline_cache_size(klt))
        if klines is None or len(klines) == 0:
            return None
        return str(klines['time'][-1])

    @classmethod
    def count_bars_to_updated(cls, code, kltype=101):
        if rtbase.to_int_kltype(kltype) not in h5storage.saved_kline_types:
            return 0
        guessed = cls.guess_bars_since(cls.max_date(code, kltype), kltype)
        return guessed if guessed == sys.maxsize else max(guessed - 1, 0)

    @classmethod
    async def read_kline(cls, code, kline_type, fqt=0, length=None, start=None):
        """从HDF5文件中读取K线数据"""
        klt = rtbase.to_int_kltype(kline_type)
        if start is not None:
            length = cls.guess_bars_since(start, klt)
        def_len = h5storage.default_kline_cache_size(klt)
        if length is None:
            length = def_len
        code = rtbase.get_fullcode(code)
        kldata = h5storage.read_kline_data(code, klt, max(length, def_len))
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
        if klt not in h5storage.saved_kline_types:
            logger.error(f'kline_type {klt} not in saved_kline_types')
            return False
        close = kldata['close']
        if 'change_px' not in kldata.dtype.names:
            change_px = np.empty_like(close, dtype=np.float64)
            if len(close) > 1:
                change_px[1:] = close[1:] - close[:-1]
            kldata['change_px'] = change_px

        if 'change' not in kldata.dtype.names:
            change_px = kldata['change_px']
            change = np.empty_like(close, dtype=np.float64)
            if len(close) > 1:
                change[1:] = change_px[1:] / close[:-1]
            kldata['change'] = change

        h5storage.save_kline_data(code, klt, kldata)


class StockShareBonus(EmDataCenterRequest):
    '''get bonus share notice datacenter.eastmoney.com.
    ref: https://data.eastmoney.com/yjfp/
    '''
    @classproperty
    def db(cls):
        return MdlStockShare

    def __init__(self):
        super().__init__()
        super(EmRequest, self).__init__()
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
