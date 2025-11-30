import sys
import numpy as np
from datetime import datetime
from stockrt.sources.rtbase import rtbase
from app.lofig import logger
from .h5 import KLineStorage as h5storage
from .date import TradingDate

class Khistory:
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
    def read_kline(cls, code, kline_type, fqt=0, length=None, start=None):
        """从HDF5文件中读取K线数据"""
        klt = rtbase.to_int_kltype(kline_type)
        if start is not None:
            length = cls.guess_bars_since(start, klt)
        def_len = h5storage.default_kline_cache_size(klt)
        if length is None:
            length = def_len
        kldata = h5storage.read_kline_data(rtbase.get_fullcode(code), klt, max(length, def_len))
        if len(kldata) > length:
            kldata = kldata[-length:]

        if fqt == 0:
            return kldata

        # TODO: fqt
        return kldata

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
