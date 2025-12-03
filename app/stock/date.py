import bisect
import json
import requests
from datetime import datetime
from app.hu import classproperty, lru_cache
from .h5 import KLineStorage as kls


class TradingDate():
    @staticmethod
    def today(sep='-'):
        return datetime.now().strftime(f'%Y{sep}%m{sep}%d')

    @classproperty
    def trading_dates(self):
        return [ str(d) for d in kls.read_kline_data('sh000001')['time']]

    @classmethod
    @lru_cache(maxsize=1)
    def max_trading_date(self):
        mxdate = self.max_traded_date()
        if mxdate != self.today():
            sysdate, tradeday = self.get_today_system_date()
            if tradeday:
                if sysdate > self.trading_dates[-1]:
                    self.trading_dates.append(sysdate)
                return sysdate
        return mxdate

    @classmethod
    @lru_cache(maxsize=1)
    def max_traded_date(self):
        return kls.max_date('sh000001')

    @classmethod
    def is_trading_date(self, date):
        if date == self.max_trading_date():
            return True
        return date in self.trading_dates

    @classmethod
    @lru_cache(maxsize=1)
    def get_today_system_date(self):
        url = 'http://www.sse.com.cn/js/common/systemDate_global.js'
        sse = requests.get(url)
        if sse.status_code == 200:
            if 'var systemDate_global' in sse.text:
                sys_date = sse.text.partition('var systemDate_global')[2].strip(' =;')
                sys_date = sys_date.split()[0].strip(' =;"')
            if 'var whetherTradeDate_global' in sse.text:
                istrading_date = sse.text.partition('var whetherTradeDate_global')[2].strip(' =;')
                istrading_date = istrading_date.split()[0].strip(' =;')

            return sys_date, json.loads(istrading_date.lower())
        return None, None

    @classmethod
    def is_holiday(self, date):
        if self.is_trading_date(date):
            return False

        if date == self.today():
            sys_date, tradeday = self.get_today_system_date()
            return not tradeday

        return True

    @classmethod
    @lru_cache(maxsize=10)
    def prev_trading_date(self, date, ndays=1):
        """
        获取指定日期前第N个交易日
        :param date: 基准日期
        :param ndays: 向前偏移的天数（默认1）
        :return: 前第N个交易日日期，如果不存在返回第一天
        """
        dates = self.trading_dates
        idx = bisect.bisect_left(dates, date)
        return self.trading_dates[max(idx - ndays, 0)]

    @classmethod
    @lru_cache(maxsize=10)
    def next_trading_date(self, date, ndays=1):
        """
        获取指定日期后第N个交易日
        :param date: 基准日期
        :param ndays: 向后偏移的天数（默认1）
        :return: 后第N个交易日日期，如果不存在返回最后一天
        """
        idx = bisect.bisect_right(self.trading_dates, date)  # 找到第一个>date的索引
        return self.trading_dates[min(idx + ndays, len(self.trading_dates)) - 1]

    @classmethod
    def calc_trading_days(self, bdate, edate):
        """
        计算两个日期(含)之间的交易日数
        :param bdate: 起始日期
        :param edate: 结束日期
        :return: 交易日数
        """
        return bisect.bisect_right(self.trading_dates, edate) - bisect.bisect_left(self.trading_dates, bdate)

    @classmethod
    def clear_cache(cls):
        """强制刷新缓存"""
        cls.max_traded_date.cache_clear()
        cls.max_trading_date.cache_clear()

