import os
import json
import datetime
from bs4 import BeautifulSoup
from app.hu import lru_cache, classproperty
from app.hu.network import Network
from app.lofig import Config
from app.db import query_values, insert_many, array_to_dict_list
from .h5 import KLineStorage as kls
from .models import MdlHolidays


class TradingDate():
    @classproperty
    def holidayfile(cls):
        return os.path.join(Config.h5_history_dir(), 'holidays.json')

    @classproperty
    def holidays(cls):
        if os.path.isfile(cls.holidayfile):
            with open(cls.holidayfile, 'r') as f:
                return json.load(f)
        return []

    @staticmethod
    def today(sep='-'):
        return datetime.datetime.now().strftime(f'%Y{sep}%m{sep}%d')

    @classmethod
    @lru_cache(maxsize=1)
    def max_trading_date(cls):
        d = datetime.datetime.now().date()
        while cls.is_holiday(d.strftime('%Y-%m-%d')):
            d -= datetime.timedelta(days=1)
        return d.strftime('%Y-%m-%d')

    @classmethod
    @lru_cache(maxsize=1)
    def max_traded_date(cls):
        return kls.max_date('sh000001')

    @classmethod
    @lru_cache(maxsize=1)
    def min_traded_date(cls):
        return kls.min_date('sh000001')

    @classmethod
    def is_trading_date(cls, date):
        if not date or date < cls.min_traded_date():
            return False
        if date == cls.max_trading_date():
            return True
        return date not in cls.holidays and datetime.datetime.strptime(date, '%Y-%m-%d').weekday() < 5

    @classmethod
    def is_trading_time(cls):
        """根据当前时间判断是否为交易时段（示例：上交所/深交所）"""
        if TradingDate.is_holiday(cls.today()):
            return False

        now = datetime.datetime.now().time()

        morning = (datetime.time(9, 30), datetime.time(11, 30))
        afternoon = (datetime.time(13, 0), datetime.time(15, 0))

        return (morning[0] <= now <= morning[1]) or (afternoon[0] <= now <= afternoon[1])

    @classmethod
    def trading_ended(cls):
        if TradingDate.is_holiday(cls.today()):
            return False

        now = datetime.datetime.now().time()
        return now >= datetime.time(15, 0)

    @classmethod
    def trading_started(cls):
        if TradingDate.is_holiday(cls.today()):
            return False

        now = datetime.datetime.now().time()
        return now >= datetime.time(9, 30)

    @classmethod
    @lru_cache(maxsize=1)
    def get_today_system_date(cls):
        url = 'http://www.sse.com.cn/js/common/systemDate_global.js'
        sse = Network.session.get(url)
        if sse.status_code == 200:
            if 'var systemDate_global' in sse.text:
                sys_date = sse.text.partition('var systemDate_global')[2].strip(' =;')
                sys_date = sys_date.split()[0].strip(' =;"')
            if 'var whetherTradeDate_global' in sse.text:
                istrading_date = sse.text.partition('var whetherTradeDate_global')[2].strip(' =;')
                istrading_date = istrading_date.split()[0].strip(' =;')
            if 'var lastTradeDate_global' in sse.text:
                last_trade_date = sse.text.partition('var lastTradeDate_global')[2].strip(' =;')
                last_trade_date = last_trade_date.split()[0].strip(' =;"')

            return sys_date, last_trade_date, json.loads(istrading_date.lower())
        return None, None, None

    @classmethod
    def is_holiday(cls, date=None):
        if not date:
            daynow = datetime.datetime.now()
            date = daynow.strftime('%Y-%m-%d')
            return date in cls.holidays or daynow.weekday() >= 5
        if date in cls.holidays or datetime.datetime.strptime(date, '%Y-%m-%d').weekday() >= 5:
            return True
        return False

    @classmethod
    @lru_cache(maxsize=10)
    def prev_trading_date(cls, date, ndays=1):
        """
        获取指定日期前第N个交易日
        :param date: 基准日期
        :param ndays: 向前偏移的天数（默认1）
        :return: 前第N个交易日日期，如果不存在返回第一天
        """
        if date <= cls.min_traded_date():
            return cls.min_traded_date()
        d = datetime.datetime.strptime(date, '%Y-%m-%d')
        while ndays > 0:
            d -= datetime.timedelta(days=1)
            if not cls.is_holiday(d.strftime('%Y-%m-%d')):
                ndays -= 1
        return d.strftime('%Y-%m-%d')

    @classmethod
    @lru_cache(maxsize=10)
    def next_trading_date(cls, date, ndays=1):
        """
        获取指定日期后第N个交易日
        :param date: 基准日期
        :param ndays: 向后偏移的天数（默认1）
        :return: 后第N个交易日日期，如果不存在返回最后一天
        """
        if date < cls.min_traded_date():
            return cls.min_traded_date()
        d = datetime.datetime.strptime(date, '%Y-%m-%d')
        while ndays > 0:
            d += datetime.timedelta(days=1)
            if not cls.is_holiday(d.strftime('%Y-%m-%d')):
                ndays -= 1
        return min(d.strftime('%Y-%m-%d'), cls.max_trading_date())

    @classmethod
    def recent_trading_dates(cls, n):
        """获取最近N个交易日列表"""
        dates = [cls.max_trading_date()]
        d = datetime.datetime.strptime(cls.max_trading_date(), '%Y-%m-%d')
        while len(dates) < n:
            d -= datetime.timedelta(days=1)
            date_str = d.strftime('%Y-%m-%d')
            if not cls.is_holiday(date_str):
                dates.append(date_str)
        dates.reverse()
        return dates

    @classmethod
    def calc_trading_days(cls, bdate, edate):
        """
        计算两个日期(含)之间的交易日数
        :param bdate: 起始日期
        :param edate: 结束日期
        :return: 交易日数
        """
        wkdays = 0
        if ' ' in bdate:
            bdate = bdate.split(' ')[0]
        if ' ' in edate:
            edate = edate.split(' ')[0]
        d = datetime.datetime.strptime(bdate, '%Y-%m-%d')
        while d <= datetime.datetime.strptime(edate, '%Y-%m-%d'):
            if cls.is_trading_date(d.strftime('%Y-%m-%d')):
                wkdays += 1
            d += datetime.timedelta(days=1)
        return wkdays

    @classmethod
    def clear_cache(cls):
        """强制刷新缓存"""
        cls.max_traded_date.cache_clear()
        cls.max_trading_date.cache_clear()
        cls.min_traded_date.cache_clear()

    @classmethod
    async def update_holiday(cls):
        url = 'https://www.tdx.com.cn/url/holiday/'
        response = Network.session.get(url)
        response.raise_for_status()
        response.encoding = 'gbk'
        soup = BeautifulSoup(response.text, 'html.parser', from_encoding='gbk')
        txt_data = soup.select_one('textarea#data')
        txt = txt_data.get_text()
        holidays = txt.strip().splitlines()
        cn_holidays = []
        for hol in holidays:
            d, n, r, *_ = hol.split('|')
            d = d[:4] + '-' + d[4:6] + '-' + d[6:]
            if r == '中国':
                if d not in cls.holidays:
                    cn_holidays.append([d, n])
                    cls.holidays.append(d)
        if len(cn_holidays) > 0:
            await insert_many(MdlHolidays, array_to_dict_list(MdlHolidays, cn_holidays))
            with open(cls.holidayfile, 'w') as f:
                return json.dump(cls.holidays, f)
