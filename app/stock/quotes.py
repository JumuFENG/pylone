import sys
import re
import json
import numpy as np
from datetime import datetime
from traceback import format_exc
from typing import Union, List, Dict, Any, Optional
import stockrt as srt
from stockrt.sources.rtbase import rtbase
from app.lofig import logger
from app.hu import classproperty, time_stamp
from app.hu.network import Network
from . import dynamic_cache, get_cache
from .date import TradingDate


class Quotes:
    """股票行情数据获取类，提供实时行情和K线数据"""

    # K线类型对应的缓存时间配置
    CACHE_DURATION_MAP = {
        1: 10,    # 1分钟K线缓存10秒
        5: 10,    # 5分钟K线缓存10秒
        15: 30,   # 15分钟K线缓存30秒
        'quotes': 3,  # 实时行情缓存3秒
        'default': 30  # 其他类型默认缓存30秒
    }

    @classmethod
    def _normalize_codes(cls, codes: Union[str, List[str]]) -> List[str]:
        """标准化股票代码输入"""
        if isinstance(codes, str):
            return codes.split(',') if ',' in codes else [codes]
        return codes if isinstance(codes, list) else [str(codes)]

    @classmethod
    def _get_cached_data(cls, codes: List[str], cache_key_prefix: str,
                        cache_duration: int) -> tuple[Dict[str, Any], List[str]]:
        """获取缓存数据并返回结果和未缓存的代码列表"""
        cache = get_cache(cache_duration)
        result = {}
        uncached_codes = []

        for code in codes:
            cache_key = f"{cache_key_prefix}_{code}"
            if cache_key in cache:
                result[code] = cache[cache_key]
            else:
                uncached_codes.append(code)

        return result, uncached_codes

    @classmethod
    def _cache_and_merge_data(cls, result: Dict[str, Any], new_data: Dict[str, Any],
                             cache_key_prefix: str, cache_duration: int) -> Dict[str, Any]:
        """缓存新数据并合并到结果中"""
        if not new_data:
            return result

        cache = get_cache(cache_duration)
        for code, data in new_data.items():
            cache_key = f"{cache_key_prefix}_{code}"
            cache[cache_key] = data
            result[code] = data

        return result

    @classmethod
    def get_quotes(cls, codes: Union[str, List[str]]) -> Dict[str, Any]:
        """
        获取实时行情数据

        Args:
            codes: 股票代码，可以是单个代码字符串或代码列表

        Returns:
            Dict[str, Any]: 股票代码到行情数据的映射
        """
        try:
            normalized_codes = cls._normalize_codes(codes)
            cache_duration = cls.CACHE_DURATION_MAP['quotes']

            result, uncached_codes = cls._get_cached_data(
                normalized_codes, 'quotes', cache_duration
            )

            if uncached_codes:
                new_quotes = srt.quotes(uncached_codes)
                result = cls._cache_and_merge_data(
                    result, new_quotes, 'quotes', cache_duration
                )

            return result

        except Exception as e:
            logger.error(f"获取行情数据失败: {e}")
            return {}

    @classmethod
    def get_klines(cls, codes: Union[str, List[str]], kline_type: str = 'd') -> Dict[str, Any]:
        """
        获取K线数据

        Args:
            codes: 股票代码，可以是单个代码字符串或代码列表
            kline_type: K线类型，如 '1', '5', '15', 'd', 'w', 'm'

        Returns:
            Dict[str, Any]: 股票代码到K线数据的映射
        """
        try:
            normalized_codes = cls._normalize_codes(codes)
            klt = rtbase.to_int_kltype(kline_type)

            # 特殊处理实时K线
            if klt == 101:
                return cls.day_kline_from_quotes(normalized_codes)

            return cls._get_cached_klines(normalized_codes, klt)

        except Exception as e:
            logger.error(f"获取K线数据失败: {e}")
            return {}

    @classmethod
    def day_kline_from_quotes(cls, codes: List[str]) -> Dict[str, Any]:
        """获取实时K线数据（基于实时行情构造）"""
        if not TradingDate.trading_started():
            return {}
        quotes_data = cls.get_quotes(codes)
        result = {}

        for code, quote in quotes_data.items():
            if quote:
                result[code] = [[
                    TradingDate.today(),
                    quote['open'],
                    quote['price'],
                    quote['high'],
                    quote['low'],
                    quote['volume'],
                    quote['amount'],
                    quote['change'],
                    quote['change_px'],
                    quote['amplitude'] if 'amplitude' in quote else 0,
                    quote['turnover'] if 'turnover' in quote else 0
                ]]

        return result

    @classmethod
    def _get_cached_klines(cls, codes: List[str], kline_type: int) -> Dict[str, Any]:
        """获取缓存的K线数据"""
        cache_duration = cls.CACHE_DURATION_MAP.get(kline_type, cls.CACHE_DURATION_MAP['default'])
        cache_key_prefix = f'klines_{kline_type}'

        result, uncached_codes = cls._get_cached_data(
            codes, cache_key_prefix, cache_duration
        )

        if uncached_codes:
            new_klines = srt.klines(uncached_codes, kline_type)
            today = TradingDate.today()
            for c, v in new_klines.items():
                new_klines[c] = [kl for kl in v if kl[0] >= today]
            result = cls._cache_and_merge_data(
                result, new_klines, cache_key_prefix, cache_duration
            )

        return result
