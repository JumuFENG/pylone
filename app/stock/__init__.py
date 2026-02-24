import datetime
import asyncio
from typing import Callable, Any, Union
from cachetools import TTLCache, LRUCache
from functools import wraps, lru_cache
from decimal import Decimal, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING
from .date import TradingDate


offline_cache = LRUCache(maxsize=3000)

@lru_cache(maxsize=10)
def get_short_cache(ttl=None):
    ttl = 1 if ttl is None else ttl
    return TTLCache(maxsize=10000, ttl=ttl)

@lru_cache(maxsize=1)
def get_async_lru_cache():
    return LRUCache(maxsize=10000)

def get_cache(ttl=None):
    short_cache = get_short_cache(ttl)
    if TradingDate.is_trading_time():
        if offline_cache.currsize > 0:
            offline_cache.clear()
        return short_cache
    else:
        if short_cache.currsize > 0:
            short_cache.clear()
        return offline_cache

def make_cache_key(func: Callable, args: tuple, kwargs: dict) -> str:
    return f"{func.__qualname__}:{args}:{tuple(sorted(kwargs.items()))}"

def dynamic_cache(ttl: Union[int, None] = None):
    return _make_async_cache_decorator(get_cache, has_ttl=True)(ttl)

def async_lru():
    return _make_async_cache_decorator(get_async_lru_cache, has_ttl=False)()


def _make_async_cache_decorator(cache_getter, has_ttl: bool = False):
    """Return a decorator factory for async functions using a provided cache getter.

    cache_getter: callable that returns a cache instance. If has_ttl is True,
                  the getter will be called with a single `ttl` argument.
    """
    def decorator_factory(ttl=None):
        def decorator(func):
            lock = asyncio.Lock()
            @wraps(func)
            async def wrapper(*args, **kwargs):
                cache = cache_getter(ttl) if has_ttl else cache_getter()
                key = make_cache_key(func, args, kwargs)
                if key in cache:
                    return cache[key]

                async with lock:
                    if key in cache:
                        return cache[key]

                    result = await func(*args, **kwargs)
                    cache[key] = result
                    return result
            return wrapper
        return decorator
    return decorator_factory

def zdf_from_code(code):
    zdf = 10
    if code.startswith('sz30') or code.startswith('sh68') or code.startswith('30') or code.startswith('68'):
        zdf = 20
    elif code.startswith('bj'):
        zdf = 30
    return zdf

def precious_decimal(precious):
    exp = '0.'
    for i in range(0, precious):
        exp += '0'
    return Decimal(exp)

def zt_priceby(lclose, zdf=10, precious=2):
    ''' 以昨日收盘价计算涨停价格
    '''
    if zdf == 30:
        return float(Decimal(str(lclose * 1.3)).quantize(Decimal('0.00'), ROUND_FLOOR))
    pdec = precious_decimal(precious)
    zprc = float(Decimal(str((int(round(lclose * 100, 0)) + lclose * zdf) / 100.0)).quantize(pdec, ROUND_HALF_UP))
    return zprc

def dt_priceby(lclose, zdf=10, precious=2):
    ''' 以昨日收盘价计算涨停价格
    '''
    if zdf == 30:
        return float(Decimal(str(lclose * 0.7)).quantize(Decimal('0.00'), ROUND_CEILING))
    pdec = precious_decimal(precious)
    dprc = float(Decimal(str((int(round(lclose * 100, 0)) - lclose * zdf) / 100.0)).quantize(pdec, ROUND_HALF_UP))
    return dprc
