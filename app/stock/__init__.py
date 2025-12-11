import datetime
import asyncio
from typing import Callable, Any
from cachetools import TTLCache, LRUCache
from functools import wraps, lru_cache
from .date import TradingDate


offline_cache = LRUCache(maxsize=3000)

@lru_cache(maxsize=10)
def get_short_cache(ttl=None):
    ttl = 1 if ttl is None else ttl
    return TTLCache(maxsize=10000, ttl=ttl)

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
    return f"{func.__name__}:{args}:{tuple(sorted(kwargs.items()))}"

def dynamic_cache(ttl: int | None = None):
    def decorator(func):
        cache = get_cache(ttl)
        lock = asyncio.Lock()
        @wraps(func)
        async def wrapper(*args, **kwargs):
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
