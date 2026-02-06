import os
import time
import base64
from functools import lru_cache
from weakref import WeakKeyDictionary
from datetime import datetime


class classproperty:
    def __init__(self, initializer):
        self._func = initializer
        self._cache = WeakKeyDictionary()

    def __get__(self, instance, owner):
        if owner not in self._cache:
            self._cache[owner] = self._func(owner)
        return self._cache[owner]

import importlib.util
if importlib.util.find_spec("ddddocr") is None:
    def img_to_text(img):
        return None
else:
    import ddddocr
    @lru_cache(maxsize=1)
    def ocr() -> ddddocr.DdddOcr:
        return ddddocr.DdddOcr(show_ad=False)

    def img_to_text(img):
        if isinstance(img, str) and os.path.isfile(img):
            with open(img, 'rb') as f:
                img = f.read()
        return ocr().classification(img)

def time_stamp():
    return int(time.time()*1000)

def delay_seconds(daytime:str)->float:
    '''计算当前时间到daytime的时间间隔'''
    dnow = datetime.now()
    dtarr = daytime.split(':')
    hr = int(dtarr[0])
    minutes = 0 if len(dtarr) < 2 else int(dtarr[1])
    secs = 0 if len(dtarr) < 3 else int(dtarr[2])
    target_time = dnow.replace(hour=hr, minute=minutes, second=secs)
    return (target_time - dnow).total_seconds()

def to_cls_secucode(code):
    return code[2:] + '.BJ' if code.startswith('bj') else code.lower()

class FixedPointConverter:
    def __init__(self, precision=4):
        self.precision = precision
        self.scale = 10 ** precision

    def float_to_int(self, float_array, dtype='int32'):
        """将浮点数转换为整数"""
        return (float_array * self.scale).astype(dtype)

    def int_to_float(self, int_array):
        """将整数转换回浮点数"""
        return int_array.astype('float64') / self.scale
