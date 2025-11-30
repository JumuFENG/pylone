import ddddocr
import os
import base64
from functools import lru_cache
from weakref import WeakKeyDictionary

class classproperty:
    def __init__(self, initializer):
        self._func = initializer
        self._cache = WeakKeyDictionary()

    def __get__(self, instance, owner):
        if owner not in self._cache:
            self._cache[owner] = self._func(owner)
        return self._cache[owner]

@lru_cache(maxsize=1)
def ocr() -> ddddocr.DdddOcr:
    return ddddocr.DdddOcr(show_ad=False)

def img_to_text(img):
    if isinstance(img, str) and os.path.isfile(img):
        with open(img, 'rb') as f:
            img = f.read()
    return ocr().classification(img)

class FixedPointConverter:
    def __init__(self, precision=4):
        self.precision = precision
        self.scale = 10 ** precision

    def float_to_int(self, float_array):
        """将浮点数转换为整数"""
        return (float_array * self.scale).astype('int32')

    def int_to_float(self, int_array):
        """将整数转换回浮点数"""
        return int_array.astype('float64') / self.scale
