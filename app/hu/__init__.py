import ddddocr
import os
import base64
from functools import lru_cache

@lru_cache(maxsize=1)
def ocr() -> ddddocr.DdddOcr:
    return ddddocr.DdddOcr(show_ad=False)

def img_to_text(img):
    if isinstance(img, str) and os.path.isfile(img):
        with open(img, 'rb') as f:
            img = f.read()
    return ocr().classification(img)
