from fastapi import APIRouter, Query, HTTPException
from typing import Optional
import requests
import base64
from app import PostParams, pparam_doc
from app.hu import img_to_text
from .stock.history import Khistory as khis
from .stock.date import TradingDate
from .stock.manager import AllStocks

router = APIRouter(
    prefix="/api",
    tags=["api"],
    responses={404: {"description": "Not found"}},
)

@router.get("/")
async def root():
    return {"message": "Hello World"}

@router.get("/tradingdates")
async def tradingdates(len: int = Query(30, gt=0)):
    return TradingDate.trading_dates[-len:]

@router.get("/stockhist")
async def stock_hist(
    code: str = Query(..., min_length=6, max_length=8),
    kltype: str = Query(...),
    fqt: int = Query(0, ge=0),
    length: int = Query(None, ge=0),
    start: str = Query(None, min_length=8, max_length=10)
):
    try:
        data = await khis.read_kline(code, kltype, fqt, length, start)
        return data.tolist()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stockzthist")
async def stock_zthist(
    date: str = Query(...),
    concept: str = Query(...),
    daily: bool = Query(...),
):
    # TODO: get stock zt history data
    # if daily:
    #     return json.dumps(szi.dumpDailyZt())
    # if concept is None:
    #     zt = szi.dumpDataByDate(date)
    #     return json.dumps(zt)
    # else:
    #     szi = StockZtDaily()
    #     zt = szi.dumpZtDataByConcept(date, concept)
    #     return json.dumps(zt)
    return []

@router.get("/stockdthist")
async def stock_dthist(
    date: str = Query(...),
):
    # TODO: get stock dt history data
    # sdi = StockDtInfo()
    # dt = sdi.dumpDataByDate(date)
    # return json.dumps(dt)
    return []

@router.get("/allstockinfo")
async def all_stock_info():
    stksInfo = await AllStocks.read_all()
    if stksInfo:
        t_map = {'ABStock': 'AB', 'BJStock': 'AB', 'ETF': 'E', 'LOF': 'L'}
        return [{
            'c': s.code, 'n': s.name, 't': t_map.get(s.typekind, '')
        } for s in stksInfo if s.typekind in ['ABStock', 'BJStock', 'ETF', 'LOF'] ]
    return []

def get_http_request(
    url: str,
    host: str = None,
    referer: str = None
):
    if not url:
        raise HTTPException(status_code=400, detail="No url specified.")

    if ':' not in url:
        while len(url) % 4 != 0:
            url += '='
        url = base64.b64decode(url).decode()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:138.0) Gecko/20100101 Firefox/138.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }

    if host is not None:
        headers['Host'] = host
    if referer is not None:
        headers['Referer'] = referer

    response = requests.get(url, headers=headers)
    return response.content.decode('utf-8')

@router.get("/get")
async def http_request_get(
    url: str = Query(...),
    host: str = Query(None, min_length=1, max_length=255),
    referer: str = Query(None, min_length=1, max_length=255)
):
    return get_http_request(url, host, referer)

@router.post("/get", openapi_extra=pparam_doc([
    ("url", "string", "url", True),
    ("host", "string", "host", False),
    ("referer", "string", "referer", False)
]))
async def http_request_post(
    url: str = PostParams.create("url"),
    host: Optional[str] = PostParams.create("host", default=None),
    referer: Optional[str] = PostParams.create("referer", default=None)
):
    return get_http_request(url, host, referer)


def recognize_image(img):
    if img is None:
        raise HTTPException(status_code=400, detail="No img specified.")
    if img.startswith('http'):
        return img_to_text(requests.get(img).content)
    if ',' in img:
        img = img.split(',')[1]
    return img_to_text(img)

@router.get("/captcha")
async def get_captcha(
    img: str = Query(...)
):
    return recognize_image(img)

@router.post("/captcha", openapi_extra=pparam_doc([("img", "string", "base64 string", True)]))
async def post_captcha(img: str = PostParams.create("img")):
    return recognize_image(img)
