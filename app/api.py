from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import RedirectResponse
from typing import Optional
from urllib.parse import urlencode
import requests
import base64
from app import PostParams, pparam_doc
from app.hu import img_to_text
from app.lofig import Config
from app.hu.network import Network
from app.hu.ollama import ollama
from app.admin.system_settings import SystemSettings
from .stock.history import Khistory as khis, StockDtInfo
from .stock.date import TradingDate
from .stock.manager import AllStocks, AllBlocks
from .stock.router import stock_kline
from .selectors import SelectorsFactory as sfac, StockZtDaily


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
    return TradingDate.recent_trading_dates(len)

@router.get("/stockhist")
async def stock_hist(
    code: str = Query(..., min_length=6),
    kltype: str = Query(...),
    fqt: int = Query(0, ge=0),
    length: int = Query(None, ge=0),
    start: str = Query(None, min_length=8, max_length=10)
):
    return await stock_kline(code, kltype, fqt, length, start)

@router.get("/stockzthist")
async def stock_zthist(
    date: str = Query(...),
    concept: str = Query(...),
):
    szi: StockZtDaily = sfac.get('StockZtDaily')
    if concept is None:
        return await szi.dump_main_stocks_zt0(date)
    return await szi.dump_by_concept(date, concept)

@router.get("/stockdthist")
async def stock_dthist(
    date: str = Query(...),
):
    sdi = StockDtInfo()
    return await sdi.dumpDataByDate(date)

@router.get("/allstockinfo")
async def all_stock_info():
    stksInfo = await AllStocks.read_all()
    if stksInfo:
        t_map = {'ABStock': 'AB', 'BJStock': 'AB', 'ETF': 'E', 'LOF': 'L'}
        return [{
            'c': s.code, 'n': s.name, 't': t_map.get(s.typekind, '')
        } for s in stksInfo if s.typekind in ['ABStock', 'BJStock', 'ETF', 'LOF'] ]
    return []

@router.get("/allbks")
async def all_bks():
    bks = await AllBlocks.read_all()
    return [{'code': c, 'name': n, 'chgignore': i} for c,n,i in bks]

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
        if ' ' in url:
            url = url.replace(' ', '+')
        url = base64.b64decode(url).decode()

    headers = Network.headers.copy()

    if host is not None:
        headers['Host'] = host
    if referer is not None:
        headers['Referer'] = referer

    response = requests.get(url, headers=headers)
    if response.headers.get('content-type') == 'application/json':
        return response.json()
    try:
        return response.json()
    except:
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
        img = requests.get(img).content
    elif ',' in img:
        img = img.split(',')[1]

    itext = img_to_text(img)

    if itext is None:
        ollama_instance = ollama(Config.client_config().get('ollama_api_key', None))
        if isinstance(img, bytes):
            img = base64.b64encode(img).decode('utf-8')
        itext = ollama_instance.img_to_text(img)

    if itext is None:
        raise HTTPException(status_code=500, detail="Image recognition failed.")
    return itext

@router.get("/captcha")
async def get_captcha(
    img: str = Query(...)
):
    return recognize_image(img)

@router.post("/captcha", openapi_extra=pparam_doc([("img", "string", "base64 string", True)]))
async def post_captcha(img: str = PostParams.create("img")):
    return recognize_image(img)
