from fastapi import APIRouter, Query, Form, Body, Depends, HTTPException, Request
import requests
import base64
from app.hu import img_to_text


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
    # TODO: get trading dates
    return [] * len

@router.get("/stockhist")
async def stock_hist(
    code: str = Query(..., min_length=6, max_length=6),
    kltype: str = Query(...),
    fqt: int = Query(..., gt=0),
    len: int = Query(..., gt=0),
    start: str = Query(..., min_length=8, max_length=8)
):
    # TODO: get stock history data
    return []

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
    # TODO: get all stock info
    # stkmkts = StockGlobal.getAllStocksShortInfo()
    # return json.dumps(stkmkts)
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

@router.post("/get")
async def http_request_post(
    url: str = Body(...),
    host: str = Body(None, min_length=1, max_length=255),
    referer: str = Body(None, min_length=1, max_length=255)
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

@router.post("/captcha")
async def post_captcha(request: Request):
    content_type = request.headers.get("content-type", "")

    if "application/json" in content_type:
        data = await request.json()
        img = data.get("img")
    elif "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
        form = await request.form()
        img = form.get("img")
    else:
        raise HTTPException(status_code=400, detail="Unsupported content type. Use application/json or form data.")

    if not img:
        raise HTTPException(status_code=400, detail="No img specified.")

    return recognize_image(img)
