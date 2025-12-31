import json
from fastapi import APIRouter, Query, HTTPException, Depends
from typing import Optional
from traceback import format_exc
from app import PostParams, pparam_doc
from app.lofig import logger
from app.users.manager import (
    User, fastapi_users, get_current_user_basic, verify_user,
    UserStockManager as usm)
from app.admin.system_settings import SystemSettings
from app.selectors import SelectorsFactory as sfac
from .date import TradingDate
from .manager import AllStocks, AllBlocks, StockMarketStats
from .history import Khistory as khis, FflowHistory as fhis, StockHotRank
from .quotes import Quotes as qot, srt
from .schemas import PmStock


router = APIRouter(
    prefix="/stock",
    tags=["stock"],
    responses={404: {"description": "Not found"}},
)

@router.get("")
async def stock_get(
    act: str = Query(..., embed=True),
    date: Optional[str] = Query(None, embed=True),
    acc: Optional[str] = Query(None, embed=True),
    accid: Optional[int] = Query(None, embed=True),
    key: Optional[str] = Query(None, embed=True),
    days: Optional[int] = Query(0, embed=True),
    stocks: Optional[str] = Query(None, embed=True),
    bks: Optional[str] = Query(None, embed=True),
    rank: Optional[int] = Query(40, embed=True),
    steps: Optional[int] = Query(0, embed=True),
    codes: Optional[str] = Query(None, embed=True),
    basic_user: Optional[User] = Depends(get_current_user_basic),
    bearer_user: Optional[User] = Depends(fastapi_users.current_user(optional=True)),
):
    if stocks:
        stocks = stocks.lower()
    if codes:
        codes = codes.lower()
    if act == "bk_ignored":
        bks = await AllBlocks.read_ignored()
        return [b for b, in bks]
    if act == "rtbkchanges":
        return await AllBlocks.update_bk_changed()
    if act == "sm_stats":
        return await StockMarketStats.latest_stats()
    if act == "f4lost":
        return await AllStocks.get_purelost4up()
    if act == "planeddividen":
        return await khis.stock_bonus_handler.dividenDetailsLaterThan(date)
    if act == "dtmap":
        sdm = sfac.get('StockDtMap')
        dtmap = await sdm.dumpDataByDate(date)
        return dtmap
    if act == 'stockbks':
        return await query_stock_bks(stocks)
    if act == 'bkstocks':
        return await query_bk_stocks(bks)
    if act == 'ztstocks':
        szd = sfac.get('StockZtDaily')
        return await szd.dumpZtStocksInDays(days)
    if act == 'hotstocks':
        szd = sfac.get('StockZtDaily')
        stks = await szd.get_hot_stocks(TradingDate.prev_trading_date(TradingDate.max_traded_date(), days))
        return stks
    if act == 'hotbks':
        date = TradingDate.max_traded_date()
        topbks5 = []
        for i in range(days):
            topbks5 += await AllBlocks.bkchanges.dumpTopBks(date)
            topbks5 += await AllBlocks.clsbkchanges.dumpTopBks(date)
            date = TradingDate.prev_trading_date(date)
        return list(set(topbks5))
    if act == 'hotrankrt':
        return await hotrankrt(rank)
    if act == 'zdtemot':
        sze = sfac.get('StockZdtEmotion')
        if date is not None:
            return await sze.dumpDataByDate(date)
        return await sze.dumpDataInDays(days)
    if act == 'ztstepshist':
        if days == 0:
            days = 3
        if steps == 0:
            steps = 4
        szd = sfac.get('StockZtDaily')
        return await szd.dumpDailySteps(steps, days)
    if act == 'zdtindays':
        codes = qot._normalize_codes(codes)
        codes = [srt.get_fullcode(x) for x in codes]
        if date is None:
            date = TradingDate.today()
        dzts = {}
        logger.error('not implemented yet for zdtindays')
        # saus = StockAuctionUpSelector()
        # for code in codes:
        #     dzts[code] = saus.calc_dzt_num(code, date)
        return dzts
    if act == 'getistr':
        if key == 'istrategy_zt1wb':
            wbtbl = sfac.get('StockZt1WbSelector')
            z1stks = await wbtbl.dumpDataByDate()
            td = TradingDate.max_traded_date()
            return [x[1] for x in z1stks if x[0] == td]
        elif key == 'istrategy_3brk':
            s3btbl = sfac.get('StockTrippleBullSelector')
            return await s3btbl.getDaysCandidatesHighLow(days, True)
        elif key == 'istrategy_hsrzt0':
            shs = sfac.get('StockHotStocksRetryZt0Selector')
            return await shs.dumpDataByDate()
        elif key == 'istrategy_hotrank0':
            # hotranktbl = StockHotrank0Selector()
            # rked5d = hotranktbl.getRanked(TradingDate.max_traded_date())
            # return rked5d
            return []
        raise HTTPException(404, detail=f"Unknown istrategy key: {key}")
    if act in ("watchings", ):
        user = await verify_user(basic_user or bearer_user, acc, accid)
        return await usm.watchings_with_strategy(user)
    return {"message": f"Hello {act}"}

@router.post("", openapi_extra=pparam_doc([
    ("act", "string", "act", True),
    ("acc", "string", "acc", False),
    ("accid", "integer", "10", False),
    ("code", "string", "sh600001", False),
    ("key", "string", "istr_key", False),
    ("data", "string", "{}", False),
    ("ohstks", "string", "[]", False),
]))
async def stock_post(
    act: str = PostParams.create("act"),
    acc: Optional[str] = PostParams.create("acc", default=None),
    accid: Optional[int] = PostParams.create("accid", default=None),
    code: Optional[str] = PostParams.create("code", default=None),
    key: Optional[str] = PostParams.create("key", default=None),
    data: Optional[str] = PostParams.create("data", default=None),
    ohstks: Optional[str] = PostParams.create("ohstks", default=None),
    date: Optional[str] = PostParams.create("date", default=None),
    auctions: Optional[str] = PostParams.create("auctions", default=None),
    matched: Optional[str] = PostParams.create("matched", default=None),
    basic_user: Optional[User] = Depends(get_current_user_basic),
    bearer_user: Optional[User] = Depends(fastapi_users.current_user(optional=True)),
):
    if code:
        code = code.lower()
    if act in ('deals', 'fixdeals', 'strategy', 'forget', 'costdog', 'rmwatch', 'strategy'):
        user = await verify_user(basic_user or bearer_user, acc, accid)
        if act == 'deals':
            await usm.add_deals(user, data)
        elif act == 'fixdeals':
            await usm.fix_deals(user, data)
        elif act == 'forget':
            await usm.forget_stock(user, data)
        elif act == 'costdog':
            usm.save_costdog(user, data)
        elif act == 'rmwatch':
            usm.remove_user_stock_with_deals(user, data)
        elif act == 'strategy':
            if len(code) != 8:
                code = srt.get_fullcode(code)
            if data is None or len(data) == 0:
                await usm.remove_strategy(user, code)
            else:
                await usm.save_strategy(user, code, data)
        return
    if act == 'save_auction_details':
        logger.warning('save_auction_details is deprecated')
        # sad = StockAuctionDetails()
        # sad.saveDailyAuctions(date, json.loads(auctions))
        return
    if act == 'save_auction_matched':
        logger.warning('save_auction_matched is deprecated')
        # aucs = StockAuctionUpSelector()
        # aucs.save_daily_auction_matched(json.loads(matched))
        return
    if act == 'setistr':
        if key == 'istrategy_hotrank0':
            rks = json.loads(data)
            mdt = TradingDate.max_trading_date()
            tprks = [[srt.get_fullcode(x[0]), mdt] + x[1:] for x in rks]
            logger.warning('setistr istrategy_hotrank0 is deprecated')
            # hotranktbl = StockHotrank0Selector()
            # hotranktbl.saveDailyHotrank0(tprks)
        if key == 'istrategy_hotstks_open':
            ohstks = json.loads(ohstks)
            ohtbl = sfac.get('StockHotStocksOpenSelector')
            await ohtbl.saveDailyHotStocksOpen(ohstks)
        return
    return {"message": f"Hello {act}"}

@router.get("/stockbks")
async def query_stock_bks(
    stocks: str = Query(..., min_length=6)
):
    stocks = qot._normalize_codes(stocks)
    bkignored = await AllBlocks.read_ignored()
    bks = {}
    for s in stocks:
        s_bks = await AllBlocks.stock_bks(srt.get_fullcode(s))
        cbks = []
        for k in s_bks:
            if k in bkignored:
                continue
            cbks.append([k, await AllBlocks.get_bk_name(k)])
        bks[s] = cbks
    return bks

@router.get("/bkstocks")
async def query_bk_stocks(
    bks: str = Query(..., min_length=3)
):
    bks = bks.split(',')
    stks = {}
    for c in bks:
        stks[c] = await AllBlocks.bk_stocks(c)
    return stks

@router.get("/allstockinfo", response_model=list[PmStock])
async def get_all_stock_info():
    return await AllStocks.read_all()

@router.get("/klines")
async def stock_kline(
    code: str = Query(..., min_length=6),
    kltype: str = Query(...),
    fqt: int = Query(0, ge=0),
    length: int = Query(None, ge=0),
    start: str = Query(None, min_length=8, max_length=10)
):
    try:
        code = qot._normalize_codes(code)

        result = {}
        codes_unfinished = []
        codes_unsaved = []
        for c in code:
            data = await khis.read_kline(c, kltype, 0, length, start)
            if data is None:
                codes_unsaved.append(c)
                continue
            result[c] = data.tolist()
            if result[c][-1][0] < TradingDate.max_trading_date():
                codes_unfinished.append(c)

        realtime_kline_enabled = await SystemSettings.get('realtime_kline_enabled', '0')
        if realtime_kline_enabled == '1' and len(codes_unfinished) > 0:
            qklines = qot.get_klines(codes_unfinished, kltype)
            for c, kl in qklines.items():
                if c not in result:
                    result[c] = kl
                    continue
                if result[c][-1][0] < kl[0][0]:
                    result[c].extend(kl)
                    if fqt > 0:
                        result[c] = await khis.fix_price(c, result[c], fqt)
        if len(codes_unsaved) > 0:
            result.update(srt.klines(codes_unsaved, kltype, length, fqt))
        return result
    except Exception as e:
        logger.error(e)
        logger.debug(format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quotes")
def stock_quotes(code: str = Query(..., min_length=6)):
    try:
        code = qot._normalize_codes(code)
        return qot.get_quotes(code)
    except Exception as e:
        logger.error(e)
        logger.debug(format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tlines")
def stock_tlines(code: str = Query(..., min_length=6)):
    try:
        code = qot._normalize_codes(code)
        return qot.get_tlines(code)
    except Exception as e:
        logger.error(e)
        logger.debug(format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stock_fflow")
async def stock_fflow(code: str = Query(..., min_length=6), date: str = Query(None, min_length=8, max_length=10)):
    try:
        code = srt.get_fullcode(code)
        return fhis.get_main_fflow(code, date)
    except Exception as e:
        logger.error(e)
        logger.debug(format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/hotrankrt")
async def hotrankrt(rank: int = Query(40, ge=10)):
    try:
        if rank > 100:
            rank = 100
        shr = StockHotRank()
        rks = shr.getGbRanks()
        i = 1
        while i * 20 < rank:
            i += 1
            rks += shr.getGbRanks(i)
        return rks
    except Exception as e:
        logger.error(e)
        logger.debug(format_exc())
        raise HTTPException(status_code=500, detail=str(e))
