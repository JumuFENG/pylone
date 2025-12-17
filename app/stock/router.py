from fastapi import APIRouter, Query, HTTPException, Depends
from typing import Optional
from app import PostParams, pparam_doc
from app.users.manager import (
    User, fastapi_users, get_current_user_basic, verify_user,
    UserStockManager as usm)
from .manager import AllStocks, AllBlocks, StockMarketStats
from .history import rtbase, StockDtMap
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
    basic_user: Optional[User] = Depends(get_current_user_basic),
    bearer_user: Optional[User] = Depends(fastapi_users.current_user(optional=True)),
):
    if act == "bk_ignored":
        bks = await AllBlocks.read_ignored()
        return [b for b, in bks]
    if act == "rtbkchanges":
        return await AllBlocks.update_bk_changed()
    if act == "sm_stats":
        return await StockMarketStats.latest_stats()
    if act == "f4lost":
        return await AllStocks.get_purelost4up()
    if act == "dtmap":
        sdm = StockDtMap()
        dtmap = sdm.dumpDataByDate(date)
        return dtmap
    if act in ("watchings", ):
        user = await verify_user(basic_user or bearer_user)
        return await usm.watchings_with_strategy(user)
    return {"message": f"Hello {act}"}

@router.post("", openapi_extra=pparam_doc([
    ("act", "string", "act", True),
    ("acc", "string", "acc", False),
    ("accid", "integer", "10", False),
    ("data", "string", "{}", False)
]))
async def stock_post(
    act: str = PostParams.create("act"),
    acc: Optional[str] = PostParams.create("acc"),
    accid: Optional[int] = PostParams.create("accid"),
    code: Optional[str] = PostParams.create("code"),
    data: Optional[str] = PostParams.create("data"),
    basic_user: Optional[User] = Depends(get_current_user_basic),
    bearer_user: Optional[User] = Depends(fastapi_users.current_user(optional=True)),
):
    if act in ('deals', 'fixdeals', 'strategy', 'forget', 'costdog', 'rmwatch', 'strategy'):
        user = await verify_user(basic_user or bearer_user, acc, accid)
        if act == 'deals':
            await usm.add_deals(user, data)
        elif act == 'fixdeals':
            await usm.fix_deals(user, data)
        elif act == 'strategy':
            await usm.save_strategy(user, data)
        elif act == 'forget':
            await usm.forget_stock(user, data)
        elif act == 'costdog':
            usm.save_costdog(user, data)
        elif act == 'rmwatch':
            usm.remove_user_stock_with_deals(user, data)
        elif act == 'strategy':
            if len(code) != 8:
                code = rtbase.get_fullcode(code)
            if data is None or len(data) == 0:
                await usm.remove_strategy(user, code)
            else:
                await usm.save_strategy(user, code, data)
        return
    return {"message": f"Hello {act}"}

@router.get("/allstockinfo", response_model=list[PmStock])
async def get_all_stock_info():
    return await AllStocks.read_all()

