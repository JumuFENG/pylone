from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from app import PostParams, pparam_doc
from .manager import AllStocks, AllBlocks, StockMarketStats
from .schemas import PmStock


router = APIRouter(
    prefix="/stock",
    tags=["stock"],
    responses={404: {"description": "Not found"}},
)

@router.get("")
async def stock_get(act: str = Query(..., embed=True)):
    if act == "bk_ignored":
        bks = await AllBlocks.read_ignored()
        return [b for b, in bks]
    if act == "rtbkchanges":
        return await AllBlocks.update_bk_changed()
    if act == "sm_stats":
        return await StockMarketStats.latest_stats()
    if act == "f4lost":
        return await AllStocks.get_purelost4up()
    return {"message": f"Hello {act}"}

@router.post("", openapi_extra=pparam_doc([("act", "string", "act", True)]))
async def stock_post(act: str = PostParams.create("act")):
    return {"message": f"Hello {act}"}

@router.get("/allstockinfo", response_model=list[PmStock])
async def get_all_stock_info():
    return await AllStocks.read_all()

