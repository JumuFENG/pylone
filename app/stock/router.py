from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from app import PostParams, pparam_doc
from .manager import AllStocks
from .schemas import PmStock


router = APIRouter(
    prefix="/stock",
    tags=["stock"],
    responses={404: {"description": "Not found"}},
)

@router.get("")
async def stock_get(act: str = Query(...)):
    return {"message": f"Hello {act}"}

@router.post("", openapi_extra=pparam_doc([("act", "string", "act", True)]))
async def stock_post(act: str = PostParams.create("act")):
    return {"message": f"Hello {act}"}

@router.get("/allstockinfo", response_model=list[PmStock])
async def get_all_stock_info():
    return await AllStocks.read_all()
