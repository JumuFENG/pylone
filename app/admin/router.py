import json
from fastapi import APIRouter, Depends, HTTPException
from typing import List
from sqlalchemy import select
from fastapi import Body, Query
from app.users.manager import current_superuser
from app.db import async_session_maker
from app.users.models import User
from app.users.schemas import UserRead
from app.stock.manager import AllStocks, AllBlocks
from app.stock.schemas import PmStock
from app.hu.network import Network as net

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/users", response_model=List[UserRead])
async def admin_user_list(user=Depends(current_superuser)):
    """获取所有用户列表（仅管理员）"""
    async with async_session_maker() as session:
        result = await session.execute(select(User))
        users = result.scalars().all()
    
    return users

@router.get("/search", response_model=List[PmStock])
async def search_stocks(keyword: str, user=Depends(current_superuser)):
    url = 'https://searchadapter.eastmoney.com/api/suggest/get'
    params = {'type': '14', 'markettype': '', 'mktnum': '', 'jys':'', 'classify': '', 'securitytype':'', 'status': '', 'count': '10', 'input': keyword}
    response = net.fetch_url(url, params=params)
    searched = json.loads(response)['QuotationCodeTable']
    if searched['Status'] != 0 or searched['TotalCount'] < 1:
        return []
    def get_typekind(data) -> str:
        classify = data['Classify']
        if data['SecurityTypeName'] == '京A':
            return 'BJStock'
        if classify == 'AStock':
            if data['SecurityTypeName'] == '沪A' or data['SecurityTypeName'] == '深A':
                return 'ABStock'
        if classify == 'Index':
            return 'Index'
        elif classify == 'Fund':
            if data['TypeUS'] == '9':
                return 'ETF'
            elif data['TypeUS'] == '10':
                return 'LOF'
            return classify
        else:
            return classify
    def get_fullcode(data) -> str:
        mkt = data['MarketType']
        classify = data['Classify']
        if data['SecurityTypeName'] == '京A':
            return f"bj{data['Code']}"
        if classify == 'Index':
            if mkt == '1':
                return f"sh{data['Code']}"
            elif mkt == '2':
                return f"sz{data['Code']}"
        elif classify == 'Fund':
            if mkt == '1': # "TypeUS": "9", "JYS": "9",
                return f"sh{data['Code']}"
            elif mkt == '2': # "TypeUS": "10", "JYS": "10",
                return f"sz{data['Code']}"
        return f"{data['Code']}"

    results = [{
        'code': get_fullcode(d), 'name': d['Name'], 'typekind': get_typekind(d),
    } for d in searched['Data']]
    return results

@router.post("/addstock")
async def add_stock(stock: PmStock, user=Depends(current_superuser)):
    await AllStocks.load_info(stock)

@router.post("/removestock")
async def remove_stock(code:str = Body(..., embed=True), user=Depends(current_superuser)):
    await AllStocks.remove(code)

@router.post("/ignore_bk")
async def ignore_bk(code:str = Body(..., embed=True), ignore:int = Body(1, embed=True), user=Depends(current_superuser)):
    await AllBlocks.ignore_bk(code, ignore)
