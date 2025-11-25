from fastapi import APIRouter, Depends, HTTPException
from app.users.manager import current_superuser
from sqlalchemy import select
from app.db import async_session_maker
from app.users.models import User
from typing import List
from app.users.schemas import UserRead

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/users", response_model=List[UserRead])
async def admin_user_list(user=Depends(current_superuser)):
    """获取所有用户列表（仅管理员）"""
    async with async_session_maker() as session:
        result = await session.execute(select(User))
        users = result.scalars().all()
    
    return users
