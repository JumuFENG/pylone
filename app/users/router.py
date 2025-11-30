from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import select
from app.db import async_session_maker
from .manager import (
    fastapi_users,
    cookie_auth_backend,
    bearer_auth_backend,
    current_active_user,
    current_superuser,
    get_user_manager,
    get_user_db,
    get_current_user_basic
)
from .schemas import UserRead, UserCreate, UserUpdate
from .models import User

router = APIRouter()

router.include_router(
    fastapi_users.get_auth_router(cookie_auth_backend),
    prefix="/auth/jwt",
    tags=["auth"],
)

router.include_router(
    fastapi_users.get_auth_router(bearer_auth_backend),
    prefix="/auth/bearer",
    tags=["auth"],
)

router.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    prefix="/auth",
    tags=["auth"],
)

@router.get("/users/me", response_model=UserRead, tags=["users"])
async def users_me(
    basic_user: User = Depends(get_current_user_basic),
    bearer_user: User = Depends(fastapi_users.current_user(optional=True))
):
    if bearer_user:
        return bearer_user
    elif basic_user:
        return basic_user
    else:
        raise HTTPException(status_code=404, detail="用户不存在")

@router.patch("/users/{user_id}", response_model=UserRead, tags=["users"])
async def update_user_protected(
    user_id: int,
    user_update: UserUpdate,
    current_user: User = Depends(current_active_user)
):
    """更新用户信息（保护 ID 为 1 的超级管理员）"""
    # 保护超级管理员
    if user_id == 1 and current_user.id != 1:
        raise HTTPException(
            status_code=403,
            detail="无法修改超级管理员账户"
        )

    if user_id != current_user.id and not current_user.is_superuser:
        raise HTTPException(
            status_code=403,
            detail="无权修改其他用户信息"
        )

    async for user_db in get_user_db():
        async for user_manager in get_user_manager():
            user = await user_db.get(user_id)
            if not user:
                raise HTTPException(status_code=404, detail="用户不存在")

            updated_user = await user_manager.update(user_update, user, safe=False, request=None)
            return updated_user


@router.delete("/users/{user_id}", status_code=204, tags=["users"])
async def delete_user_protected(
    user_id: int,
    current_user: User = Depends(current_superuser)
):
    """删除用户（仅管理员，保护 ID 为 1 的超级管理员）"""
    if user_id == 1:
        raise HTTPException(
            status_code=403,
            detail="无法删除超级管理员账户"
        )

    async for user_db in get_user_db():
        async for user_manager in get_user_manager():
            user = await user_db.get(user_id)
            if not user:
                raise HTTPException(status_code=404, detail="用户不存在")

            await user_manager.delete(user, request=None)
            return

@router.get("/users/subaccounts", response_model=List[UserRead], tags=["users"])
async def get_subaccounts(user=Depends(current_active_user)):
    """获取子账户列表"""
    async with async_session_maker() as session:
        result = await session.execute(select(User).where(User.parent_id == user.id))
        users = result.scalars().all()
    return users
