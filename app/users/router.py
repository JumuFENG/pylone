from fastapi import APIRouter, HTTPException, Depends
from .manager import fastapi_users, auth_backend, current_superuser
from .schemas import UserRead, UserCreate, UserUpdate
from app.db import async_session_maker
from .models import User

router = APIRouter()

router.include_router(
    fastapi_users.get_auth_router(auth_backend),
    prefix="/auth/jwt",
    tags=["auth"],
)

router.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    prefix="/auth",
    tags=["auth"],
)

router.include_router(
    fastapi_users.get_users_router(UserRead, UserUpdate),
    prefix="/users",
    tags=["users"],
)


# 添加保护超级管理员的中间件检查
@router.patch("/users/{user_id}", response_model=UserRead, tags=["users"])
async def update_user_protected(
    user_id: int,
    user_update: UserUpdate,
    current_user: User = Depends(current_superuser)
):
    """更新用户信息（保护 ID 为 1 的超级管理员）"""
    if user_id == 1:
        raise HTTPException(
            status_code=403,
            detail="无法修改超级管理员账户"
        )
    
    # 使用 FastAPI Users 的默认更新逻辑
    from .manager import get_user_manager, get_user_db
    
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
    """删除用户（保护 ID 为 1 的超级管理员）"""
    if user_id == 1:
        raise HTTPException(
            status_code=403,
            detail="无法删除超级管理员账户"
        )
    
    # 使用 FastAPI Users 的默认删除逻辑
    from .manager import get_user_manager, get_user_db
    
    async for user_db in get_user_db():
        async for user_manager in get_user_manager():
            user = await user_db.get(user_id)
            if not user:
                raise HTTPException(status_code=404, detail="用户不存在")
            
            await user_manager.delete(user, request=None)
            return
