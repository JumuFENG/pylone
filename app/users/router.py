import jwt
import time
import json
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query, Request, Response
from sqlalchemy import select
from app import PostParams, pparam_doc
from app.db import async_session_maker
from .manager import (
    fastapi_users,
    cookie_auth_backend,
    bearer_auth_backend,
    verify_user,
    get_user_manager,
    get_current_user_basic,
    get_jwt_strategy,
    cfg,
    UserStockManager as usm
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
    basic_user: Optional[User] = Depends(get_current_user_basic),
    bearer_user: Optional[User] = Depends(fastapi_users.current_user(optional=True)),
    request: Request = None,
    response: Response = None,
    auto_refresh: bool = Query(default=False)
):
    user = basic_user or bearer_user
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")

    token = None
    if request is not None:
        token = request.cookies.get('fastapiusersauth')
    if token and auto_refresh:
        try:
            payload = jwt.decode(token, cfg['jwt_secret'], algorithms=["HS256"])
            exp = payload.get('exp')
            if exp - int(time.time()) <= 3 * 24 * 3600:
                new_token = await get_jwt_strategy().write_token(user)
                response.set_cookie(
                    'fastapiusersauth',
                    new_token,
                    httponly=True,
                    secure=cfg.get('cookie_secure', False),
                    max_age=cfg.get('jwt_lifetime_seconds', 0),
                    path='/'
                )
        except Exception:
            # 忽略解码错误，不影响返回用户信息
            pass

    return user

@router.patch("/users/{user_id}", response_model=UserRead, tags=["users"])
async def update_user_protected(
    user_id: int,
    user_update: UserUpdate,
    current_user: User = Depends(fastapi_users.current_user(active=True))
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

    async for user_manager in get_user_manager():
        user = await user_manager.user_db.get(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")

        updated_user = await user_manager.update(user_update, user, safe=False, request=None)
        return updated_user


@router.delete("/users/{user_id}", status_code=204, tags=["users"])
async def delete_user_protected(
    user_id: int,
    current_user: User = Depends(fastapi_users.current_user(active=True))
):
    """删除用户（仅管理员，保护 ID 为 1 的超级管理员）"""
    if user_id == 1:
        raise HTTPException(
            status_code=403,
            detail="无法删除超级管理员账户"
        )

    async for user_manager in get_user_manager():
        user = None
        if current_user.id != user_id and not current_user.is_superuser:
            user = await user_manager.get_sub_account(current_user, None, user_id)
            if not user:
                raise HTTPException(
                    status_code=403,
                    detail="无权删除"
                )

        if user is None:
            user = await user_manager.user_db.get(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")

        await user_manager.delete(user, request=None)
        return

@router.get("/users/subaccounts", response_model=List[UserRead], tags=["users"])
async def get_subaccounts(user=Depends(fastapi_users.current_user(active=True))):
    """获取子账户列表"""
    async with async_session_maker() as session:
        result = await session.execute(select(User).where(User.parent_id == user.id))
        users = result.scalars().all()
    return users

@router.get("/users/bind", response_model=List[UserRead], tags=["users"])
async def get_userbind(
    onlystock: Optional[bool] = Query(default=False),
    basic_user: Optional[User] = Depends(get_current_user_basic),
    bearer_user: Optional[User] = Depends(fastapi_users.current_user(optional=True))
):
    """获取子账户列表"""
    user = basic_user or bearer_user
    async with async_session_maker() as session:
        parent_id = user.parent_id or user.id
        result = await session.execute(select(User).where(User.parent_id == parent_id))
        users = result.scalars().all()
    return users

async def owner_user(user: User):
    if user.parent_id:
        async for user_manager in get_user_manager():
            return await user_manager.get(user.parent_id)
    return user

@router.get("/user/dealcategory")
async def get_dealcategory(
    basic_user: Optional[User] = Depends(get_current_user_basic),
    bearer_user: Optional[User] = Depends(fastapi_users.current_user(optional=True))
):
    user = await owner_user(basic_user or bearer_user)
    dcat = await usm.get_dealcategory(user)
    return [list(d) for d in dcat]

@router.get("/user/trackdeals")
async def get_trackdeals(
    name: Optional[str] = Query(default=None),
    realcash: Optional[bool] = Query(default=False),
    acc: Optional[str] = Query(None, embed=True),
    accid: Optional[int] = Query(None, embed=True),
    basic_user: Optional[User] = Depends(get_current_user_basic),
    bearer_user: Optional[User] = Depends(fastapi_users.current_user(optional=True))
):
    if name != 'archived':
        user = await verify_user(basic_user or bearer_user, acc, accid)
        return await usm.get_track_deals(user, name)
    else:
        ds = await usm.get_archived_deals(basic_user or bearer_user, realcash)
        return {'tname':name, 'deals': ds}

@router.post("/user/trackdeals", openapi_extra=pparam_doc([
    ("name", "string", "name", True),
    ("desc", "string", "desc", False),
    ("data", "string", "{}", False),
    ("acc", "string", "test", False),
    ("accid", "string", "8", False)
]))
async def track_deals(
    name: Optional[str] = PostParams.create("name", default=None),
    desc: Optional[int] = PostParams.create("desc", default=None),
    data: Optional[str] = PostParams.create("data", default=None),
    acc: Optional[str] = PostParams.create("acc", default=None),
    accid: Optional[int] = PostParams.create("accid", default=None),
    basic_user: Optional[User] = Depends(get_current_user_basic),
    bearer_user: Optional[User] = Depends(fastapi_users.current_user(optional=True))
):
    user = await verify_user(basic_user or bearer_user, acc, accid)
    await usm.add_track_deals(user, name, json.loads(data), desc)

@router.get("/user/archivedcodes")
async def get_archivedcodes(
    since: Optional[str] = Query(default=None),
    realcash: Optional[int] = Query(default=0),
    basic_user: Optional[User] = Depends(get_current_user_basic),
    bearer_user: Optional[User] = Depends(fastapi_users.current_user(optional=True))
):
    return list(await usm.get_archived_code_since(basic_user or bearer_user, since, realcash, True))
