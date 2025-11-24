from contextlib import asynccontextmanager
from typing import List
from fastapi import FastAPI, Depends, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from app.lofig import Config, logger
from app.db import engine, Base, User, get_async_session
from app.schemas import UserCreate, UserRead, UserUpdate
from app.users import auth_backend, fastapi_users, current_active_user
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时创建数据库表
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables created")
    yield
    # 关闭时清理
    logger.info("Shutting down")


app = FastAPI(title="User Management API", lifespan=lifespan)

# 注册认证路由
app.include_router(
    fastapi_users.get_auth_router(auth_backend),
    prefix="/auth/jwt",
    tags=["auth"],
)

# 注册用户注册路由
app.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    prefix="/auth",
    tags=["auth"],
)

# 注册用户管理路由
app.include_router(
    fastapi_users.get_users_router(UserRead, UserUpdate),
    prefix="/users",
    tags=["users"],
)


@app.get("/")
async def root():
    return RedirectResponse(url="/login.html")


@app.get("/protected-route")
async def protected_route(user: User = Depends(current_active_user)):
    return {"message": f"Hello {user.email}!", "user_id": user.id}


# 管理员专用：获取所有用户列表
@app.get("/admin/users", response_model=List[UserRead], tags=["admin"])
async def get_all_users(
    user: User = Depends(current_active_user),
    session: AsyncSession = Depends(get_async_session)
):
    if not user.is_superuser:
        raise HTTPException(status_code=403, detail="需要管理员权限")
    
    result = await session.execute(select(User))
    users = result.scalars().all()
    return users


# 挂载静态文件
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/", StaticFiles(directory="static", html=True), name="html")


if __name__ == '__main__':
    import uvicorn
    cfg = Config.all_configs()
    uvicorn.run("main:app", host="0.0.0.0", port=cfg.get('client', {}).get('port', 8000), reload=True)
