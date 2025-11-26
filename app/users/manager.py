from fastapi_users import FastAPIUsers, BaseUserManager, exceptions
from fastapi_users.authentication import (
    CookieTransport,
    BearerTransport,
    AuthenticationBackend,
    JWTStrategy
)
from fastapi_users.db import SQLAlchemyUserDatabase
from sqlalchemy import select
from typing import AsyncGenerator, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Request

from app.lofig import Config, logger
from app.db import async_session_maker
from .models import User
from .schemas import UserRead, UserCreate, UserUpdate


cfg = {'jwt_secret': 'JWT_SECRET_SHOULD_CHANGE_IN_PRODUCTION', 'jwt_lifetime_seconds': 86400, 'cookie_secure': False}
cfg.update(Config.client_config())

# Cookie 认证传输
cookie_transport = CookieTransport(cookie_secure=cfg['cookie_secure'])

# Bearer Token 认证传输
bearer_transport = BearerTransport(tokenUrl="auth/bearer/login")

def get_jwt_strategy() -> JWTStrategy:
    return JWTStrategy(secret=cfg['jwt_secret'], lifetime_seconds=cfg['jwt_lifetime_seconds'])

cookie_auth_backend = AuthenticationBackend(
    name="jwt-cookie",
    transport=cookie_transport,
    get_strategy=get_jwt_strategy,
)

bearer_auth_backend = AuthenticationBackend(
    name="jwt-bearer",
    transport=bearer_transport,
    get_strategy=get_jwt_strategy,
)

async def get_user_db() -> AsyncGenerator[SQLAlchemyUserDatabase, None]:
    async with async_session_maker() as session:
        yield SQLAlchemyUserDatabase(session, User)


class UserManager(BaseUserManager[User, int]):
    def parse_id(self, value):
        return int(value)

    async def authenticate(self, credentials):
        """
        自定义认证方法，支持使用 id, email 或 username 登录
        """

        try:
            if credentials.username.isdigit():
                user = await self.user_db.get(self.parse_id(credentials.username))
            else:
                user = await self.user_db.get_by_email(credentials.username)

            if not user:
                # 如果通过 email/id 没找到，尝试通过 username 查找
                async with async_session_maker() as session:
                    result = await session.execute(
                        select(User).where(User.username == credentials.username)
                    )
                    user = result.scalar_one_or_none()

            if not user:
                # 运行密码验证以防止时序攻击
                self.password_helper.hash(credentials.password)
                return None

            # 验证密码
            verified, updated_password_hash = self.password_helper.verify_and_update(
                credentials.password, user.hashed_password
            )
            if not verified:
                return None

            # 如果密码哈希需要更新（例如算法升级）
            if updated_password_hash is not None:
                user.hashed_password = updated_password_hash
                await self.user_db.update(user)

            return user
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return None


async def get_user_manager():
    async for user_db in get_user_db():
        yield UserManager(user_db)


fastapi_users = FastAPIUsers[User, int](
    get_user_manager,
    [cookie_auth_backend, bearer_auth_backend],
)

current_active_user = fastapi_users.current_user(active=True)
current_superuser = fastapi_users.current_user(superuser=True)
