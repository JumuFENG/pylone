from fastapi_users import FastAPIUsers, BaseUserManager
from fastapi_users.authentication import CookieTransport, AuthenticationBackend, JWTStrategy
from fastapi_users.db import SQLAlchemyUserDatabase
from typing import AsyncGenerator, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Request

from app.lofig import Config, logger
from app.db import async_session_maker
from .models import User
from .schemas import UserRead, UserCreate, UserUpdate


cfg = {'jwt_secret': 'JWT_SECRET_SHOULD_CHANGE_IN_PRODUCTION', 'jwt_lifetime_seconds': 86400, 'cookie_secure': False}
cfg.update(Config.client_config())

cookie_transport = CookieTransport(cookie_secure=cfg['cookie_secure'])
def get_jwt_strategy() -> JWTStrategy:
    return JWTStrategy(secret=cfg['jwt_secret'], lifetime_seconds=cfg['jwt_lifetime_seconds'])

auth_backend = AuthenticationBackend(
    name="jwt",
    transport=cookie_transport,
    get_strategy=get_jwt_strategy,
)

async def get_user_db() -> AsyncGenerator[SQLAlchemyUserDatabase, None]:
    async with async_session_maker() as session:
        yield SQLAlchemyUserDatabase(session, User)


class UserManager(BaseUserManager[User, int]):
    def parse_id(self, value):
        return int(value)


async def get_user_manager():
    async for user_db in get_user_db():
        yield UserManager(user_db)


fastapi_users = FastAPIUsers[User, int](
    get_user_manager,
    [auth_backend],
)

current_active_user = fastapi_users.current_user(active=True)
current_superuser = fastapi_users.current_user(superuser=True)
