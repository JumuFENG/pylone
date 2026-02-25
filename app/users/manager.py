import jwt
import time
from typing import AsyncGenerator, Optional
from sqlalchemy import select
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.security.http import HTTPBearer, HTTPAuthorizationCredentials

from app.lofig import Config, logger
from app.db import async_session_maker
from .models import User
from .schemas import UserRead, UserCreate

cfg = {'jwt_secret': 'JWT_SECRET_SHOULD_CHANGE_IN_PRODUCTION', 'jwt_lifetime_seconds': 2592000, 'cookie_secure': False}
cfg.update(Config.client_config())

# Detect if fastapi_users is available
FASTAPI_USERS_AVAILABLE = False
try:
    from fastapi_users import FastAPIUsers, BaseUserManager, exceptions
    from fastapi_users.authentication import (
        CookieTransport,
        BearerTransport,
        AuthenticationBackend,
        JWTStrategy
    )
    from fastapi_users.db import SQLAlchemyUserDatabase
    from fastapi_users.password import PasswordHelper
    FASTAPI_USERS_AVAILABLE = True
except Exception:
    pass

if not FASTAPI_USERS_AVAILABLE:
    # Fallback implementations when fastapi_users is not available
    from passlib.context import CryptContext

    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    class FallbackPasswordHelper:
        """Fallback password helper using passlib/bcrypt"""

        @staticmethod
        def hash(password: str) -> str:
            return pwd_context.hash(password)

        @staticmethod
        def verify(plain_password: str, hashed_password: str) -> bool:
            return pwd_context.verify(plain_password, hashed_password)

        @staticmethod
        def verify_and_update(plain_password: str, hashed_password: str):
            verified = pwd_context.verify(plain_password, hashed_password)
            updated_hash = None
            if verified and pwd_context.needs_update(hashed_password):
                updated_hash = pwd_context.hash(plain_password)
            return verified, updated_hash

        @staticmethod
        def generate() -> str:
            import secrets
            return secrets.token_urlsafe(12)

    class FallbackJWTStrategy:
        """Fallback JWT strategy using PyJWT"""

        def __init__(self, secret: str, lifetime_seconds: int):
            self.secret = secret
            self.lifetime_seconds = lifetime_seconds

        async def write_token(self, user) -> str:
            payload = {
                'sub': str(user.id),
                'email': getattr(user, 'email', None),
                'username': getattr(user, 'username', None),
                'exp': int(time.time()) + self.lifetime_seconds
            }
            return jwt.encode(payload, self.secret, algorithm="HS256")

        async def read_token(self, token: str) -> Optional[dict]:
            try:
                return jwt.decode(token, self.secret, algorithms=["HS256"])
            except jwt.InvalidTokenError:
                return None

        async def destroy_token(self, token: str) -> None:
            pass

    def get_jwt_strategy() -> FallbackJWTStrategy:
        return FallbackJWTStrategy(secret=cfg['jwt_secret'], lifetime_seconds=cfg['jwt_lifetime_seconds'])

    # Minimal fallback for SQLAlchemyUserDatabase
    class FallbackUserDatabase:
        def __init__(self, session, user_model):
            self.session = session
            self.user_model = user_model

        async def get(self, id):
            result = await self.session.execute(
                select(self.user_model).where(self.user_model.id == id)
            )
            return result.scalar_one_or_none()

        async def get_by_email(self, email):
            result = await self.session.execute(
                select(self.user_model).where(self.user_model.email == email)
            )
            return result.scalar_one_or_none()

        async def create(self, create_dict):
            user = self.user_model(**create_dict)
            self.session.add(user)
            await self.session.commit()
            await self.session.refresh(user)
            return user

        async def update(self, user):
            await self.session.commit()
            return user

        async def delete(self, user):
            await self.session.delete(user)
            await self.session.commit()

    async def get_user_db() -> AsyncGenerator[FallbackUserDatabase, None]:
        async with async_session_maker() as session:
            yield FallbackUserDatabase(session, User)

    class FallbackUserManager:
        def __init__(self, user_db):
            self.user_db = user_db
            self.password_helper = FallbackPasswordHelper()

        async def get(self, id):
            return await self.user_db.get(id)

        async def get_by_email(self, email):
            return await self.user_db.get_by_email(email)

        async def create(self, user_create: UserCreate, safe: bool = True):
            # Check if user already exists
            existing = await self.user_db.get_by_email(user_create.email) if user_create.email else None
            if existing:
                raise Exception("User already exists")

            # Also check username
            result = await self.user_db.session.execute(
                select(User).where(User.username == user_create.username)
            )
            if result.scalar_one_or_none():
                raise Exception("Username already exists")

            # Hash password
            hashed_password = self.password_helper.hash(user_create.password)

            user_dict = {
                'username': user_create.username,
                'hashed_password': hashed_password,
                'email': user_create.email,
            }
            if hasattr(user_create, 'nickname'):
                user_dict['nickname'] = user_create.nickname
            if hasattr(user_create, 'parent_id'):
                user_dict['parent_id'] = user_create.parent_id
            if hasattr(user_create, 'realcash'):
                user_dict['realcash'] = user_create.realcash

            return await self.user_db.create(user_dict)

        async def authenticate(self, credentials: HTTPBasicCredentials) -> Optional[User]:
            if not credentials:
                return None

            # Try to find user by email or username
            user = await self.user_db.get_by_email(credentials.username)
            if not user:
                result = await self.user_db.session.execute(
                    select(User).where(User.username == credentials.username)
                )
                user = result.scalar_one_or_none()

            if not user:
                self.password_helper.hash(credentials.password)
                return None

            verified, updated_password_hash = self.password_helper.verify_and_update(
                credentials.password, user.hashed_password
            )
            if not verified:
                logger.error(f"Authentication error: {credentials.username} not verified!")
                return None

            if updated_password_hash is not None:
                user.hashed_password = updated_password_hash
                await self.user_db.update(user)

            return user

        async def get_sub_account(self, user: User, acc: str = None, accid: int = None) -> Optional[User]:
            if not acc and not accid:
                return user

            if acc:
                result = await self.user_db.session.execute(
                    select(User).where(User.username == acc, User.parent_id == user.id)
                )
                subaccount = result.scalar_one_or_none()
                if '.' not in acc and not subaccount:
                    acc = f'{user.username}.{acc}'
                    result = await self.user_db.session.execute(
                        select(User).where(User.username == acc, User.parent_id == user.id)
                    )
                    subaccount = result.scalar_one_or_none()
                return subaccount
            if accid:
                result = await self.user_db.session.execute(
                    select(User).where(User.id == accid, User.parent_id == user.id)
                )
                return result.scalar_one_or_none()
            return user

        async def delete(self, user, request=None):
            await self.user_db.delete(user)

        async def update(self, user_update, user, safe=True, request=None):
            update_data = user_update.model_dump(exclude_unset=True)
            if 'password' in update_data:
                update_data['hashed_password'] = self.password_helper.hash(update_data.pop('password'))

            for field, value in update_data.items():
                setattr(user, field, value)

            return await self.user_db.update(user)

    # Fallback auth backends
    class FallbackAuthBackend:
        def __init__(self, name, transport, get_strategy):
            self.name = name
            self.transport = transport
            self.get_strategy = get_strategy

        async def login(self, user, request=None):
            token = await self.get_strategy().write_token(user)
            return {"access_token": token, "token_type": "bearer"}

        async def logout(self, user, request=None):
            return None

    class FallbackCookieTransport:
        def __init__(self, cookie_secure=False):
            self.cookie_secure = cookie_secure

        async def get_login_response(self, token, response=None):
            return {"access_token": token}

        async def get_logout_response(self, response=None):
            return None

    class FallbackBearerTransport:
        def __init__(self, tokenUrl):
            self.tokenUrl = tokenUrl

        async def get_login_response(self, token):
            return {"access_token": token, "token_type": "bearer"}

        async def get_logout_response(self):
            return None

    # Create fallback backends
    cookie_transport = FallbackCookieTransport(cookie_secure=cfg['cookie_secure'])
    bearer_transport = FallbackBearerTransport(tokenUrl="auth/bearer/login")

    cookie_auth_backend = FallbackAuthBackend(
        name="jwt-cookie",
        transport=cookie_transport,
        get_strategy=get_jwt_strategy,
    )

    bearer_auth_backend = FallbackAuthBackend(
        name="jwt-bearer",
        transport=bearer_transport,
        get_strategy=get_jwt_strategy,
    )

    # Mock fastapi_users with minimal functionality
    class FallbackFastAPIUsers:
        def __init__(self, get_user_manager, auth_backends):
            self.get_user_manager = get_user_manager
            self.auth_backends = auth_backends

        def get_auth_router(self, backend):
            from fastapi import APIRouter
            router = APIRouter()

            @router.post("/login")
            async def login(request, response, credentials: HTTPBasicCredentials = Depends(HTTPBasic())):
                async for user_manager in self.get_user_manager():
                    user = await user_manager.authenticate(credentials)
                    if not user:
                        raise HTTPException(status_code=401, detail="Invalid credentials")

                    if backend.name == "jwt-cookie":
                        token = await backend.get_strategy().write_token(user)
                        response.set_cookie(
                            key="fastapiusersauth",
                            value=token,
                            httponly=True,
                            secure=cfg.get('cookie_secure', False),
                            max_age=cfg.get('jwt_lifetime_seconds', 0),
                            path="/"
                        )
                        return {"access_token": token}
                    else:
                        token = await backend.get_strategy().write_token(user)
                        return {"access_token": token, "token_type": "bearer"}

            @router.post("/logout")
            async def logout(response):
                response.delete_cookie("fastapiusersauth")
                return None

            return router

        def get_register_router(self, user_schema, user_create_schema):
            from fastapi import APIRouter
            router = APIRouter()

            @router.post("/register", response_model=user_schema)
            async def register(user_create: user_create_schema):
                async for user_db in get_user_db():
                    user_manager = FallbackUserManager(user_db)
                    try:
                        user = await user_manager.create(user_create)
                        return user
                    except Exception as e:
                        raise HTTPException(status_code=400, detail=str(e))

            return router

        def current_user(self, optional=False, superuser=False, active=False):
            async def dependency(
                credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=not optional)),
                basic_credentials: HTTPBasicCredentials = Depends(HTTPBasic(auto_error=not optional))
            ):
                token = None
                if credentials:
                    token = credentials.credentials
                elif basic_credentials:
                    # For basic auth
                    async for user_manager in self.get_user_manager():
                        user = await user_manager.authenticate(basic_credentials)
                        return user

                if not token:
                    if optional:
                        return None
                    raise HTTPException(status_code=401, detail="Not authenticated")

                strategy = get_jwt_strategy()
                payload = await strategy.read_token(token)
                if not payload:
                    if optional:
                        return None
                    raise HTTPException(status_code=401, detail="Invalid token")

                user_id = payload.get('sub')
                if not user_id:
                    if optional:
                        return None
                    raise HTTPException(status_code=401, detail="Invalid token payload")

                async for user_db in get_user_db():
                    user_manager = FallbackUserManager(user_db)
                    user = await user_manager.get(int(user_id))

                    if not user:
                        if optional:
                            return None
                        raise HTTPException(status_code=401, detail="User not found")

                    if superuser and not getattr(user, 'is_superuser', False):
                        raise HTTPException(status_code=403, detail="Not enough permissions")

                    if active and not getattr(user, 'is_active', True):
                        raise HTTPException(status_code=403, detail="User is inactive")

                    return user

            return dependency

    async def get_user_manager():
        async for user_db in get_user_db():
            yield FallbackUserManager(user_db)

    async def get_current_user_basic(
        credentials: HTTPBasicCredentials = Depends(HTTPBasic(auto_error=False))
    ) -> Optional[User]:
        if credentials is None:
            return None
        async for user_manager in get_user_manager():
            return await user_manager.authenticate(credentials)

    fastapi_users = FallbackFastAPIUsers(get_user_manager, [cookie_auth_backend, bearer_auth_backend])
    current_superuser = fastapi_users.current_user(superuser=True)

    async def verify_user(parent, acc=None, accid=None):
        if not parent:
            raise HTTPException(status_code=401, detail="Invalid verify token")
        if parent.id == accid or parent.username == acc:
            return parent
        async for user_manager in get_user_manager():
            subuser = await user_manager.get_sub_account(parent, acc, accid)
        return subuser

else:
    # Original fastapi_users implementation
    from fastapi_users import FastAPIUsers, BaseUserManager, exceptions
    from fastapi_users.authentication import (
        CookieTransport,
        BearerTransport,
        AuthenticationBackend,
        JWTStrategy
    )
    from fastapi_users.db import SQLAlchemyUserDatabase
    from fastapi_users.password import PasswordHelper

    cookie_transport = CookieTransport(cookie_secure=cfg['cookie_secure'])
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
            try:
                if credentials.username.isdigit():
                    user = await self.user_db.get(self.parse_id(credentials.username))
                else:
                    user = await self.user_db.get_by_email(credentials.username)

                if not user:
                    async with async_session_maker() as session:
                        result = await session.execute(
                            select(User).where(User.username == credentials.username)
                        )
                        user = result.scalar_one_or_none()

                if not user:
                    self.password_helper.hash(credentials.password)
                    return None

                verified, updated_password_hash = self.password_helper.verify_and_update(
                    credentials.password, user.hashed_password
                )
                if not verified:
                    logger.error(f"Authentication error: {credentials.username} not verified!")
                    return None

                if updated_password_hash is not None:
                    user.hashed_password = updated_password_hash
                    await self.user_db.update(user)

                return user
            except Exception as e:
                logger.error(f"Authentication error: {e}")
                return None

        async def get_sub_account(self, user: User, acc: str = None, accid: int = None) -> Optional[User]:
            if not acc and not accid:
                return user

            if acc:
                async with async_session_maker() as session:
                    result = await session.execute(select(User).where(User.username == acc, User.parent_id == user.id))
                    subaccount = result.scalar_one_or_none()
                    if '.' not in acc and not subaccount:
                        acc = f'{user.username}.{acc}'
                        result = await session.execute(select(User).where(User.username == acc, User.parent_id == user.id))
                        subaccount = result.scalar_one_or_none()
                return subaccount
            if accid:
                async with async_session_maker() as session:
                    result = await session.execute(select(User).where(User.id == accid, User.parent_id == user.id))
                    subaccount = result.scalar_one_or_none()
                return subaccount
            return user

    async def get_user_manager():
        async for user_db in get_user_db():
            yield UserManager(user_db)

    async def get_current_user_basic(
        credentials: HTTPBasicCredentials = Depends(HTTPBasic(auto_error=False))
    ) -> Optional[User]:
        if credentials is None:
            return None
        async for user_manager in get_user_manager():
            return await user_manager.authenticate(credentials)

    fastapi_users = FastAPIUsers[User, int](
        get_user_manager,
        [cookie_auth_backend, bearer_auth_backend],
    )

    current_superuser = fastapi_users.current_user(superuser=True)

    async def verify_user(parent, acc=None, accid=None):
        if not parent:
            raise exceptions.InvalidVerifyToken()
        if parent.id == accid or parent.username == acc:
            return parent
        async for user_manager in get_user_manager():
            subuser = await user_manager.get_sub_account(parent, acc, accid)
        return subuser
