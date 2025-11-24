from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String
from fastapi_users.db import SQLAlchemyBaseUserTable
from app.lofig import Config

cfg = Config.all_configs().get('database', {})

SQLALCHEMY_DATABASE_URL = f"mysql+aiomysql://{cfg.get('user', 'root')}:{cfg.get('password', '')}@{cfg.get('host', 'localhost')}:{cfg.get('port', 3306)}/{cfg.get('database', 'user_management')}"

engine = create_async_engine(SQLALCHEMY_DATABASE_URL)
async_session_maker = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        yield session


class User(SQLAlchemyBaseUserTable[int], Base):
    __tablename__ = "user"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String(1024), nullable=False)
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)
    is_superuser: Mapped[bool] = mapped_column(default=False, nullable=False)
    is_verified: Mapped[bool] = mapped_column(default=False, nullable=False)
    
    # 可以添加自定义字段
    username: Mapped[str] = mapped_column(String(50), unique=True, index=True, nullable=True)
