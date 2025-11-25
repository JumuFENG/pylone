from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.lofig import Config

cfg = {'user': 'root', 'password': '', 'host': 'localhost', 'port': 3306, 'dbname': 'user_management'}
cfg.update(Config.database_config())

DATABASE_URL = f"mysql+aiomysql://{cfg['user']}:{Config.simple_decrypt(cfg['password'])}@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
async_session_maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

async def get_async_session() -> AsyncSession:
    async with async_session_maker() as session:
        yield session
