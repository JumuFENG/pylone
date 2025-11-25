from fastapi_users.db import SQLAlchemyBaseUserTable
from sqlalchemy import Column, String, Integer
from app.db import Base

class User(SQLAlchemyBaseUserTable, Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
