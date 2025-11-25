from fastapi_users import schemas
from typing import Optional
from pydantic import Field

class UserRead(schemas.BaseUser[int]):
    username: str

class UserCreate(schemas.BaseUserCreate):
    username: str

class UserUpdate(schemas.BaseUserUpdate):
    username: Optional[str] = None
