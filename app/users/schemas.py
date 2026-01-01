from fastapi_users import schemas
from typing import Optional
from pydantic import Field, BaseModel

class UserRead(schemas.BaseUser[int]):
    username: str
    nickname: Optional[str]
    parent_id: Optional[int]
    realcash: Optional[int]

class UserCreate(schemas.BaseUserCreate):
    username: str
    nickname: Optional[str] = None
    parent_id: Optional[int] = None
    realcash: Optional[int] = 0

class UserUpdate(schemas.BaseUserUpdate):
    username: Optional[str] = None
    nickname: Optional[str] = None
    parent_id: Optional[int] = None
    realcash: Optional[int] = None

class TokenRequest(BaseModel):
    id: Optional[int] = None
    username: Optional[str] = None
    email: Optional[str] = None
    password: str

class TokenResponse(BaseModel):
    access_token: str

