from typing import Optional
from pydantic import BaseModel


class UserBase(BaseModel):
    """Base user schema with common fields"""
    username: str
    nickname: Optional[str] = None
    email: Optional[str] = None


class UserRead(UserBase):
    """Schema for reading user data"""
    id: int
    parent_id: Optional[int] = None
    realcash: Optional[int] = 0
    is_active: Optional[bool] = True
    is_superuser: Optional[bool] = False

    class Config:
        from_attributes = True


class UserCreate(UserBase):
    """Schema for creating a new user"""
    password: str
    parent_id: Optional[int] = None
    realcash: Optional[int] = 0


class UserUpdate(BaseModel):
    """Schema for updating user data"""
    username: Optional[str] = None
    nickname: Optional[str] = None
    email: Optional[str] = None
    password: Optional[str] = None
    parent_id: Optional[int] = None
    realcash: Optional[int] = None
    is_active: Optional[bool] = None
    is_superuser: Optional[bool] = None


class TokenRequest(BaseModel):
    id: Optional[int] = None
    username: Optional[str] = None
    email: Optional[str] = None
    password: str


class TokenResponse(BaseModel):
    access_token: str
