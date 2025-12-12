from fastapi_users import schemas
from typing import Optional
from pydantic import Field, BaseModel


class PmStock(BaseModel):
    code: str
    name: Optional[str]
    typekind: Optional[str] = None
    setup_date: Optional[str] = None
    quit_date: Optional[str] = None


class KNode(BaseModel):
    time: str
    open: float
    close: float
    high: float
    low: float
    volume: Optional[float] = 0
    amount: Optional[float] = 0
    change: Optional[float] = 0
    change_px: Optional[float] = 0
    amplitude: Optional[float] = 0
    turnover: Optional[float] = 0
