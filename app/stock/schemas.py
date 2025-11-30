from fastapi_users import schemas
from typing import Optional
from pydantic import Field, BaseModel


class PmStock(BaseModel):
    code: str
    name: Optional[str]
    typekind: Optional[str] = None
    setup_date: Optional[str] = None
    quit_date: Optional[str] = None
