from sqlalchemy import Column, String, Integer, SmallInteger, Float, LargeBinary, PrimaryKeyConstraint
from app.db import Base


class MdlDt3(Base):
    __tablename__ = "stock_dt3_pickup"

    code = Column(String(10), nullable=False)
    date = Column(String(20), nullable=False)
    date3 = Column(String(20), nullable=True)
    date4 = Column(String(20), nullable=True)
    buy = Column(SmallInteger, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('code', 'date', name='pk_dt3'),
    )
