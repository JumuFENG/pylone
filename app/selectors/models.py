from sqlalchemy import Column, String, Integer, SmallInteger, Float, LargeBinary, PrimaryKeyConstraint
from app.db import Base


class MdlDt3(Base):
    __tablename__ = "stock_dt3_pickup"

    date = Column(String(20), nullable=False)
    code = Column(String(10), nullable=False)
    date3 = Column(String(20), nullable=True)
    date4 = Column(String(20), nullable=True)
    buy = Column(SmallInteger, nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint('code', 'date', name='pk_dt3'),
    )

class MdlHotstksOpen(Base):
    __tablename__ = "stock_day_hotstks_open"

    date = Column(String(20), nullable=False)
    code = Column(String(10), nullable=False)
    zdate = Column(String(20), nullable=True)  # 涨停日期
    days = Column(SmallInteger, default=0)
    step = Column(SmallInteger, default=0)
    emrk = Column(SmallInteger, default=0)

    __table_args__ = (
        PrimaryKeyConstraint('code', 'date', name='pk_hotstks_open'),
    )


class MdlZt0hrst0(Base):
    __tablename__ = "stock_day_hotstks_retry_zt0"

    date = Column(String(20), nullable=False)
    code = Column(String(10), nullable=False)
    days = Column(SmallInteger, default=0)
    step = Column(SmallInteger, default=0)
    remdays = Column(SmallInteger, default=0)
    dropdate = Column(String(20), nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint('code', 'date', name='pk_zt0hrst0'),
    )


class MdlZt1wb(Base):
    __tablename__ = "stock_zt1wb_pickup"

    date = Column(String(20), nullable=False)
    code = Column(String(10), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('code', 'date', name='pk_zt1wb'),
    )


class MdlDayZdtEmotion(Base):
    __tablename__ = "stock_day_zdtemotion"

    date = Column(String(20), nullable=False, primary_key=True)
    ztcnt = Column(SmallInteger, default=0)
    zt0cnt = Column(SmallInteger, default=0)
    dtcnt = Column(SmallInteger, default=0)
    amount = Column(Float, default=0)


class Mdl3Bull(Base):
    __tablename__ = "stock_tripple_bull_pickup"

    date = Column(String(20), nullable=False)
    code = Column(String(10), nullable=False)
    prepk = Column(SmallInteger, default=1)
    bdate = Column(String(20), nullable=False)
    fdate = Column(String(20), nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint('code', 'date', name='pk_3bull'),
    )
