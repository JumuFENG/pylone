from sqlalchemy import Column, String, Integer, SmallInteger, Float, LargeBinary, PrimaryKeyConstraint
from app.db import Base, engine


class MdlAllStock(Base):
    __tablename__ = "all_stocks"

    code = Column(String(20), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    typekind = Column(String(20), nullable=True)
    setup_date = Column(String(20), nullable=True)
    quit_date = Column(String(20), nullable=True)

class MdlStockList(Base):
    __tablename__ = "stock_lists"

    lkey = Column(String(20), nullable=False)
    code = Column(String(20), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('lkey', 'code', name='pk_stock_lists'),
    )

class MdlStockShare(Base):
    __tablename__ = "stock_bonus_shares"

    code = Column(String(20), nullable=False)
    report_date = Column(String(20), nullable=True)
    register_date = Column(String(20), nullable=True)
    ex_dividend_date = Column(String(20), nullable=True)
    progress = Column(String(20), nullable=True)
    total_bonus = Column(Float, default=0)
    bonus_share = Column(Float, default=0)
    transfer_share = Column(Float, default=0)
    cash_dividend = Column(Float, default=0)
    dividend_yield = Column(Float, default=0)
    eps = Column(Float, default=0)
    bvps = Column(Float, default=0)
    total_shares = Column(Float, nullable=True)
    bonus_details = Column(String(64), nullable=True)

    __table_args__ = (
        PrimaryKeyConstraint('code', 'report_date', name='pk_stock_bonus_shares'),
    )


class MdlSysSettings(Base):
    __tablename__ = "sys_settings"

    id = Column(Integer, nullable=False, primary_key=True)
    key = Column(String(64), nullable=False, unique=True)
    value = Column(String(255), nullable=False)
    name = Column(String(255), nullable=False, default="")
    valtype = Column(SmallInteger, nullable=False, default=0)


class MdlStockBk(Base):
    __tablename__ = "stock_bks"

    code = Column(String(10), nullable=False, primary_key=True)
    name = Column(String(255), nullable=False, default="")
    chgignore = Column(SmallInteger, nullable=False, default=0)


class MdlStockBkMap(Base):
    __tablename__ = "stock_bk_map"

    bk = Column(String(10), nullable=False)
    stock = Column(String(10), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('bk', 'stock', name='pk_stock_bk_map'),
    )


class MdlStockChanges(Base):
    __tablename__ = "stock_changes"

    code = Column(String(10), nullable=False)
    time = Column(String(20), nullable=False)
    chgtype = Column(SmallInteger, nullable=False)
    info = Column(String(64), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('code', 'time', 'chgtype', name='pk_stock_changes'),
    )


class MdlStockBkChanges(Base):
    __tablename__ = "stock_changes_embk"

    code = Column(String(10), nullable=False)
    time = Column(String(20), nullable=False)
    change = Column(Float, nullable=False)  #板块涨跌幅
    amount = Column(Float, nullable=False)  #主力净流入
    ydct = Column(Float, nullable=False)  #异动次数
    ydpos = Column(Integer, nullable=False)  # 正异动
    ydabs = Column(Integer, nullable=False)  # 绝对正异动  正异动-负异动
    ztcnt = Column(Integer, nullable=False)  # 涨停数 封涨停-打开涨停
    dtcnt = Column(Integer, nullable=False)  # 跌停数 封跌停-打开跌停
    y4 = Column(Integer, nullable=False, default=0)
    y8 = Column(Integer, nullable=False, default=0)
    y16 = Column(Integer, nullable=False, default=0)
    y32 = Column(Integer, nullable=False, default=0)
    y64 = Column(Integer, nullable=False, default=0)
    y128 = Column(Integer, nullable=False, default=0)
    y8193 = Column(Integer, nullable=False, default=0)
    y8194 = Column(Integer, nullable=False, default=0)
    y8201 = Column(Integer, nullable=False, default=0)
    y8202 = Column(Integer, nullable=False, default=0)
    y8203 = Column(Integer, nullable=False, default=0)
    y8204 = Column(Integer, nullable=False, default=0)
    y8207 = Column(Integer, nullable=False, default=0)
    y8208 = Column(Integer, nullable=False, default=0)
    y8209 = Column(Integer, nullable=False, default=0)
    y8210 = Column(Integer, nullable=False, default=0)
    y8211 = Column(Integer, nullable=False, default=0)
    y8212 = Column(Integer, nullable=False, default=0)
    y8213 = Column(Integer, nullable=False, default=0)
    y8214 = Column(Integer, nullable=False, default=0)
    y8215 = Column(Integer, nullable=False, default=0)
    y8216 = Column(Integer, nullable=False, default=0)
    y8217 = Column(Integer, nullable=False, default=0)
    y8218 = Column(Integer, nullable=False, default=0)
    y8219 = Column(Integer, nullable=False, default=0)
    y8220 = Column(Integer, nullable=False, default=0)
    y8221 = Column(Integer, nullable=False, default=0)
    y8222 = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        PrimaryKeyConstraint('code', 'time', name='pk_stock_changes_embk'),
    )


class MdlStockBkClsChanges(Base):
    __tablename__ = "stock_changes_clsbk"

    code = Column(String(10), nullable=False)
    time = Column(String(20), nullable=False)
    change = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    ztcnt = Column(Integer, nullable=False)
    dtcnt = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('code', 'time', name='pk_stock_changes_clsbk'),
    )


class MdlDayZtStocks(Base):
    __tablename__ = "day_zt_stocks"

    code = Column(String(10), nullable=False)
    time = Column(String(20), nullable=False)
    fund = Column(Float, nullable=False) #涨停封单
    hsl = Column(Float, nullable=False) #换手率
    lbc = Column(Integer, nullable=False) #连板数
    days = Column(Integer, nullable=False) #总天数
    zbc = Column(Integer, nullable=False) #炸板数
    bk = Column(String(63), nullable=False) #板块
    cpt = Column(String(255), nullable=False) #概念
    mkt = Column(SmallInteger, nullable=False) #市场 0:主板 1:科创创业 2:北交所 3:ST

    __table_args__ = (
        PrimaryKeyConstraint('code', 'time', name='pk_day_zt_stocks'),
    )


class MdlDayDtStocks(Base):
    __tablename__ = "day_dt_stocks"

    code = Column(String(10), nullable=False)
    time = Column(String(20), nullable=False)
    fund = Column(Float, nullable=False) #跌停封单
    fba = Column(Float, nullable=False) #板上成交额
    hsl = Column(Float, nullable=False) #换手率
    lbc = Column(Integer, nullable=False) #连板数
    zbc = Column(Integer, nullable=False) #炸板数/开板数
    bk = Column(String(63), nullable=False) #板块
    mkt = Column(SmallInteger, nullable=False) #市场 0:主板 1:科创创业 2:北交所 3:ST

    __table_args__ = (
        PrimaryKeyConstraint('code', 'time', name='pk_day_dt_stocks'),
    )


class MdlDayZtConcepts(Base):
    __tablename__ = "day_zt_concepts"

    time = Column(String(20), nullable=False)
    cpt = Column(String(255), nullable=False)
    ztcnt = Column(Integer, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('time', 'cpt', name='pk_day_zt_concepts'),
    )


class MdlDayDtMap(Base):
    __tablename__ = "day_dt_maps"

    time = Column(String(20), nullable=False)
    code = Column(String(10), nullable=False)
    step = Column(SmallInteger, nullable=False)
    success = Column(SmallInteger, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('time', 'code', name='pk_day_dt_maps'),
    )


class MdlSMStats(Base):
    __tablename__ = "stock_market_stats"

    date = Column(String(10), nullable=False)
    time = Column(String(10), nullable=False)
    stats = Column(LargeBinary, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('date', 'time', name='pk_stock_market_stats'),
    )

