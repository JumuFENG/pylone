from sqlalchemy import Column, String, Integer, SmallInteger, Float, PrimaryKeyConstraint
from app.db import Base, engine


class MdlAllStock(Base):
    __tablename__ = "all_stocks"

    code = Column(String(20), primary_key=True, nullable=False)
    name = Column(String(255), nullable=False)
    typekind = Column(String(20), nullable=True)
    setup_date = Column(String(20), nullable=True)
    quit_date = Column(String(20), nullable=True)

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

    key = Column(String(20), nullable=False, primary_key=True)
    value = Column(String(255), nullable=False)


class UserStocks(Base):
    __tablename__ = "user_stocks"

    user_id = Column(Integer, nullable=False)
    code = Column(String(10), nullable=False)
    cost_hold = Column(Float)
    portion_hold = Column(Float)
    aver_price = Column(Float)
    keep_eye = Column(SmallInteger, default=1)
    fee = Column(Float)
    amount = Column(Integer)
    uramount = Column(String(255))

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'code', name='pk_user_stocks'),
    )

class UserStrategy(Base):
    __tablename__ = "user_strategy"
    user_id = Column(Integer, nullable=False)
    code = Column(String(10), primary_key=True)
    id = Column(Integer, primary_key=True)
    skey = Column(String(64))
    trans = Column(SmallInteger)
    data = Column(String(255))

class UserCostdog(Base):
    __tablename__ = "user_costdog"
    ckey = Column(String(20), primary_key=True)
    user_id = Column(Integer, nullable=False)
    data = Column(String(255))

class UcostdogUrque(Base):
    __tablename__ = "ucostdog_urque"
    ckey = Column(String(20), primary_key=True)
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    urdata = Column(String(255))

class UserOrders(Base):
    __tablename__ = "user_orders"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    code = Column(String(10))
    date = Column(String(20))
    count = Column(Integer)
    price = Column(Float)
    sid = Column(String(10))
    typebs = Column(String(10))

class UserEarned(Base):
    __tablename__ = "user_earned"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    date = Column(String(20))
    earned = Column(Float)
    total_earned = Column(Float)

class UserEarning(Base):
    __tablename__ = "user_earning"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    date = Column(String(20))
    cost = Column(Float)
    市值 = Column(Float)

class DealsTable:
    user_id = Column(Integer, nullable=False)
    date = Column(String(20))
    code = Column(String(10))
    typebs = Column(String(10))
    sid = Column(String(10))
    price = Column(Float)
    portion = Column(Integer)
    fee = Column(Float)
    feeYh = Column(Float)
    feeGh = Column(Float)

class UserDeals(Base, DealsTable):
    __tablename__ = "user_deals"
    id = Column(Integer, primary_key=True, autoincrement=True)

class UserUnknownDeals(Base, DealsTable):
    __tablename__ = "user_deals_unknown"
    id = Column(Integer, primary_key=True, autoincrement=True)

class UserArchivedDeals(Base, DealsTable):
    __tablename__ = "user_deals_archived"
    id = Column(Integer, primary_key=True, autoincrement=True)

class UserStockBuy(Base):
    __tablename__ = "user_stock_buy"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    date = Column(String(20))
    code = Column(String(10))
    portion = Column(Integer)
    price = Column(Float)
    cost = Column(Float)
    soldout = Column(SmallInteger, default=0)
    soldptn = Column(Integer)
    sid = Column(String(10))
    fee = Column(Float)
    feeYh = Column(Float)
    feeGh = Column(Float)

class UserStockSell(Base):
    __tablename__ = "user_stock_sell"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    date = Column(String(20))
    code = Column(String(10))
    portion = Column(Integer)
    price = Column(Float)
    money_sold = Column(Float)
    cost_sold = Column(Float)
    earned = Column(Float)
    return_percent = Column(Float)
    rolled_in = Column(Integer)
    rollin_netvalue = Column(Float)
    sid = Column(String(10))
    fee = Column(Float)
    feeYh = Column(Float)
    feeGh = Column(Float)
