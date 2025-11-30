from sqlalchemy import Column, String, Integer, SmallInteger, Float, PrimaryKeyConstraint
from app.db import Base, engine


class MdlAllStock(Base):
    __tablename__ = "all_stocks"

    id = Column(Integer, primary_key=True)
    code = Column(String(20), nullable=False)
    name = Column(String(255), nullable=False)
    typekind = Column(String(20), nullable=True)
    setup_date = Column(String(20), nullable=True)
    quit_date = Column(String(20), nullable=True)

class UserStocks(Base):
    __tablename__ = "user_stocks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    code = Column(String(10), nullable=False)
    cost_hold = Column(Float)
    portion_hold = Column(Float)
    aver_price = Column(Float)
    keep_eye = Column(SmallInteger, default=1)
    fee = Column(Float)
    amount = Column(Integer)
    uramount = Column(String(255))

class UserStrategy(Base):
    __tablename__ = "user_strategy"
    code = Column(String(10), primary_key=True)
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
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
