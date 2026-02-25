from sqlalchemy import Column, String, Integer, SmallInteger, Float, PrimaryKeyConstraint, Boolean
from app.db import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    nickname = Column(String(50), nullable=True)
    parent_id = Column(Integer, nullable=True)
    realcash = Column(SmallInteger, nullable=False, default=1)
    email = Column(String(320), unique=True, index=True, nullable=False)
    hashed_password = Column(String(1024), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    is_superuser = Column(Boolean, nullable=False, default=False)
    is_verified = Column(Boolean, nullable=False, default=False)


class UserStocks(Base):
    __tablename__ = "user_stocks"

    user_id = Column(Integer, nullable=False)
    code = Column(String(10), nullable=False)
    cost_hold = Column(Float)
    portion_hold = Column(Float)
    aver_price = Column(Float)
    keep_eye = Column(SmallInteger, default=1)
    amount = Column(Integer)
    uramount = Column(String(255))

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'code', name='pk_user_stocks'),
    )


class UserStrategy(Base):
    __tablename__ = "user_strategy"

    user_id = Column(Integer, nullable=False)
    code = Column(String(10))
    id = Column(Integer)
    skey = Column(String(64))
    trans = Column(SmallInteger)
    data = Column(String(255))

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'code', 'id', name='pk_user_strategy'),
    )


class UserCostdog(Base):
    __tablename__ = "user_costdog"

    user_id = Column(Integer, nullable=False)
    ckey = Column(String(20))
    data = Column(String(255))

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'ckey', name='pk_user_costdog'),
    )


class UcostdogUrque(Base):
    __tablename__ = "ucostdog_urque"

    user_id = Column(Integer, nullable=False)
    ckey = Column(String(20), nullable=False)
    id = Column(Integer, nullable=False)
    urdata = Column(String(255))

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'ckey', 'id', name='pk_ucostdog_urque'),
    )


class OrdersTable():
    user_id = Column(Integer, nullable=False)
    code = Column(String(10))
    time = Column(String(20))
    portion = Column(Integer)
    price = Column(Float)
    sid = Column(String(10))
    typebs = Column(String(10))


class UserOrders(Base, OrdersTable):
    __tablename__ = "user_orders"

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'code', 'time', 'sid', name='pk_user_orders'),
    )

class UserFullOrders(Base, OrdersTable):
    __tablename__ = "user_full_orders"

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'code', 'time', 'sid', name='pk_user_full_orders'),
    )


class UserEarned(Base):
    __tablename__ = "user_earned"

    user_id = Column(Integer, nullable=False)
    date = Column(String(20))
    earned = Column(Float)
    total_earned = Column(Float)

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'date', name='pk_user_earned'),
    )


class UserEarning(Base):
    __tablename__ = "user_earning"

    user_id = Column(Integer, nullable=False)
    date = Column(String(20))
    cost = Column(Float)
    amount = Column(Float)

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'date', name='pk_user_earning'),
    )


class BasicDeals:
    user_id = Column(Integer, nullable=False)
    time = Column(String(20))
    code = Column(String(10))
    typebs = Column(String(10))
    sid = Column(String(10))
    price = Column(Float)
    portion = Column(Integer)


class DealsTable(BasicDeals):
    fee = Column(Float)
    feeYh = Column(Float)
    feeGh = Column(Float)


class UserUnknownDeals(Base, DealsTable):
    __tablename__ = "user_deals_unknown"

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'time', 'code', 'typebs', 'sid', name='pk_user_deals_unknown'),
    )


class UserArchivedDeals(Base, DealsTable):
    __tablename__ = "user_deals_archived"

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'time', 'code', 'typebs', 'sid', name='pk_user_deals_archived'),
    )

class UserStockBuy(Base):
    __tablename__ = "user_stock_buy"

    user_id = Column(Integer, nullable=False)
    time = Column(String(20))
    code = Column(String(10))
    portion = Column(Integer)
    price = Column(Float)
    cost = Column(Float)
    soldout = Column(SmallInteger, default=0)
    soldptn = Column(Integer, default=0)
    sid = Column(String(10))
    fee = Column(Float)
    feeYh = Column(Float)
    feeGh = Column(Float)

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'time', 'code', 'sid', name='pk_user_stock_buy'),
    )

class UserStockSell(Base):
    __tablename__ = "user_stock_sell"

    user_id = Column(Integer, nullable=False)
    time = Column(String(20))
    code = Column(String(10))
    portion = Column(Integer)
    price = Column(Float)
    money_sold = Column(Float, default=0)
    cost_sold = Column(Float, default=0)
    earned = Column(Float, default=0)
    return_percent = Column(Float, default=0)
    sid = Column(String(10))
    fee = Column(Float)
    feeYh = Column(Float)
    feeGh = Column(Float)

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'time', 'code', 'sid', name='pk_user_stock_sell'),
    )


class UserTrackNames(Base):
    __tablename__ = "user_track_names"

    user_id = Column(Integer, nullable=False)
    tkey = Column(String(64))
    tname = Column(String(255))

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'tkey', name='pk_user_track_names'),
    )


class UserTrackDeals(Base, BasicDeals):
    __tablename__ = "user_track_deals"

    tkey = Column(String(64))

    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'tkey', 'time', 'code', 'sid', name='pk_user_track_deals'),
    )

