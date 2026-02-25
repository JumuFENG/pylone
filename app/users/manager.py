import jwt
import json
try:
    from fastapi_users import FastAPIUsers, BaseUserManager, exceptions
    from fastapi_users.authentication import (
        CookieTransport,
        BearerTransport,
        AuthenticationBackend,
        JWTStrategy
    )
    from fastapi_users.db import SQLAlchemyUserDatabase
    from fastapi_users.password import PasswordHelper
    CRYPTO_AVAILABLE = True
except Exception:
    CRYPTO_AVAILABLE = False

# Import other required modules
from typing import AsyncGenerator, Optional, Union
from sqlalchemy import select
from fastapi import Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.lofig import Config, logger
from app.db import (
    async_session_maker, query_one_value, query_one_record, query_values, query_aggregate, upsert_one, insert_many, upsert_many, delete_records,
    array_to_dict_list)
from app.stock.manager import AllStocks
from app.stock.date import TradingDate
from app.stock.quotes import Quotes as qot
from .models import (
    User, UserStocks, UserStrategy, UserOrders, UserFullOrders, UserUnknownDeals, UserArchivedDeals, UserStockBuy, UserStockSell,
    UserEarned, UserEarning, UserTrackNames, UserTrackDeals)


cfg = {'jwt_secret': 'JWT_SECRET_SHOULD_CHANGE_IN_PRODUCTION', 'jwt_lifetime_seconds': 2592000, 'cookie_secure': False}
cfg.update(Config.client_config())

# Fallback implementations when cryptography is not available
if not CRYPTO_AVAILABLE:
    from passlib.context import CryptContext
    import time

    # Use bcrypt via passlib for password hashing
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

    class FallbackPasswordHelper:
        """Fallback password helper using passlib/bcrypt"""

        @staticmethod
        def hash(password: str) -> str:
            return pwd_context.hash(password)

        @staticmethod
        def verify(plain_password: str, hashed_password: str) -> bool:
            return pwd_context.verify(plain_password, hashed_password)

        @staticmethod
        def verify_and_update(plain_password: str, hashed_password: str):
            """Verify password and return (verified, updated_hash)"""
            verified = pwd_context.verify(plain_password, hashed_password)
            updated_hash = None
            if verified and pwd_context.needs_update(hashed_password):
                updated_hash = pwd_context.hash(plain_password)
            return verified, updated_hash

        @staticmethod
        def generate() -> str:
            """Generate a random password"""
            import secrets
            return secrets.token_urlsafe(12)

    class FallbackJWTStrategy:
        """Fallback JWT strategy using PyJWT"""

        def __init__(self, secret: str, lifetime_seconds: int):
            self.secret = secret
            self.lifetime_seconds = lifetime_seconds

        async def write_token(self, user) -> str:
            """Generate JWT token for user"""
            payload = {
                'sub': str(user.id),
                'email': getattr(user, 'email', None),
                'username': getattr(user, 'username', None),
                'exp': int(time.time()) + self.lifetime_seconds
            }
            return jwt.encode(payload, self.secret, algorithm="HS256")

        async def read_token(self, token: str) -> Optional[dict]:
            """Read and validate JWT token"""
            try:
                return jwt.decode(token, self.secret, algorithms=["HS256"])
            except jwt.InvalidTokenError:
                return None

        async def destroy_token(self, token: str) -> None:
            """JWT tokens are stateless, nothing to destroy"""
            pass

# Cookie and Bearer transport
cookie_transport = CookieTransport(cookie_secure=cfg['cookie_secure'])
bearer_transport = BearerTransport(tokenUrl="auth/bearer/login")

# Define get_jwt_strategy based on cryptography availability
if CRYPTO_AVAILABLE:
    def get_jwt_strategy() -> JWTStrategy:
        return JWTStrategy(secret=cfg['jwt_secret'], lifetime_seconds=cfg['jwt_lifetime_seconds'])
else:
    def get_jwt_strategy() -> FallbackJWTStrategy:
        return FallbackJWTStrategy(secret=cfg['jwt_secret'], lifetime_seconds=cfg['jwt_lifetime_seconds'])

def _get_jwt_strategy():
    return get_jwt_strategy()

cookie_auth_backend = AuthenticationBackend(
    name="jwt-cookie",
    transport=cookie_transport,
    get_strategy=_get_jwt_strategy,
)

bearer_auth_backend = AuthenticationBackend(
    name="jwt-bearer",
    transport=bearer_transport,
    get_strategy=_get_jwt_strategy,
)

async def get_user_db() -> AsyncGenerator[SQLAlchemyUserDatabase, None]:
    async with async_session_maker() as session:
        yield SQLAlchemyUserDatabase(session, User)


class UserManager(BaseUserManager[User, int]):
    # Use fallback password helper if cryptography not available
    if not CRYPTO_AVAILABLE:
        password_helper = FallbackPasswordHelper()

    def parse_id(self, value):
        return int(value)

    async def authenticate(self, credentials):
        """
        自定义认证方法，支持使用 id, email 或 username 登录
        """

        try:
            if credentials.username.isdigit():
                user = await self.user_db.get(self.parse_id(credentials.username))
            else:
                user = await self.user_db.get_by_email(credentials.username)

            if not user:
                # 如果通过 email/id 没找到，尝试通过 username 查找
                async with async_session_maker() as session:
                    result = await session.execute(
                        select(User).where(User.username == credentials.username)
                    )
                    user = result.scalar_one_or_none()

            if not user:
                # 运行密码验证以防止时序攻击
                self.password_helper.hash(credentials.password)
                return None

            # 验证密码
            verified, updated_password_hash = self.password_helper.verify_and_update(
                credentials.password, user.hashed_password
            )
            if not verified:
                logger.error(f"Authentication error: {credentials.username} not verified!")
                return None

            # 如果密码哈希需要更新（例如算法升级）
            if updated_password_hash is not None:
                user.hashed_password = updated_password_hash
                await self.user_db.update(user)

            return user
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return None

    async def get_sub_account(self, user: User, acc: str=None, accid: int=None) -> Optional[User]:
        if not acc and not accid:
            return user

        if acc:
            async with async_session_maker() as session:
                result = await session.execute(select(User).where(User.username == acc, User.parent_id == user.id))
                subaccount = result.scalar_one_or_none()
                if '.' not in acc and not subaccount:
                    acc = f'{user.username}.{acc}'
                    result = await session.execute(select(User).where(User.username == acc, User.parent_id == user.id))
                    subaccount = result.scalar_one_or_none()
            return subaccount
        if accid:
            async with async_session_maker() as session:
                result = await session.execute(select(User).where(User.id == accid, User.parent_id == user.id))
                subaccount = result.scalar_one_or_none()
            return subaccount
        return user


async def get_user_manager():
    async for user_db in get_user_db():
        yield UserManager(user_db)

# Basic Auth 支持（auto_error=False 允许在没有 Basic Auth 时返回 None 而不是抛出 401）
async def get_current_user_basic(
    credentials: HTTPBasicCredentials = Depends(HTTPBasic(auto_error=False))
) -> User:
    """
    使用 Basic Auth 认证用户
    """
    if credentials is None:
        return None

    async for user_manager in get_user_manager():
        return await user_manager.authenticate(credentials)

fastapi_users = FastAPIUsers[User, int](
    get_user_manager,
    [cookie_auth_backend, bearer_auth_backend],
)

current_superuser = fastapi_users.current_user(superuser=True)

async def verify_user(parent, acc=None, accid=None):
    if not parent:
        raise exceptions.InvalidVerifyToken()
    if parent.id == accid or parent.username == acc:
        return parent
    async for user_manager in get_user_manager():
        subuser = await user_manager.get_sub_account(parent, acc, accid)
    return subuser


class UserStockManager():
    last_sid = None
    @classmethod
    async def kick_archived(cls, user: User, deal: dict):
        if deal['sid'] == '':
            return deal
        dsid = deal['sid']
        ad = await query_one_record(UserArchivedDeals, UserArchivedDeals.user_id == user.id, UserArchivedDeals.code == deal['code'], UserArchivedDeals.typebs == deal['tradeType'], UserArchivedDeals.sid == dsid)
        if ad is None:
            return deal

        if ad.time.partition(' ')[0] != deal['time'].partition(' ')[0]:
            return deal

        if ad.portion > int(deal['count']):
            raise Exception(f'Archived count > deal count, please check the database, table:{UserArchivedDeals.__tablename__}, {deal["code"]}, 委托编号={deal["sid"]}')
        deal['count'] = int(deal['count']) - ad.portion
        if 'fee' in deal:
            if ad.fee != float(deal['fee']) or ad.feeYh != float(deal['feeYh']) or ad.feeGh != float(deal['feeGh']) or ad.price != float(deal['price']):
                upfee = {'price': deal['price'], 'fee': deal['fee'], 'feeYh': deal['feeYh'], 'feeGh': deal['feeGh'], 'user_id': user.id, 'time': deal['time'], 'code': deal['code'], 'typebs': deal['tradeType'], 'sid': deal['sid']}
                await upsert_one(UserArchivedDeals, upfee, ['user_id', 'time', 'code', 'typebs', 'sid'])
        return deal if deal['count'] > 0 else None

    @classmethod
    def fake_sid(cls):
        if cls.last_sid is None:
            cls.last_sid = 0
        cls.last_sid += 1
        return f'fk{cls.last_sid}'

    @classmethod
    async def add_deals(cls, user: User, deals: Union[dict, str]):
        if isinstance(deals, str):
            deals = json.loads(deals)

        deals = [{**dl, 'code': dl['code'].lower(), 'sid': str(dl['sid'])} for dl in deals]

        dlarr = []
        udeals = []
        exists = {}
        for dl in deals:
            deal = await cls.kick_archived(user, dl)
            if deal is None:
                continue
            if deal['code'] not in exists:
                exists[deal['code']] = await AllStocks.is_exists(deal['code'])
            if not exists[deal['code']]:
                udeals.append(deal)
                continue
            if deal['tradeType'] == 'B' or deal['tradeType'] == 'S':
                if deal['sid'] == '':
                    deal['sid'] = cls.fake_sid()
                dlarr.append(deal)
            else:
                udeals.append(deal)

        if len(udeals) > 0:
            await cls.add_unknown_code_deal(user, udeals)
        if len(dlarr) > 0:
            await cls.add_buy_sells(user, dlarr)
            await cls.calc_earned(user)

    @classmethod
    async def add_buy_sells(cls, user, deals):
        bdeals = [{
            'user_id': user.id,
            'time': deal['time'],
            'sid': deal['sid'],
            'price': deal['price'],
            'portion': deal['count'],
            'fee': deal.get('fee', 0),
            'feeYh': deal.get('feeYh', 0),
            'feeGh': deal.get('feeGh', 0),
            'code': deal['code']
        } for deal in deals if deal['tradeType'] == 'B']

        sdeals = [{
            'user_id': user.id,
            'time': deal['time'],
            'sid': deal['sid'],
            'price': deal['price'],
            'portion': deal['count'],
            'fee': deal.get('fee', 0),
            'feeYh': deal.get('feeYh', 0),
            'feeGh': deal.get('feeGh', 0),
            'code': deal['code']
        } for deal in deals if deal['tradeType'] == 'S']

        if len(bdeals) > 0:
            await cls._add_or_update_deals(UserStockBuy, user, bdeals)

        if len(sdeals) > 0:
            await cls._add_or_update_deals(UserStockSell, user, sdeals)

        codes = {deal['code'] for deal in deals}
        for code in codes:
            await cls._fix_buy_sell_portion(user, code)

    @classmethod
    async def _add_or_update_deals(self, buy_table, user, deals):
        dicdeals = {}
        for deal in deals:
            if deal['code'] not in dicdeals:
                dicdeals[deal['code']] = []
            dicdeals[deal['code']].append(deal)

        for code, values in dicdeals.items():
            nvalues = []
            for val in values:
                odls = None
                if val['sid'].startswith('fk'):
                    # 委托编号为0可能是未确认的记录，可能有多条
                    dealtime = val['time'].partition(' ')[0]
                    odls = await query_values(buy_table, ['time', 'sid'], buy_table.user_id == user.id, buy_table.time == dealtime, buy_table.code == code)
                else:
                    # 委托编号如果重复需要删除重复只保留一条.
                    odls = await query_values(buy_table, ['time', 'sid'], buy_table.user_id == user.id, buy_table.sid == val['sid'], buy_table.code == code)

                val['time'] = val['time'].partition(' ')[0] if val['sid'].startswith('fk') else val['time']
                updated = False
                for odl in odls:
                    if odl[0].split()[0] == val['time'].split()[0]:
                        if val['time'] != odl[0] or val['sid'] != odl[1]:
                            await delete_records(buy_table, buy_table.user_id == user.id, buy_table.time == odl[0], buy_table.code == code, buy_table.sid == odl[1])
                        await upsert_one(buy_table, val, ['user_id', 'code', 'time', 'sid'])
                        updated = True
                        break
                if not updated:
                    nvalues.append(val)

            if len(nvalues) > 0:
                await insert_many(buy_table, nvalues)

    @classmethod
    async def _fix_buy_sell_portion(cls, user, code):
        buys = await query_values(UserStockBuy, ['sid', 'time', 'price', 'portion', 'soldptn', 'cost'], UserStockBuy.user_id == user.id, UserStockBuy.soldout == 0, UserStockBuy.code == code)
        sells = await query_values(UserStockSell, ['sid', 'time', 'price', 'portion', 'money_sold'], UserStockSell.user_id == user.id, UserStockSell.cost_sold == 0, UserStockSell.code == code)

        if len(buys) == 0 and len(sells) == 0:
            return

        buycount = 0
        sellcount = 0
        if buys is not None:
            for bid, bdate, bprice, bportion, bsoldportion, bcost in buys:
                buycount += (bportion - bsoldportion)
        if sells is not None:
            for sid, sdate, sprice, sportion, smoney in sells:
                sellcount += sportion
        if sellcount > buycount:
            await upsert_one(UserStocks, {'user_id': user.id, 'code': code, 'portion_hold': buycount - sellcount, 'keep_eye': 1}, ['user_id', 'code'])
            return

        portion = 0
        cost = 0
        remsell = None
        soldcost = 0
        for bid, bdate, bprice, bportion, bsoldportion, bcost in buys:
            if bcost is None:
                await upsert_one(UserStockBuy, {'cost': bprice * bportion, 'user_id': user.id, 'time': bdate, 'code': code, 'sid': bid}, ['user_id', 'time', 'code', 'sid'])

            rembportion = bportion - bsoldportion
            while rembportion > 0:
                if remsell is None or remsell[3] == 0:
                    if remsell is not None and soldcost > 0:
                        sinfo = {'cost_sold': soldcost, 'money_sold': remsell[4], 'earned': remsell[4] - soldcost, 'return_percent': (remsell[4] - soldcost) / remsell[4], 'user_id': user.id, 'time': remsell[1], 'code': code, 'sid': remsell[0]}
                        await upsert_one(UserStockSell, sinfo, ['user_id', 'time', 'code', 'sid'])
                    soldcost = 0
                    if sells is None or len(sells) == 0:
                        break
                    remsell = list(sells.pop(0))
                    (sid, sdate, sprice, sportion, smoney) = remsell
                    remsell[4] = sprice * sportion

                sid, sdate, sprice, sportion, smoney = remsell
                if sportion >= rembportion:
                    await upsert_one(UserStockBuy, {'soldptn': bportion, 'soldout': 1, 'user_id': user.id, 'time': bdate, 'code': code, 'sid': bid}, ['user_id', 'time', 'code', 'sid'])
                    if sportion == rembportion:
                        remsell[3] = 0
                    else:
                        remsell = list(remsell)
                        remsell[3] = sportion - rembportion
                    soldcost += (rembportion * bprice)
                    rembportion -= sportion
                    break
                else:
                    remsell[3] = 0
                    soldcost += (sportion * bprice)
                    rembportion -= sportion

            if rembportion > 0:
                if bportion > rembportion:
                    await upsert_one(UserStockBuy, {'soldptn': bportion - rembportion, 'user_id': user.id, 'time': bdate, 'code': code, 'sid': bid}, ['user_id', 'time', 'code', 'sid'])

                portion += rembportion
                cost += rembportion * bprice

        if remsell is not None and remsell[3] == 0 and soldcost > 0:
            sinfo = {'cost_sold': soldcost, 'money_sold': remsell[4], 'earned': remsell[4] - soldcost, 'return_percent': (remsell[4] - soldcost) / remsell[4], 'user_id': user.id, 'time': remsell[1], 'code': code, 'sid': remsell[0]}
            await upsert_one(UserStockSell, sinfo, ['user_id', 'time', 'code', 'sid'])
            soldcost = 0
        if remsell is not None and remsell[3] > 0:
            portion -= remsell[3]
        if sells is not None:
            for sr in sells:
                portion -= sr[3]
                cost -= sr[2] * sr[3]

        average = cost/portion if not portion == 0 else 0
        upinfo = {
            'user_id': user.id,
            'code': code,
            'cost_hold': cost,
            'portion_hold': portion,
            'aver_price': average
        }
        if portion != 0:
            upinfo['keep_eye'] = 1
        await upsert_one(UserStocks, upinfo, ['user_id', 'code'])

    @classmethod
    async def calc_earned(cls, user, date=None):
        '''
        从买卖成交记录计算历史收益详情
        '''
        codes = await query_values(UserStocks, UserStocks.code, UserStocks.user_id == user.id)
        earndic = {}
        for c, in codes:
            cearn = await cls.get_each_sell_earned(user, c) if date is None else await cls.get_sell_earned_after(user, c, date)
            if cearn is None:
                continue
            for k in cearn:
                if k in earndic:
                    earndic[k] += cearn[k]
                else:
                    earndic[k] = cearn[k]

        for k in sorted(earndic.keys()):
            await cls.set_earned(user, k, earndic[k])

    @classmethod
    async def get_each_sell_earned(cls, user, code):
        sell_rec = await query_values(UserStockSell, None, UserStockSell.user_id == user.id, UserStockSell.code == code)
        if len(sell_rec) == 0:
            return None

        sell_rec = array_to_dict_list(UserStockSell, sell_rec)
        sells = []
        for sr in list(sell_rec):
            fee = 0 if sr['fee'] is None else sr['fee']
            fee += 0 if sr['feeYh'] is None else sr['feeYh']
            fee += 0 if sr['feeGh'] is None else sr['feeGh']
            sells.append({'date': sr['time'].split()[0], 'price':sr['price'], 'ptn': sr['portion'], 'fee': fee})

        buy_rec = await query_values(UserStockBuy, None, UserStockBuy.user_id == user.id, UserStockBuy.code == code)
        buy_rec = array_to_dict_list(UserStockBuy, buy_rec)
        buys = []
        for br in buy_rec:
            fee = 0 if br['fee'] is None else br['fee']
            fee += 0 if br['feeYh'] is None else br['feeYh']
            fee += 0 if br['feeGh'] is None else br['feeGh']
            buys.append({'date': br['time'].split()[0], 'price':br['price'], 'ptn': br['portion'], 'fee': fee})
        return cls.sell_earned_by_day(buys, sells)

    @staticmethod
    def sell_earned_by_day(buys, sells):
        rembuy = None
        earndic = {}
        for s in sells:
            if s['date'] not in earndic:
                earndic[s['date']] = 0

            remsold = s['ptn']
            earned = s['ptn'] * s['price'] - s['fee']
            while remsold > 0:
                if rembuy is None or rembuy['ptn'] == 0:
                    if len(buys) == 0:
                        earned = 0
                        break
                    rembuy = buys.pop(0)
                if remsold >= rembuy['ptn']:
                    earned -= rembuy['fee']
                    earned -= rembuy['ptn'] * rembuy['price']
                    remsold -= rembuy['ptn']
                    rembuy = None
                else:
                    earned -= remsold * rembuy['price']
                    rembuy['ptn'] -= remsold
                    remsold = 0
                if remsold == 0:
                    earndic[s['date']] += earned
        return earndic

    @classmethod
    async def get_sell_earned_after(cls, user, code, date):
        date = TradingDate.next_trading_date(date)
        sell_rec = await query_values(UserStockSell, None, UserStockSell.user_id == user.id, UserStockSell.code == code)
        if sell_rec is None:
            return None

        sell_rec = array_to_dict_list(UserStockSell, sell_rec)
        sells = []
        for sr in sell_rec:
            if sr['time'] < date:
                continue
            fee = 0 if sr['fee'] is None else sr['fee']
            fee += 0 if sr['feeYh'] is None else sr['feeYh']
            fee += 0 if sr['feeGh'] is None else sr['feeGh']
            sells.append({'date': sr['time'].split()[0], 'price':sr['price'], 'ptn': sr['portion'], 'fee': fee})

        if len(sells) == 0:
            return None

        buy_rec = await query_values(UserStockBuy, None, UserStockBuy.user_id == user.id, UserStockBuy.code == code)
        buy_rec = array_to_dict_list(UserStockBuy, buy_rec)
        buys = []
        for br in buy_rec:
            fee = 0 if br['fee'] is None else br['fee']
            fee += 0 if br['feeYh'] is None else br['feeYh']
            fee += 0 if br['feeGh'] is None else br['feeGh']
            buys.append({'date': br['time'].split()[0], 'price':br['price'], 'ptn': br['portion'], 'fee': fee})
        return cls.sell_earned_by_day(buys, sells)

    @classmethod
    async def set_earned(cls, user, date, earned):
        totalEarned = 0
        maxdate = await query_aggregate('max', UserEarned, 'date', UserEarned.user_id == user.id)
        lastEarned = await query_one_record(UserEarned, UserEarned.user_id == user.id, UserEarned.date == maxdate)
        if lastEarned is None:
            await upsert_one(UserEarned, {'user_id': user.id, 'date': date, 'earned': earned, 'total_earned': earned}, ['user_id', 'date'])
            return
        dt, ed, totalEarned = lastEarned.date, lastEarned.earned, lastEarned.total_earned
        if dt > date:
            logger.warning('can not set earned for date earlier than %s date %s, user %s', dt, date, user.username)
            return
        if dt == date:
            logger.warning('earned already exists: %s %s %s', dt, ed, totalEarned)
            totalEarned += earned - ed
            logger.warning('update to: %s %s', earned, totalEarned)
            await upsert_one(UserEarned, {'user_id': user.id, 'date': date, 'earned': earned, 'total_earned': totalEarned}, ['user_id', 'date'])
            return

        totalEarned += earned
        await upsert_one(UserEarned, {'user_id': user.id, 'date': date, 'earned': earned, 'total_earned': totalEarned}, ['user_id', 'date'])

    @classmethod
    async def add_unknown_code_deal(cls, user, deals):
        '''
        无法识别的成交记录，新股新债...
        '''
        values = []
        commontype = ['利息归本', '银行转证券', '证券转银行', '红利入账', '扣税', '融资利息']
        for deal in deals:
            if 'fee' in deal:
                if deal['tradeType'] == '配售缴款':
                    unknowndeals = await query_values(UserUnknownDeals, None, UserUnknownDeals.user_id == user.id)
                    unknowndeals = array_to_dict_list(UserUnknownDeals, unknowndeals)
                    existdeals = []
                    for ud in unknowndeals:
                        if ud['time'] < deal['time']:
                            continue
                        if ud['code'][-6:] != deal['code'][-6:]:
                            continue
                        existdeals.append(ud)

                    await cls.remove_repeat_unknown_deals_配售缴款(user, existdeals, deal)
                    continue
                if deal['tradeType'] == '新股入帐':
                    existdeals = await query_values(UserUnknownDeals, None, UserUnknownDeals.user_id == user.id, UserUnknownDeals.sid == deal['sid'])
                    await cls.remove_repeat_unknown_deals_新股入帐(user, array_to_dict_list(UserUnknownDeals, existdeals), deal)
                    continue
                if deal['tradeType'] in commontype:
                    existdeals = await query_values(UserUnknownDeals, None, UserUnknownDeals.user_id == user.id, UserUnknownDeals.time == deal['time'])
                    await cls.remove_repeat_unknown_deals_commontype(user, array_to_dict_list(UserUnknownDeals, existdeals), deal)
                    continue
                if deal['tradeType'] == 'B' or deal['tradeType'] == 'S':
                    logger.info('unknown deal %s', deal)
                values.append({
                    'user_id': user.id,
                    'time': deal['time'],
                    'code': deal['code'],
                    'typebs': deal['tradeType'],
                    'sid': cls.fake_sid() if deal['sid'] == '' else deal['sid'],
                    'price': deal['price'],
                    'portion': deal['count'],
                    'fee': deal['fee'],
                    'feeYh': deal['feeYh'],
                    'feeGh': deal['feeGh']
                })

        if len(values) > 0:
            await insert_many(UserUnknownDeals, values)

    @classmethod
    async def remove_repeat_unknown_deals_配售缴款(cls, user, existdeals, deal):
        code = deal['code']
        fkdeals = []
        for edeal in existdeals:
            if edeal['time'] < deal['time']:
                continue
            if edeal['code'][-6:] != deal['code'][-6:]:
                continue
            if not edeal.sid.startswith('fk') and edeal['sid'] != deal['sid']:
                continue
            if len(edeal['code']) > len(deal['code']):
                code = edeal['code']

            fkdeals.append(edeal)

        updeal = {
                'user_id': user.id,
                'time': deal['time'],
                'code': code,
                'typebs': 'B',
                'sid': deal['sid'],
                'price': deal['price'],
                'portion': deal['count'],
                'fee': deal['fee'],
                'feeYh': deal['feeYh'],
                'feeGh': deal['feeGh']
            }
        if len(fkdeals) > 0:
            sids = {fkdeal['sid'] for fkdeal in fkdeals}
            for sid in sids:
                if updeal['sid'] == '':
                    updeal['sid'] = sid
                if updeal['sid'].startswith('fk') and not sid.startswith('fk'):
                    updeal['sid'] = sid
                await delete_records(UserUnknownDeals, UserUnknownDeals.user_id == user.id, UserUnknownDeals.sid == sid)
        if updeal['sid'] == '':
            updeal['sid'] = cls.fake_sid()
        await upsert_one(UserUnknownDeals, updeal, ['user_id', 'time', 'code', 'typebs', 'sid'])

    @classmethod
    async def remove_repeat_unknown_deals_新股入帐(cls, user, existdeals, deal):
        fkdeals = []
        code = deal['code']
        for edeal in existdeals:
            if edeal['time'] < deal['time']:
                continue
            if edeal['sid'] != deal['sid']:
                continue
            if len(edeal['code']) > len(code) and edeal['code'][-6:] == deal['code']:
                code = edeal['code']
            fkdeals.append(edeal)

        updeal = {
            'user_id': user.id,
            'time': deal['time'],
            'code': code,
            'typebs': 'B',
            'sid': deal['sid'],
            'price': deal['price'],
            'portion': deal['count'],
            'fee': deal['fee'],
            'feeYh': deal['feeYh'],
            'feeGh': deal['feeGh']
        }
        if len(fkdeals) > 0:
            sids = {fkdeal['sid'] for fkdeal in fkdeals}
            for sid in sids:
                if updeal['sid'] == '':
                    updeal['sid'] = sid
                if updeal['sid'].startswith('fk') and not sid.startswith('fk'):
                    updeal['sid'] = sid
                await delete_records(UserUnknownDeals, UserUnknownDeals.user_id == user.id, UserUnknownDeals.sid == sid)
        if updeal['sid'] == '':
            updeal['sid'] = cls.fake_sid()
        await upsert_one(UserUnknownDeals, updeal, ['user_id', 'time', 'code', 'typebs', 'sid'])

    @classmethod
    async def remove_repeat_unknown_deals_commontype(self, user, existdeals, deal):
        updeal = {
            'user_id': user.id,
            'time': deal['time'],
            'code': deal['code'],
            'typebs': deal['tradeType'],
            'sid': deal['sid'],
            'price': deal['price'],
            'portion': deal['count'],
            'fee': deal['fee'],
            'feeYh': deal['feeYh'],
            'feeGh': deal['feeGh']
        }
        if len(existdeals) > 0:
            sids = {fkdeal['sid'] for fkdeal in existdeals}
            for sid in sids:
                await delete_records(UserUnknownDeals, UserUnknownDeals.user_id == user.id, UserUnknownDeals.sid == sid)

        if updeal['sid'] == '':
            updeal['sid'] = self.fake_sid()
        await upsert_one(UserUnknownDeals, updeal, ['user_id', 'time', 'code', 'typebs', 'sid'])

    @classmethod
    async def fix_deals(cls, user: User, deals: Union[dict, str]):
        hdeals = deals if isinstance(deals, list) else json.loads(deals)
        cdeals = {}
        for d in hdeals:
            c = d['code']
            if c not in cdeals:
                cdeals[c] = []
            cdeals[c].append(d)

        for code, deals in cdeals.items():
            bdeals = [d for d in deals if d['tradeType'] == 'B']
            sdeals = [d for d in deals if d['tradeType'] == 'S']
            await cls.replace_buysell(UserStockBuy, user, code, bdeals)
            await cls.replace_buysell(UserStockSell, user, code, sdeals)
            await cls._fix_buy_sell_portion(user, code)

    @classmethod
    async def replace_buysell(self, dealtable, user, code, deals):
        for i, deal in enumerate(deals):
            if 'code' not in deal or deal['code'] != code:
                deal['code'] = code
            deals[i] = {k: v for k, v in deal.items() if hasattr(dealtable, k)}
            deals[i]['user_id'] = user.id

        await delete_records(dealtable, dealtable.user_id == user.id, dealtable.code == code)
        await insert_many(dealtable, deals)

    @classmethod
    async def replace_orders(cls, ordtable, user, code, orders):
        for i, order in enumerate(orders):
            if 'code' not in order or order['code'] != code:
                order['code'] = code
            orders[i] = {k: v for k, v in order.items() if hasattr(ordtable, k)}
            orders[i]['user_id'] = user.id
            orders[i]['typebs'] = order['tradeType'] if 'tradeType' in order else order['type']
            if 'time' not in orders[i]:
                orders[i]['time'] = order['date']
            if 'sid' not in orders[i]:
                orders[i]['sid'] = cls.fake_sid()
            if 'portion' not in orders[i]:
                orders[i]['portion'] = order['count']

        unique_orders = {}
        for order in orders:
            key = (order['user_id'], order['code'], order['time'], order['sid'])
            unique_orders[key] = order
        orders = list(unique_orders.values())

        await delete_records(ordtable, ordtable.user_id == user.id, ordtable.code == code)
        if orders:
            await insert_many(ordtable, orders)

    @classmethod
    async def remove_deals(cls, user: User, code: str, bsid: list[str], ssid: list[str]):
        if len(bsid) > 0:
            await delete_records(UserStockBuy, UserStockBuy.user_id == user.id, UserStockBuy.code == code, UserStockBuy.sid.in_(bsid))
        if len(ssid) > 0:
            await delete_records(UserStockSell, UserStockSell.user_id == user.id, UserStockSell.code == code, UserStockSell.sid.in_(ssid))

    @classmethod
    async def archive_deals(cls, user, edate):
        codes = await query_values(UserStocks, ['code'], UserStocks.user_id == user.id)
        consumed = ()
        for c, in codes:
            ucsmd = await cls.deals_before(user, c, edate)
            if len(ucsmd) > 0:
                consumed += ucsmd

        if len(consumed) > 0:
            await cls.add_to_archive_deals_table(user, consumed)

    @classmethod
    async def deals_before(cls, user: User, code: str, date: str):
        '''
        获取卖出日期早于date的所有卖出记录以及对应的买入记录
        '''
        sell_rec = await query_values(UserStockSell, None, UserStockSell.user_id == user.id, UserStockSell.code == code, UserStockSell.time < date)
        if len(sell_rec) == 0:
            return ()
        sell_rec = array_to_dict_list(UserStockSell, sell_rec)

        bexists = await query_values(UserStockBuy, None, UserStockBuy.user_id == user.id, UserStockBuy.code == code, UserStockBuy.time < date)
        if not bexists:
            buy_rec = await query_values(UserUnknownDeals, None, UserUnknownDeals.user_id == user.id, UserUnknownDeals.code == code, UserUnknownDeals.time < date)
            buy_rec = array_to_dict_list(UserUnknownDeals, buy_rec)
            if len(buy_rec) > 0:
                await upsert_many(UserStockBuy, buy_rec, ['user_id', 'code'])
                await delete_records(UserUnknownDeals, UserUnknownDeals.user_id == user.id, UserUnknownDeals.code == code, UserUnknownDeals.time < date)
                buy_rec = await query_values(UserStockBuy, None, UserStockBuy.user_id == user.id, UserStockBuy.code == code, UserStockBuy.time < date)
            buy_rec = array_to_dict_list(UserStockBuy, buy_rec)
        else:
            buy_rec = array_to_dict_list(UserStockBuy, bexists)
        consumed = ()
        rembuy = None
        bportion = 0
        delbuy = []
        delsell = []
        for srec in sell_rec:
            consumed += (code, srec['time'], 'S', srec['portion'], srec['price'], srec['fee'], srec['feeYh'], srec['feeGh'], srec['sid']),
            delsell.append((srec['time'], srec['sid']))
            sportion = srec['portion']
            while sportion > 0:
                if len(buy_rec) == 0:
                    logger.warning('no buy record for %s %s', code, srec)
                    return ()
                if bportion == 0:
                    if rembuy is not None:
                        consumed += (code, rembuy['time'], 'B', rembuy['portion'], rembuy['price'], rembuy['fee'], rembuy['feeYh'], rembuy['feeGh'], rembuy['sid']),
                        delbuy.append((rembuy['time'], rembuy['sid']))
                    rembuy = buy_rec.pop(0)
                    bportion = rembuy['portion']
                if bportion <= sportion:
                    sportion -= bportion
                    bportion = 0
                    consumed += (code, rembuy['time'], 'B', rembuy['portion'], rembuy['price'], rembuy['fee'], rembuy['feeYh'], rembuy['feeGh'], rembuy['sid']),
                    delbuy.append((rembuy['time'], rembuy['sid']))
                    rembuy = None
                else:
                    bportion -= sportion
                    sportion = 0

        for dt, sid in delsell:
            await delete_records(UserStockSell, UserStockSell.user_id == user.id, UserStockSell.code == code, UserStockSell.time == dt, UserStockSell.sid == sid)

        if rembuy is not None:
            if bportion > 0:
                consumed += (code, rembuy['time'], 'B', rembuy['portion'] - bportion, rembuy['price'], rembuy['fee'], rembuy['feeYh'], rembuy['feeGh'], rembuy['sid']),
                usold = rembuy['portion'] - rembuy['soldptn']
                rembuy['portion'] = bportion
                rembuy['soldptn'] = rembuy['portion'] - usold
                await upsert_one(UserStockBuy, {**rembuy, 'user_id': user.id, 'code': code}, ['user_id', 'code', 'time', 'sid'])
        for dt, sid in delbuy:
            await delete_records(UserStockBuy, UserStockBuy.user_id == user.id, UserStockBuy.code == code, UserStockBuy.time == dt, UserStockBuy.sid == sid)

        return consumed

    @classmethod
    async def add_to_archive_deals_table(cls, user, values):
        valdics = []  # 需要插入的记录
        archive_table = UserArchivedDeals if user.realcash == 1 else UserTrackDeals
        for code, date, typebs, portion, price, fee, feeYh, feeGh, sid in values:
            nval = {
                'user_id': user.id,'sid': sid,
                'code': code, 'time': date, 'typebs': typebs, 'portion': portion, 'price': price
            }
            if user.realcash == 1:
                nval['fee'] = fee
                nval['feeYh'] = feeYh
                nval['feeGh'] = feeGh
            else:
                nval['tkey'] = user.username.split('.')[1]
            existing_record = await query_one_record(archive_table, archive_table.user_id == user.id, archive_table.code == code, archive_table.time == date, archive_table.typebs == typebs, archive_table.sid == sid)
            if existing_record:
                nval['portion'] = portion + existing_record.portion
            valdics.append(nval)
        if user.realcash == 1:
            await upsert_many(archive_table, valdics, ['user_id', 'code', 'time', 'typebs', 'sid'])
        else:
            await upsert_many(archive_table, valdics, ['user_id', 'tkey', 'code', 'time', 'typebs', 'sid'])

    @classmethod
    async def get_deals(cls, user, code=None):
        def transform_records(records, trade_type):
            return [
                {
                    'tradeType': trade_type,
                    'count': record.get('portion', None),
                    **{k: v for k, v in record.items() if k not in ('portion', 'user_id')}
                }
                for record in records
            ]

        conds = (UserStockBuy.user_id == user.id,)
        if code is not None:
            conds += (UserStockBuy.code == code,)
        buy_records = await query_values(UserStockBuy, None, *conds)
        buy_records = array_to_dict_list(UserStockBuy, buy_records)

        conds = (UserStockSell.user_id == user.id,)
        if code is not None:
            conds += (UserStockSell.code == code,)
        sell_records = await query_values(UserStockSell, None, *conds)
        sell_records = array_to_dict_list(UserStockSell, sell_records)

        buy_deals = transform_records(buy_records, 'B')
        sell_deals = transform_records(sell_records, 'S')

        return buy_deals + sell_deals
        # return sorted(all_deals, key=lambda x: x['time'])

    @classmethod
    async def get_archived_deals(cls, user: User, realcash=0):
        deals = await query_values(UserArchivedDeals, None, UserArchivedDeals.user_id == user.id)
        deals = array_to_dict_list(UserArchivedDeals, deals)

        if realcash == 0 or user.parent_id is not None:
            return deals

        slvs = await query_values(User, None, User.parent_id == user.id)
        slvs = [u for u in slvs if u.realcash == 1]
        for u in slvs:
            deals += await cls.get_archived_deals(u)
        return deals

    @classmethod
    async def get_archived_code_since(cls, user: User, date, realcash=0, excludehold=False):
        codes = await query_values(UserArchivedDeals, ['code'], UserArchivedDeals.user_id == user.id, UserArchivedDeals.time > date, UserArchivedDeals.typebs == 'S')
        codes = {r for r, in codes}
        if excludehold:
            excode = await query_values(UserStocks, ['code'], UserStocks.user_id == user.id, UserStocks.portion_hold > 0)
            excode = {r for r, in excode}
            codes = codes - excode
        if realcash == 0 or user.parent_id is not None:
            return codes

        slvs = await query_values(User, None, User.parent_id == user.id)
        slvs = [u for u in slvs if u.realcash == 1]
        for u in slvs:
            codes |= await cls.get_archived_code_since(u, date, 0, excludehold)
        return codes

    @classmethod
    async def save_strategy(cls, user: User, code: str, strategy: Union[dict, str]):
        strdata = strategy if isinstance(strategy, dict) else json.loads(strategy)

        ustk = {'user_id': user.id, 'code': code}
        if 'amount' in strdata:
            ustk['amount'] = strdata['amount']
        if 'uramount' in strdata:
            ustk['uramount'] = json.dumps(strdata['uramount'])
        await upsert_one(UserStocks, ustk, ['user_id', 'code'])

        if 'buydetail' in strdata:
            await cls.replace_orders(UserOrders, user, code, strdata['buydetail'])
        else:
            await delete_records(UserOrders, UserOrders.user_id == user.id, UserOrders.code == code)
        if 'buydetail_full' in strdata:
            await cls.replace_orders(UserFullOrders, user, code, strdata['buydetail_full'])
        else:
            await delete_records(UserFullOrders, UserFullOrders.user_id == user.id, UserFullOrders.code == code)
        if 'strategies' not in strdata:
            return

        svalues = []
        exid = []
        for i, s in strdata['strategies'].items():
            vdic = {
                'user_id': user.id,
                'code': code,
                'id': i,
                'skey': s['key'],
                'data': json.dumps(s)
            }
            if 'transfers' in strdata and i in strdata['transfers']:
                vdic['trans'] = strdata['transfers'][i]['transfer']
            else:
                vdic['trans'] = -1
            exid.append(int(i))
            svalues.append(vdic)
        if len(svalues) == 0:
            return
        existing_strategies = await query_values(UserStrategy, None, UserStrategy.user_id == user.id, UserStrategy.code == code)
        existing_strategies = array_to_dict_list(UserStrategy, existing_strategies)
        rids = [s['id'] for s in existing_strategies if s['id'] not in exid]
        if len(rids):
            await delete_records(UserStrategy, UserStrategy.user_id == user.id, UserStrategy.code == code, UserStrategy.id.in_(rids))
        await upsert_many(UserStrategy, svalues, ['user_id', 'code', 'id'])

    @classmethod
    async def load_strategy(self, user, code):
        ustk = await query_one_record(UserStocks, UserStocks.user_id == user.id, UserStocks.code == code)
        strdata = {'grptype': 'GroupStandard', 'strategies': {}, 'transfers': {}, 'amount': 0}
        if ustk:
            strdata['amount'] = ustk.amount
            if ustk.uramount:
                strdata['uramount'] = json.loads(ustk.uramount)

        strlst = await query_values(UserStrategy, None, UserStrategy.user_id == user.id, UserStrategy.code == code)
        strlst = [s._mapping for s in strlst ]
        for sl in strlst:
            strdata['strategies'][sl['id']] = json.loads(sl['data'])
            strdata['transfers'][sl['id']] =  {"transfer": sl['trans']}

        def orders_to_detail(odr):
            return [{**{
                k: v for k,v in o.items() if k not in ('typebs', 'portion')
            }, **{'type': o['typebs'], 'tradeType': o['typebs'], 'date': o['time'], 'count': o['portion']}} for o in odr]

        oex = await query_values(UserOrders, None, UserOrders.user_id == user.id, UserOrders.code == code)
        if oex:
            strdata['buydetail'] = orders_to_detail(array_to_dict_list(UserOrders, oex))
        foex = await query_values(UserFullOrders, None, UserFullOrders.user_id == user.id, UserFullOrders.code == code)
        if foex:
            strdata['buydetail_full'] = orders_to_detail(array_to_dict_list(UserFullOrders, foex))
        return strdata

    @classmethod
    async def remove_strategy(cls, user, code):
        count = await query_aggregate('count', UserStocks, 'code', UserStocks.code == code, UserStocks.user_id == user.id)
        if count > 0:
            await upsert_one(UserStocks, {'user_id': user.id, 'code': code, 'amount': 0, 'uramount': ''}, ['user_id', 'code'])

        await delete_records(UserStrategy, UserStrategy.user_id == user.id, UserStrategy.code == code)
        await delete_records(UserOrders, UserOrders.user_id == user.id, UserOrders.code == code)
        await delete_records(UserFullOrders, UserFullOrders.user_id == user.id, UserFullOrders.code == code)

    @classmethod
    async def watchings_with_strategy(cls, user: User):
        if user.realcash == 1:
            slst = await query_values(UserStocks, ['code', 'aver_price', 'portion_hold'], UserStocks.user_id == user.id, UserStocks.keep_eye == 1)
        else:
            slst = await query_values(UserStocks, ['code', 'aver_price', 'portion_hold'], UserStocks.user_id == user.id, UserStocks.keep_eye == 1, UserStocks.portion_hold > 0)
        return {s[0]: {'holdCost':s[1], 'holdCount': s[2], 'strategies': await cls.load_strategy(user, s[0])} for s in slst}

    @classmethod
    async def watching_stocks(cls, user: User):
        slst = await query_values(UserStocks, ['code', 'aver_price', 'portion_hold'], UserStocks.user_id == user.id, UserStocks.keep_eye == 1)
        return [s[0] for s in slst]

    @classmethod
    async def watch_stock(cls, user: User, code: str):
        await upsert_one(UserStocks, {'user_id': user.id, 'code': code, 'amount': 0, 'uramount': '', 'keep_eye': 1}, ['user_id', 'code'])

    @classmethod
    async def forget_stock(cls, user: User, code: str):
        await upsert_one(UserStocks, {'user_id': user.id, 'code': code, 'amount': 0, 'uramount': '', 'keep_eye': 0}, ['user_id', 'code'])
        await cls.remove_strategy(user, code)

    @classmethod
    async def forget_stocks(cls, user):
        p0rec = await query_values(UserStocks, ['code'], UserStocks.user_id == user.id, UserStocks.portion_hold == 0, UserStocks.keep_eye == 1)
        if len(p0rec) == 0:
            return
        p0rec = [{'user_id': user.id, 'code': r, 'keep_eye': 0} for r, in p0rec]
        await upsert_many(UserStocks, p0rec, ['user_id', 'code'])

    @classmethod
    async def update_earning(self, user: User):
        stocks = await query_values(UserStocks, ['code', 'cost_hold', 'portion_hold'], UserStocks.user_id == user.id)
        uss = {}
        for c, cost_hold, portion_hold in stocks:
            if cost_hold != 0 or portion_hold != 0:
                uss[c] = {'cost': cost_hold, 'ptn': portion_hold}

        quotes = qot.get_quotes(list(uss.keys()))
        if len(quotes) < len(uss):
            logger.warning(f'update_earning get latest prices error: fetch {len(uss)}, actual {len(quotes)}')
            for c, qt in quotes.items():
                uss[c]['price'] = qt['price']

        cost = 0
        value = 0
        for v in uss.values():
            cost += v['cost']
            value += v['ptn'] * float(v['price'])

        date = TradingDate.max_traded_date()
        await upsert_one(UserEarning, {'user_id': user.id, 'date': date, 'cost': cost, 'amount': value}, ['user_id', 'date'])

    @classmethod
    def save_costdog(cls, user: User, costdog: Union[dict, str]):
        pass

    @classmethod
    def remove_user_stock_with_deals(cls, user: User, watch: Union[dict, str]):
        pass

    @classmethod
    async def get_dealcategory(cls, user: User):
        return await query_values(UserTrackNames, ['tkey', 'tname'], UserTrackNames.user_id == user.id)

    @classmethod
    async def add_track_deals(cls, user: User, tkey: str, deals: list[dict], desc: str = None):
        tname = await query_one_value(UserTrackNames, UserTrackNames.tname, UserTrackNames.tkey == tkey, UserTrackNames.user_id == user.id)
        if tname is None or (desc is not None and tname != desc):
            await upsert_one(UserTrackNames, {'user_id': user.id, 'tkey': tkey, 'tname': desc}, ['user_id', 'tkey'])

        values = []
        for deal in deals:
            values.append({
                'user_id': user.id, 'tkey': tkey,
                **{k:v for k,v in deal.items() if k in ('time', 'code', 'typebs', 'sid', 'price', 'portion')}
            })
        if len(values) > 0:
            await upsert_many(UserTrackDeals, values, ['user_id', 'tkey', 'time', 'code', 'typebs', 'sid'])

    @classmethod
    async def get_track_deals(cls, user: User, tkey: str):
        deals = await query_values(UserTrackDeals, ['time', 'code', 'typebs', 'sid', 'price', 'portion'], UserTrackDeals.user_id == user.id, UserTrackDeals.tkey == tkey)
        track = {'tname': tkey}
        ds = []
        for d,c,tp,sid,pr,ptn in deals:
            fee = 0
            if user.username.endswith(('.normal', '.collat')):
                fYhGh = await query_one_record(UserArchivedDeals, UserArchivedDeals.user_id == user.id, UserArchivedDeals.code == c, UserArchivedDeals.time == d, UserArchivedDeals.typebs == tp, UserArchivedDeals.sid == sid)
                if fYhGh is not None:
                    fee = round(fYhGh.fee + fYhGh.feeYh + fYhGh.feeGh, 3)
            ds.append({'code': c, 'time': d, 'typebs': tp, 'sid': sid, 'price': pr, 'portion': ptn, 'fee': fee})
        track['deals'] = ds
        return track
