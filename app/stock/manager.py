import sys
from typing import List, Tuple
from datetime import datetime
from sqlalchemy import select, func, update
from sqlalchemy.exc import NoResultFound
from sqlalchemy.future import select
import stockrt as srt
from stockrt.sources.eastmoney import Em
from app.hu import classproperty
from app.lofig import logger
from app.db import async_session_maker
from .models import MdlAllStock, MdlAllIndice
from .history import Khistory as khis
from .date import TradingDate


class AllIndexes:
    @classproperty
    def db(cls):
        return MdlAllIndice

    @classmethod
    async def read_all(cls):
        async with async_session_maker() as session:
            result = await session.execute(select(cls.db))
            rows = result.scalars().all()
            # 转为字典列表
            return [row.__dict__ for row in rows]

    @classmethod
    async def load_info(cls, code):
        qt = srt.quotes(code).get(code, {})
        async with async_session_maker() as session:
            result = await session.execute(
                select(cls.db).where(cls.db.code == code)
            )
            obj = result.scalar_one_or_none()
            if obj:
                obj.name = qt["name"]
            else:
                obj = MdlAllIndice(code=code, name=qt["name"])
                session.add(obj)
            await session.commit()

    @classmethod
    async def index_name(cls, code):
        async with async_session_maker() as session:
            result = await session.execute(
                select(cls.db.name).where(cls.db.code == code)
            )
            row = result.scalar()
            return row

    @classmethod
    async def bulk_upsert_stocks(cls, stocks_data: List[dict]) -> Tuple[int, int]:
        """
        批量插入或更新股票数据
        返回: (新增数量, 更新数量)
        """
        if not stocks_data:
            return 0, 0
            
        async with async_session_maker() as session:
            try:
                # 提取所有股票代码
                codes = [stock['code'] for stock in stocks_data]
                
                # 批量查询已存在的记录
                stmt = select(cls.db).where(cls.db.code.in_(codes))
                result = await session.execute(stmt)
                existing_map = {stock.code: stock for stock in result.scalars()}
                
                # 分离新增和更新记录
                to_add = []
                update_count = 0
                
                for stock_data in stocks_data:
                    code = stock_data['code']
                    if code in existing_map:
                        # 更新现有记录
                        obj = existing_map[code]
                        for key, value in stock_data.items():
                            if hasattr(obj, key):
                                setattr(obj, key, value)
                        update_count += 1
                    else:
                        # 新增记录
                        to_add.append(cls.db(**stock_data))
                
                # 批量插入新记录
                if to_add:
                    session.add_all(to_add)
                
                await session.commit()
                return len(to_add), update_count
                
            except Exception as e:
                await session.rollback()
                raise e

    async def insert_or_update(cls, stocks_data: List[dict], chunk_size: int = 1000) -> Tuple[int, int]:
        """分块批量处理，避免内存溢出"""
        added, updated = 0, 0
        
        for i in range(0, len(stocks_data), chunk_size):
            chunk = stocks_data[i:i + chunk_size]
            result = await cls.bulk_upsert_stocks(chunk)
            added += result[0]
            updated += result[1]
        
        return added, updated

    @classmethod
    async def update_kline_data(cls, kltype='d'):
        '''
        更新表中所有指数的K线数据
        Args:
            kltype: K线类型 (d, w, m) 指数只保存这几种K线数据
        '''
        async with async_session_maker() as session:
            result = await session.execute(select(cls.db))
            rows = result.scalars().all()
            indice_code = [row.code for row in rows]
        if not indice_code:
            return

        cls.update_klines_by_code(indice_code, kltype)

    @classmethod
    def update_klines_by_code(cls, stocks, kltype: str='d'):
        """
        Updates the K-line data for a list of stock codes based on the specified K-line type.

        Args:
            stocks: A list of stock codes for which the K-line data needs to be updated.
            kltype: A string representing the K-line type (e.g., 'd' for daily).

        Returns:
            A list of stock codes for which the K-line data was not updated.

        """
        uplens = {c: khis.count_bars_to_updated(c, kltype) for c in stocks}
        fixlens = {}
        for c,l in uplens.items():
            if l == 0:
                continue
            if l not in fixlens:
                fixlens[l] = []
            fixlens[l].append(c)
        if not fixlens:
            return
        if 1 in fixlens and len(fixlens[1]) > 100:
            logger.warning('too many stocks to update for 1 day, please call update_kline_data("d") first!')
            return

        ofmt = srt.set_array_format('np')
        # srt.set_default_sources('dklines', 'dklines', ('xueqiu', 'ths', 'eastmoney', 'tdx', 'sina'), True)
        srt.set_default_sources('dklines', 'dklines', ('xueqiu', 'ths', 'tdx', 'sina'), True)
        klines = {}
        for l, codes in fixlens.items():
            if l == sys.maxsize:
                klines.update(srt.fklines(codes, kltype, 0))
            else:
                klines.update(srt.klines(codes, kltype, l+2, 0))
        for c in klines:
            if c in stocks:
                khis.save_kline(c, kltype, klines[c])
        srt.set_array_format(ofmt)
        return [c for c in sum(fixlens.values(), []) if c not in klines or len(klines[c]) == 0 or TradingDate.calc_trading_days(klines[c][-1]['time'], TradingDate.max_trading_date()) > 20]

class AllStocks(AllIndexes):
    @classproperty
    def db(cls):
        return MdlAllStock

    @classmethod
    async def is_quited(cls, code):
        code = srt.get_fullcode(code)
        async with async_session_maker() as session:
            result = await session.execute(
                select(cls.db.quit_date).where(cls.db.code == code)
            )
            row = result.scalar()
            return row

    @classmethod
    async def load_new_stocks(cls, sdate=None):
        async with async_session_maker() as session:
            if sdate is None:
                result = await session.execute(
                    select(func.max(cls.db.setup_date))
                )
                sdate = result.scalar()
            if not sdate:
                sdate = "20000101"
            sdate = int(sdate.replace('-', ''))
            clist = Em.qt_clist(
                fs='m:0+f:8,m:1+f:8,m:0+f:81+s:262144', fields='f12,f13,f14,f21,f26', fid='f26',
                qtcb=lambda data: min([int(item['f26']) for item in data]) < sdate
            )

            newstocks = []
            for nsobj in clist:
                c = nsobj['f12']
                n = nsobj['f14']
                ipodays = (datetime.now() - datetime.strptime(str(nsobj['f26']), "%Y%m%d")).days
                if ipodays > 10:
                    continue
                code = srt.get_fullcode(c)
                tp = 'BJStock' if code.startswith('BJ') else 'ABStock'
                d = str(nsobj['f26'])
                newstocks.append({
                    'code': code, 'name': n, 'typekind': tp, 'setup_date': d[0:4] + '-' + d[4:6] + '-' + d[6:]
                })

            if newstocks:
                await cls.insert_or_update(newstocks)

    @classmethod
    async def load_a_stocks(cls):
        def get_stocks_of(fs, tp, pre):
            clist = Em.qt_clist(
                fs, fields='f12,f13,f14,f21,f26', fid='f26'
            )
            stocks = []
            for s in clist:
                d = str(s['f26'])
                stocks.append({
                    'code': pre + s['f12'], 'name': s['f14'], 'typekind': tp, 'setup_date': d[0:4] + '-' + d[4:6] + '-' + d[6:]
                })
            return stocks

        astocks = []
        astocks.extend(get_stocks_of('m:1+t:2+f:!2,m:1+t:23+f:!2', 'ABStock', 'SH'))
        astocks.extend(get_stocks_of('m:1+t:2+f:2,m:1+t:23+f:2', 'TSStock', 'SH'))
        astocks.extend(get_stocks_of('m:0+t:6+f:!2,m:0+t:80+f:!2', 'ABStock', 'SZ'))
        astocks.extend(get_stocks_of('m:0+t:6+f:2,m:0+t:80+f:2', 'TSStock', 'SZ'))
        astocks.extend(get_stocks_of('m:0+t:81+s:262144+f:!2', 'BJStock', 'BJ'))
        astocks.extend(get_stocks_of('m:0+t:81+s:262144+f:2', 'TSStock', 'BJ'))

        await cls.insert_or_update(astocks)

    @classmethod
    async def get_bkstocks(self, bks):
        if isinstance(bks, str):
            bks = [bks]
        bks = ','.join(['b:' + bk for bk in bks])
        clist = Em.qt_clist(fs=bks, fields='f12,f13,f14,f26')
        return clist
    
    @classmethod
    async def load_all_funds(cls):
        def get_stocks(bks, tp):
            stocks = cls.get_bkstocks(bks)
            bkstks = []
            for s in stocks:
                d = str(s['f26'])
                bkstks.append({
                    'code': srt.get_fullcode(s['f12']), 'name': s['f14'], 'typekind': tp, 'setup_date': d[0:4] + '-' + d[4:6] + '-' + d[6:]
                })
            return bkstks
        funds = []
        funds.extend(get_stocks(['MK0021', 'MK0022', 'MK0023', 'MK0024'], 'ETF'))
        funds.extend(get_stocks(['MK0404', 'MK0405', 'MK0406', 'MK0407'], 'LOF'))
        await cls.insert_or_update(funds)
