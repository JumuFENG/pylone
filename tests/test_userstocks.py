#!/usr/bin/env python3
import unittest
from unittest.mock import Mock, ANY, patch, AsyncMock
import numpy as np
import json
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import query_one_record, query_aggregate, delete_records
from app.users.models import User, UserStocks, UserStockBuy, UserStockSell, UserArchivedDeals, UserEarned, UserStrategy, UserOrders, UserFullOrders
from app.users.manager import UserStockManager as usm


class TestUserStock(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.testuser = await query_one_record(User, User.email == 'test@test.com')

    async def __check_table_row(self, table, conds, checks):
        assert isinstance(checks, dict), 'checks should be a dict'
        conditons = [getattr(table, key) == value for key, value in conds.items()]
        query = await query_one_record(table, *conditons)

        self.assertIsNotNone(query, f'None value got, expect 1 row!')

        for k,v in checks.items():
            self.assertEqual(v, getattr(query, k), f'expected value: {v}, but get: {getattr(query, k)}')

    async def __clear_table(self, table):
        await delete_records(table)

    async def __cleanup_tables(self, tables):
        if not isinstance(tables, list):
            tables = [tables]
        for t in tables:
            await self.__clear_table(t)

    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_add_buy_deal(self):
        #  2022-06-08
        await self.__cleanup_tables([UserStockBuy, UserStockSell])

        await usm.add_deals(self.testuser, [{"time":"2022-06-08","sid":"s_001","code":"SH600497","tradeType":"B","price":"5.910000","count":"1200"}])

        await self.__check_table_row(UserStocks, {'code': "SH600497"}, {'cost_hold':7092.0, 'portion_hold':1200, 'aver_price':5.91})

    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_add_buy_sell_deals(self):
        await self.__cleanup_tables([UserStocks, UserStockBuy, UserStockSell])

        await usm.add_deals(self.testuser, [{"time":"2022-06-08","sid":"s_001","code":"SH600497","tradeType":"B","price":"5.910000","count":"1200"}])
        await usm.add_deals(self.testuser, [{"time":"2022-06-09","sid":"s_002","code":"SH600497","tradeType":"S","price":"5.990000","count":"1200"}])

        await self.__check_table_row(UserStocks, {'code': "SH600497"}, {'cost_hold':0.0, 'portion_hold':0, 'aver_price':0})

    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_add_buy_buy_deals(self):
        await self.__cleanup_tables([UserStocks, UserStockBuy, UserStockSell])

        await usm.add_deals(self.testuser, [{"time":"2022-06-08","sid":"s_001","code":"SH600497","tradeType":"B","price":"5.910000","count":"1200"}])
        await usm.add_deals(self.testuser, [{"time":"2022-06-09","sid":"s_002","code":"SH600497","tradeType":"B","price":"5.390000","count":"1300"}])
        await usm.add_deals(self.testuser, [
            {"time":"2025-03-14 09:25:00","sid":"71947","code":"SZ002693","tradeType":"B","price":"10.9900","count":"400","fee":"5.00","feeYh":"0.00","feeGh":"0.00"},
            {"time":"2025-03-14 09:25:00","sid":"71953","code":"SZ000701","tradeType":"B","price":"5.9400","count":"1100","fee":"5.00","feeYh":"0.00","feeGh":"0.00"},])
        await self.__check_table_row(UserStocks, {'code': "SH600497"}, {'cost_hold':14099.0, 'portion_hold':2500, 'aver_price':5.6396})
        await self.__check_table_row(UserStocks, {'code': "SZ002693"}, {'cost_hold':4396.0, 'portion_hold':400, 'aver_price':10.9900})


    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_add_buy_buy_sell_deals(self):
        await self.__cleanup_tables([UserStocks, UserStockBuy, UserStockSell])

        await usm.add_deals(self.testuser, [{"time":"2022-06-08","sid":"s_001","code":"SH600497","tradeType":"B","price":"5.910000","count":"1200"}])
        await usm.add_deals(self.testuser, [{"time":"2022-06-09","sid":"s_002","code":"SH600497","tradeType":"B","price":"5.390000","count":"1300"}])
        await usm.add_deals(self.testuser, [{"time":"2022-06-10","sid":"s_003","code":"SH600497","tradeType":"S","price":"5.090000","count":"2500"}])

        await self.__check_table_row(UserStocks, {'code': "SH600497"}, {'cost_hold':0.0, 'portion_hold':0, 'aver_price':0})

    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_buy_buy_sell_partial(self):
        await self.__cleanup_tables([UserStocks, UserStockBuy, UserStockSell])

        await usm.add_deals(self.testuser, [{"time":"2022-02-18","sid":"s_001","code":"SZ002045","tradeType":"B","price":"11.2700","count":"800"}])
        await usm.add_deals(self.testuser, [{"time":"2022-02-28","sid":"s_002","code":"SZ002045","tradeType":"B","price":"10.1600","count":"900"}])
        await usm.add_deals(self.testuser, [{"time":"2022-02-28","sid":"s_003","code":"SZ002045","tradeType":"B","price":"10.1700","count":"900"}])
        await usm.add_deals(self.testuser, [{"time":"2022-03-01","sid":"s_004","code":"SZ002045","tradeType":"S","price":"10.3000","count":"900"}])
        await self.__check_table_row(UserStockBuy, {'sid': "s_002"}, {'portion':900, 'soldout':0, 'soldptn':100})

        await usm.add_deals(self.testuser, [{"time":"2022-03-15","sid":"s_005","code":"SZ002045","tradeType":"B","price":"9.1090","count":"1000"}])
        await usm.add_deals(self.testuser, [{"time":"2022-04-21","sid":"s_006","code":"SZ002045","tradeType":"B","price":"7.8300","count":"1200"}])
        await usm.add_deals(self.testuser, [{"time":"2022-06-02","sid":"s_007","code":"SZ002045","tradeType":"S","price":"8.9350","count":"1200"}])
        await self.__check_table_row(UserStockBuy, {'sid': "s_002"}, {'portion':900, 'soldout':1, 'soldptn':900})
        await self.__check_table_row(UserStockBuy, {'sid': "s_003"}, {'portion':900, 'soldout':0, 'soldptn':400})

        await self.__check_table_row(UserStocks, {'code': "SZ002045"}, {'cost_hold':23590.0, 'portion_hold':2700, 'aver_price':8.73704})


    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_archive_deals_1(self):
        await self.__cleanup_tables([UserStocks, UserEarned, UserArchivedDeals, UserStockBuy, UserStockSell])

        await usm.add_deals(self.testuser, [{"code":"SH588300", "time":"2021-07-05", "tradeType":"B", "count":900, "price":0.9790, "fee":0.22, "feeYh":0.00, "feeGh":0.00, "sid":"192270" },
            {"code":"SH588300", "time":"2021-07-06", "tradeType":"B", "count":3100, "price":0.9530, "fee":0.74, "feeYh":0.00, "feeGh":0.00, "sid":"243715" },
            {"code":"SH588300", "time":"2021-07-16", "tradeType":"B", "count":3000, "price":0.9820, "fee":0.35, "feeYh":0.00, "feeGh":0.00, "sid":"1410475" },
            {"code":"SH588300", "time":"2021-07-19", "tradeType":"B", "count":2000, "price":0.9740, "fee":0.23, "feeYh":0.00, "feeGh":0.00, "sid":"1154776" },
            {"code":"SH588300", "time":"2021-07-26", "tradeType":"B", "count":3000, "price":0.9540, "fee":0.34, "feeYh":0.00, "feeGh":0.00, "sid":"664918" },
            {"code":"SH588300", "time":"2021-07-28", "tradeType":"B", "count":900, "price":0.9000, "fee":0.10, "feeYh":0.00, "feeGh":0.00, "sid":"353493" },
            {"code":"SH588300", "time":"2021-07-28", "tradeType":"B", "count":2100, "price":0.9300, "fee":0.23, "feeYh":0.00, "feeGh":0.00, "sid":"821612" }])
        await usm.add_deals(self.testuser, [{"code":"SH588300", "time":"2021-07-30", "tradeType":"S", "count":5000, "price":0.9710, "sid":"77329" }])
        await self.__check_table_row(UserStockSell, {'sid': "77329"}, {'portion':5000, 'money_sold':4855, 'fee':0})

        await usm.add_deals(self.testuser, [{"code":"SH588300", "time":"2021-07-30", "tradeType":"S", "count":5000, "price":0.9710, "fee":0.58, "feeYh":0.00, "feeGh":0.00, "sid":"77329" }])
        await self.__check_table_row(UserStockSell, {'sid': "77329"}, {'portion':5000, 'money_sold':4855, 'fee':0.58})

        await usm.archive_deals(self.testuser, '2021-08')
        await self.__check_table_row(UserArchivedDeals, {'sid': '77329'}, {'portion':5000, 'fee':0.58})
        await self.__check_table_row(UserArchivedDeals, {'sid': '1410475'}, {'portion':1000})
        ssum = await query_aggregate('sum', UserArchivedDeals, UserArchivedDeals.portion, UserArchivedDeals.user_id == self.testuser.id, UserArchivedDeals.typebs == 'S')
        bsum = await query_aggregate('sum', UserArchivedDeals, UserArchivedDeals.portion, UserArchivedDeals.user_id == self.testuser.id, UserArchivedDeals.typebs == 'B')
        self.assertEqual(ssum, bsum, 'buy portion NOT Equals to sell portion!')


    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_archive_deals_2(self):
        await self.__cleanup_tables([UserStocks, UserEarned, UserArchivedDeals, UserStockBuy, UserStockSell])

        await usm.add_deals(self.testuser, [{"code":"SH588300", "time":"2021-07-05", "tradeType":"B", "count":900, "price":0.9790, "fee":0.22, "feeYh":0.00, "feeGh":0.00, "sid":"192270" },
            {"code":"SH588300", "time":"2021-07-06", "tradeType":"B", "count":3100, "price":0.9530, "fee":0.74, "feeYh":0.00, "feeGh":0.00, "sid":"243715" },
            {"code":"SH588300", "time":"2021-07-16", "tradeType":"B", "count":3000, "price":0.9820, "fee":0.35, "feeYh":0.00, "feeGh":0.00, "sid":"1410475" },
            {"code":"SH588300", "time":"2021-07-19", "tradeType":"B", "count":2000, "price":0.9740, "fee":0.23, "feeYh":0.00, "feeGh":0.00, "sid":"1154776" },
            {"code":"SH588300", "time":"2021-07-26", "tradeType":"B", "count":3000, "price":0.9540, "fee":0.34, "feeYh":0.00, "feeGh":0.00, "sid":"664918" },
            {"code":"SH588300", "time":"2021-07-28", "tradeType":"B", "count":900, "price":0.9000, "fee":0.10, "feeYh":0.00, "feeGh":0.00, "sid":"353493" },
            {"code":"SH588300", "time":"2021-07-28", "tradeType":"B", "count":2100, "price":0.9300, "fee":0.23, "feeYh":0.00, "feeGh":0.00, "sid":"821612" },
            {"code":"SH588300", "time":"2021-07-30", "tradeType":"S", "count":5000, "price":0.9710, "fee":0.58, "feeYh":0.00, "feeGh":0.00, "sid":"77329" },
            {"code":"SH588300", "time":"2021-08-13", "tradeType":"B", "count":5300, "price":0.9320, "fee":0.59, "feeYh":0.00, "feeGh":0.00, "sid":"1428159" },
            {"code":"SH588300", "time":"2021-09-01", "tradeType":"B", "count":5800, "price":0.8580, "fee":0.60, "feeYh":0.00, "feeGh":0.00, "sid":"280527" },
            {"code":"SH588300", "time":"2021-09-17", "tradeType":"B", "count":5800, "price":0.8510, "fee":0.59, "feeYh":0.00, "feeGh":0.00, "sid":"966285" },
            {"code":"SH588300", "time":"2021-09-24", "tradeType":"B", "count":5800, "price":0.8580, "fee":0.60, "feeYh":0.00, "feeGh":0.00, "sid":"87198" },
            {"code":"SH588300", "time":"2021-09-24", "tradeType":"B", "count":5800, "price":0.8580, "fee":0.60, "feeYh":0.00, "feeGh":0.00, "sid":"87292" },
            {"code":"SH588300","time":"2021-09-24","count":5800,"price":0.8580,"tradeType":"B","sid":"87305","fee":0.60,"feeYh":0.00,"feeGh":0.00}])
        await usm.add_deals(self.testuser, [{"code":"SH588300", "time":"2021-09-24", "tradeType":"S", "count":26900, "price":0.8600, "sid":"359963"}])
        await self.__check_table_row(UserStockSell, {'sid':"359963"}, {'portion':26900, 'money_sold':23134, 'fee':0})

        await usm.add_deals(self.testuser, [{"code":"SH588300", "time":"2021-09-24", "tradeType":"S", "count":26900, "price":0.8600, "fee":2.78, "feeYh":0.00, "feeGh":0.00, "sid":"359963" }])
        await self.__check_table_row(UserStockSell, {'sid':"359963"}, {'portion':26900, 'money_sold':23134, 'fee':2.78})

        await usm.archive_deals(self.testuser, '2021-10')
        await self.__check_table_row(UserArchivedDeals, {'sid': "359963"}, {'portion':26900, 'fee':2.78})
        await self.__check_table_row(UserArchivedDeals, {'sid': "1410475"}, {'portion':3000})
        await self.__check_table_row(UserArchivedDeals, {'sid': "966285"}, {'portion':5800})
        ssum = await query_aggregate('sum', UserArchivedDeals, UserArchivedDeals.portion, UserArchivedDeals.user_id == self.testuser.id, UserArchivedDeals.typebs == 'S')
        bsum = await query_aggregate('sum', UserArchivedDeals, UserArchivedDeals.portion, UserArchivedDeals.user_id == self.testuser.id, UserArchivedDeals.typebs == 'B')
        self.assertEqual(ssum, bsum, 'buy portion NOT Equals to sell portion!')

    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_archive_deals_3(self):
        await self.__cleanup_tables([UserStocks, UserEarned, UserArchivedDeals, UserStockBuy, UserStockSell])

        await usm.add_deals(self.testuser, [{"code":"SH588300", "time":"2021-07-05", "tradeType":"B", "count":900, "price":0.9790, "fee":0.22, "feeYh":0.00, "feeGh":0.00, "sid":"192270" },
            {"code":"SH588300", "time":"2021-07-06", "tradeType":"B", "count":3100, "price":0.9530, "fee":0.74, "feeYh":0.00, "feeGh":0.00, "sid":"243715" },
            {"code":"SH588300", "time":"2021-07-16", "tradeType":"B", "count":3000, "price":0.9820, "fee":0.35, "feeYh":0.00, "feeGh":0.00, "sid":"1410475" },
            {"code":"SH588300", "time":"2021-07-19", "tradeType":"B", "count":2000, "price":0.9740, "fee":0.23, "feeYh":0.00, "feeGh":0.00, "sid":"1154776" },
            {"code":"SH588300", "time":"2021-07-26", "tradeType":"B", "count":3000, "price":0.9540, "fee":0.34, "feeYh":0.00, "feeGh":0.00, "sid":"664918" },
            {"code":"SH588300", "time":"2021-07-28", "tradeType":"B", "count":900, "price":0.9000, "fee":0.10, "feeYh":0.00, "feeGh":0.00, "sid":"353493" },
            {"code":"SH588300", "time":"2021-07-28", "tradeType":"B", "count":2100, "price":0.9300, "fee":0.23, "feeYh":0.00, "feeGh":0.00, "sid":"821612" },
            {"code":"SH588300", "time":"2021-07-30", "tradeType":"S", "count":5000, "price":0.9710, "fee":0.58, "feeYh":0.00, "feeGh":0.00, "sid":"77329" },
            {"code":"SH588300", "time":"2021-08-13", "tradeType":"B", "count":5300, "price":0.9320, "fee":0.59, "feeYh":0.00, "feeGh":0.00, "sid":"1428159" },
            {"code":"SH588300", "time":"2021-09-01", "tradeType":"B", "count":5800, "price":0.8580, "fee":0.60, "feeYh":0.00, "feeGh":0.00, "sid":"280527" },
            {"code":"SH588300", "time":"2021-09-17", "tradeType":"B", "count":5800, "price":0.8510, "fee":0.59, "feeYh":0.00, "feeGh":0.00, "sid":"966285" },
            {"code":"SH588300", "time":"2021-09-24", "tradeType":"B", "count":5800, "price":0.8580, "fee":0.60, "feeYh":0.00, "feeGh":0.00, "sid":"87198" },
            {"code":"SH588300", "time":"2021-09-24", "tradeType":"B", "count":5800, "price":0.8580, "fee":0.60, "feeYh":0.00, "feeGh":0.00, "sid":"87292" },
            {"code":"SH588300","time":"2021-09-24","count":5800,"price":0.8580,"tradeType":"B","sid":"87305","fee":0.60,"feeYh":0.00,"feeGh":0.00},
            {"code":"SH588300", "time":"2021-09-24", "tradeType":"S", "count":26900, "price":0.8600, "fee":2.78, "feeYh":0.00, "feeGh":0.00, "sid":"359963" }])

        await self.__check_table_row(UserStockBuy, {'sid':"1410475"}, {'portion':3000, 'soldout':1, 'soldptn':3000})
        await usm.archive_deals(self.testuser, '2021-08')
        await self.__check_table_row(UserStockBuy, {'sid':"1410475"}, {'portion':2000, 'soldout':1, 'soldptn':2000})
        ssum = await query_aggregate('sum', UserArchivedDeals, UserArchivedDeals.portion, UserArchivedDeals.user_id == self.testuser.id, UserArchivedDeals.typebs == 'S')
        bsum = await query_aggregate('sum', UserArchivedDeals, UserArchivedDeals.portion, UserArchivedDeals.user_id == self.testuser.id, UserArchivedDeals.typebs == 'B')

        await usm.add_deals(self.testuser, [{"code":"SH588300", "time":"2021-07-16", "tradeType":"B", "count":3000, "price":0.9820, "fee":0.35, "feeYh":0.00, "feeGh":0.00, "sid":"1410475" }])
        await self.__check_table_row(UserStockBuy, {'sid':"1410475"}, {'portion':2000, 'soldout':1, 'soldptn':2000})

        await usm.archive_deals(self.testuser, '2021-10')
        ssum = await query_aggregate('sum', UserArchivedDeals, UserArchivedDeals.portion, UserArchivedDeals.user_id == self.testuser.id, UserArchivedDeals.typebs == 'S')
        bsum = await query_aggregate('sum', UserArchivedDeals, UserArchivedDeals.portion, UserArchivedDeals.user_id == self.testuser.id, UserArchivedDeals.typebs == 'B')
        self.assertEqual(ssum, bsum, 'buy portion NOT Equals to sell portion!')

        srec = await query_aggregate('count', UserStockSell, UserStockSell.code, UserStockSell.user_id == self.testuser.id, UserStockSell.code == 'SH588300')
        self.assertEqual(srec, 0, 'sell records should delete when archived ')


    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_archive_deals_4(self):
        await self.__cleanup_tables([UserStocks, UserEarned, UserArchivedDeals, UserStockBuy, UserStockSell])
        await usm.add_deals(self.testuser, [{"code":"SH588300", "time":"2021-09-24", "tradeType":"B", "count":5800, "price":0.8580, "fee":0.60, "feeYh":0.00, "feeGh":0.00, "sid":"87198" },
            {"code":"SH588300", "time":"2021-09-24", "tradeType":"B", "count":5800, "price":0.8580, "fee":0.60, "feeYh":0.00, "feeGh":0.00, "sid":"87292" },
            {"code":"SH588300","time":"2021-09-24","count":5800,"price":0.8580,"tradeType":"B","sid":"87305","fee":0.60,"feeYh":0.00,"feeGh":0.00},
            {"code":"SH588300", "time":"2021-11-19", "tradeType":"B", "count":5600, "price":0.8870, "fee":0.60, "feeYh":0.00, "feeGh":0.00, "sid":"527694" },
            {"code":"SH588300","time":"2022-01-04","count":7000,"price":0.8280,"tradeType":"B","sid":"840993","fee":0.70,"feeYh":0.00,"feeGh":0.00},
            {"code":"SH588300","time":"2022-01-05","count":5600,"price":0.7940,"tradeType":"B","sid":"822210","fee":0.53,"feeYh":0.00,"feeGh":0.00},
            {"code":"SH588300","time":"2022-01-06","count":10000,"price":0.7960,"tradeType":"B","sid":"330656","fee":1.19,"feeYh":0.00,"feeGh":0.00},
            {"code":"SH588300","time":"2022-01-07","count":12500,"price":0.7950,"tradeType":"B","sid":"232892","fee":1.49,"feeYh":0.00,"feeGh":0.00},
            {"code":"SH588300","time":"2022-01-26","count":12600,"price":0.7650,"tradeType":"B","sid":"172538","fee":1.45,"feeYh":0.00,"feeGh":0.00},
            {"code":"SH588300","time":"2022-01-26","count":5600,"price":0.7700,"tradeType":"B","sid":"718618","fee":0.52,"feeYh":0.00,"feeGh":0.00},
            {"code":"SH588300","time":"2022-02-08","count":12600,"price":0.7230,"tradeType":"B","sid":"115589","fee":1.37,"feeYh":0.00,"feeGh":0.00},
            {"code":"SH588300","time":"2022-02-15","count":5600,"price":0.7000,"tradeType":"B","sid":"72297","fee":0.47,"feeYh":0.00,"feeGh":0.00},
            {"code":"SH588300","time":"2022-03-02", "tradeType":"S", "count":5600, "price":0.7300, "fee":0.61, "feeYh":0.00, "feeGh":0.00, "sid":"173470" },
            {"code":"SH588300","time":"2022-03-07","count":14300,"price":0.6960,"tradeType":"B","sid":"111980","fee":1.49,"feeYh":0.00,"feeGh":0.00},
            {"code":"SH588300","time":"2022-04-12","count":15600,"price":0.6370,"tradeType":"B","sid":"25796","fee":1.49,"feeYh":0.00,"feeGh":0.00},
            {"code":"SH588300","time":"2022-04-20","count":16400,"price":0.6070,"tradeType":"B","sid":"328715","fee":1.49,"feeYh":0.00,"feeGh":0.00},
            {"code":"SH588300", "time":"2022-05-16", "tradeType":"S", "count":16400, "price":0.6060, "sid":"192309" }])

        await self.__check_table_row(UserStockBuy, {'sid':"87198"}, {'portion':5800, 'soldout':1, 'soldptn':5800})
        await self.__check_table_row(UserStockBuy, {'sid':"527694"}, {'portion':5600, 'soldout':0, 'soldptn':4600})

        await usm.add_deals(self.testuser, [{"code":"SH588300", "time":"2022-05-16", "tradeType":"S", "count":16400, "price":0.6060, "fee":1.49, "feeYh":0.00, "feeGh":0.00, "sid":"192309" },
            {"code":"SH588300","time":"2022-06-09","count":15600,"price":0.6570,"tradeType":"S","sid":"189309"}])
        await self.__check_table_row(UserStockBuy, {'sid':"527694"}, {'portion':5600, 'soldout':1, 'soldptn':5600})
        await self.__check_table_row(UserStockBuy, {'sid':"330656"}, {'portion':10000, 'soldout':0, 'soldptn':2000})

        await usm.archive_deals(self.testuser, '2022-06')
        await self.__check_table_row(UserStocks, {'code': "SH588300"}, {'portion_hold':103200,'aver_price':0.708635})
        srec = await query_aggregate('count', UserStockSell, UserStockSell.code == 'SH588300', UserStockSell.user_id == self.testuser.id)
        self.assertGreater(srec, 0, 'sell records not found!')

        await usm.add_deals(self.testuser, [{"code":"SH588300","time":"2022-06-09","count":15600,"price":0.6570,"tradeType":"S","sid":"189309","fee":0.00,"feeYh":0.00,"feeGh":0.00}])
        await usm.archive_deals(self.testuser, '2022-07')
        await self.__check_table_row(UserStocks, {'code': "SH588300"}, {'portion_hold':103200,'aver_price':0.708635})
        srec = await query_aggregate('count', UserStockSell, UserStockSell.code == 'SH588300', UserStockSell.user_id == self.testuser.id)
        self.assertEqual(srec, 0, 'sell records should be delete when archived with no buy records!')

    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_update_archived_fee(self):
        await self.__cleanup_tables([UserStocks, UserEarned, UserArchivedDeals, UserStockBuy, UserStockSell])

        await usm.add_deals(self.testuser, [{"code":"SH588300", "time":"2021-07-05", "tradeType":"B", "count":900, "price":0.9790, "fee":0.22, "feeYh":0.00, "feeGh":0.00, "sid":"192270" },
            {"code":"SH588300", "time":"2021-07-06", "tradeType":"B", "count":3100, "price":0.9530, "fee":0.74, "feeYh":0.00, "feeGh":0.00, "sid":"243715" },
            {"code":"SH588300", "time":"2021-07-16", "tradeType":"B", "count":3000, "price":0.9820, "fee":0.35, "feeYh":0.00, "feeGh":0.00, "sid":"1410475" },
            {"code":"SH588300", "time":"2021-07-19", "tradeType":"B", "count":2000, "price":0.9740, "fee":0.23, "feeYh":0.00, "feeGh":0.00, "sid":"1154776" },
            {"code":"SH588300", "time":"2021-07-26", "tradeType":"B", "count":3000, "price":0.9540, "fee":0.34, "feeYh":0.00, "feeGh":0.00, "sid":"664918" },
            {"code":"SH588300", "time":"2021-07-28", "tradeType":"B", "count":900, "price":0.9000, "fee":0.10, "feeYh":0.00, "feeGh":0.00, "sid":"353493" },
            {"code":"SH588300", "time":"2021-07-28", "tradeType":"B", "count":2100, "price":0.9300, "fee":0.23, "feeYh":0.00, "feeGh":0.00, "sid":"821612" },
            {"code":"SH588300", "time":"2021-07-30", "tradeType":"S", "count":5000, "price":0.9710, "sid":"77329" }])
        await usm.archive_deals(self.testuser, '2021-08')
        await self.__check_table_row(UserArchivedDeals, {'sid':"77329"}, {'portion':5000, 'price':0.9710, 'fee':0})

        await usm.add_deals(self.testuser, [{"code":"SH588300", "time":"2021-07-30", "tradeType":"S", "count":5000, "price":0.9710, "fee":0.58, "feeYh":0.00, "feeGh":0.00, "sid":"77329" }])
        await self.__check_table_row(UserArchivedDeals, {'sid':"77329"}, {'portion':5000, 'price':0.9710, 'fee':0.58})

    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_archive_update(self):
        await self.__cleanup_tables([UserStocks, UserEarned, UserArchivedDeals, UserStockBuy, UserStockSell])

        await usm.add_deals(self.testuser, [
            {"time":"2021-12-15","sid":"90546","code":"SH603726","tradeType":"B","price":"16.2600","count":"300","fee":"5.00","feeYh":".00","feeGh":".10"},
            {"time":"2021-12-23","sid":"344117","code":"SH603726","tradeType":"S","price":"14.5060","count":"300","fee":"5.00","feeYh":"4.35","feeGh":".09"},
            {"time":"2021-12-31","sid":"1156657","code":"SH603726","tradeType":"B","price":"14.1900","count":"300","fee":"5.00","feeYh":".00","feeGh":".09"},
            {"time":"2022-01-05","sid":"1830500","code":"SH603726","tradeType":"S","price":"14.0800","count":"300","fee":"5.00","feeYh":"4.22","feeGh":".08"},
            {"time":"2022-01-06","sid":"1321420","code":"SH603726","tradeType":"B","price":"14.3100","count":"300","fee":"5.00","feeYh":".00","feeGh":".09"},
            {"time":"2022-04-01","sid":"344750","code":"SH603726","tradeType":"B","price":"13.1800","count":"400","fee":"5.00","feeYh":".00","feeGh":".12"},
            {"time":"2022-04-26","sid":"148257","code":"SH603726","tradeType":"B","price":"11.4000","count":"400","fee":"5.00","feeYh":".00","feeGh":".10"},
            {"time":"2022-05-27","sid":"1317012","code":"SH603726","tradeType":"S","price":"11.9100","count":"400","fee":"5.00","feeYh":"4.76","feeGh":".05"}
        ])

        await usm.archive_deals(self.testuser, '2022-06')
        await self.__check_table_row(UserStocks, {'code': "SH603726"}, {'portion_hold':700,'aver_price':12.1629})
        await usm.add_deals(self.testuser, [
            {"time":"2021-12-15","sid":"90546","code":"SH603726","tradeType":"B","price":"16.2600","count":"300","fee":"5.00","feeYh":".00","feeGh":".10"},
            {"time":"2021-12-23","sid":"344117","code":"SH603726","tradeType":"S","price":"14.5060","count":"300","fee":"5.00","feeYh":"4.35","feeGh":".09"},
            {"time":"2021-12-31","sid":"1156657","code":"SH603726","tradeType":"B","price":"14.1900","count":"300","fee":"5.00","feeYh":".00","feeGh":".09"},
            {"time":"2022-01-05","sid":"1830500","code":"SH603726","tradeType":"S","price":"14.0800","count":"300","fee":"5.00","feeYh":"4.22","feeGh":".08"},
            {"time":"2022-01-06","sid":"1321420","code":"SH603726","tradeType":"B","price":"14.3100","count":"300","fee":"5.00","feeYh":".00","feeGh":".09"},
            {"time":"2022-04-01","sid":"344750","code":"SH603726","tradeType":"B","price":"13.1800","count":"400","fee":"5.00","feeYh":".00","feeGh":".12"},
            {"time":"2022-04-26","sid":"148257","code":"SH603726","tradeType":"B","price":"11.4000","count":"400","fee":"5.00","feeYh":".00","feeGh":".10"},
            {"time":"2022-05-27","sid":"1317012","code":"SH603726","tradeType":"S","price":"11.9100","count":"400","fee":"5.00","feeYh":"4.76","feeGh":".05"}
        ])
        await self.__check_table_row(UserStocks, {'code': "SH603726"}, {'portion_hold':700,'aver_price':12.1629})


    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_archive_update_1(self):
        await self.__cleanup_tables([UserStocks, UserEarned, UserArchivedDeals, UserStockBuy, UserStockSell])

        await usm.add_deals(self.testuser, [
            {"code":"SZ000055", "time": "2021-12-30", "tradeType":"B", "count":1000, "price":"4.8800", "fee":"5.00", "feeYh":"0.00", "feeGh":0.00, "sid":"1003933"},
            {"code":"SZ000055", "time": "2022-01-27", "tradeType":"B", "count":1000, "price":"4.4200", "fee":"5.00", "feeYh":"0.00", "feeGh":0.00, "sid":"246834"},
            {"code":"SZ000055", "time": "2022-04-26", "tradeType":"B", "count":1200, "price":"3.9100", "fee":"5.00", "feeYh":"0.00", "feeGh":0.00, "sid":"210151"},
            {"code":"SZ000055", "time": "2022-05-12", "tradeType":"S", "count":1200, "price":"4.0900", "fee":"5.00", "feeYh":"4.91", "feeGh":0.00, "sid":"1265386"}
        ])

        await usm.archive_deals(self.testuser, '2022-06')
        await self.__check_table_row(UserStocks, {'code': "SZ000055"}, {'portion_hold':2000,'aver_price':4.114})
        await usm.add_deals(self.testuser, [{"code":"SZ000055", "time": "2022-06-09", "tradeType":"S", "count":1000, "price":"4.2400", "fee":"5.00", "feeYh":"4.24", "feeGh":0.00, "sid":"884840"}])
        await self.__check_table_row(UserStocks, {'code': "SZ000055"}, {'portion_hold':1000,'aver_price':3.91})
        await self.__check_table_row(UserStockBuy, {'sid':"246834"}, {'portion':800, 'soldout':1, 'soldptn':800})
        await self.__check_table_row(UserStockBuy, {'sid':"210151"}, {'portion':1200, 'soldout':0, 'soldptn':200})


    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_add_dividen_shares(self):

        await self.__cleanup_tables([UserStocks, UserStockBuy])

        await usm.add_deals(self.testuser, [
            {"time":"2022-06-16 15:00:00","sid":"","code":"SZ002459","tradeType":"B","price":".0000","count":"40","fee":".00","feeYh":".00","feeGh":".00"}
        ])

        await self.__check_table_row(UserStocks, {'code': "SZ002459"}, {'portion_hold':40,'aver_price':0})
        await self.__check_table_row(UserStockBuy, {'user_id':self.testuser.id, 'time': "2022-06-16"}, {'portion':40})

        await usm.add_deals(self.testuser, [
            {"time":"2022-06-16 15:00:00","sid":"","code":"SZ002459","tradeType":"B","price":".0000","count":"40","fee":".00","feeYh":".00","feeGh":".00"}
        ])

        await self.__check_table_row(UserStocks, {'code': "SZ002459"}, {'portion_hold':40,'aver_price':0})
        await self.__check_table_row(UserStockBuy, {'user_id':self.testuser.id, 'time': "2022-06-16"}, {'portion':40})


    @patch('app.stock.manager.AllStocks.is_exists', AsyncMock(return_value=True))
    async def test_add_buy_sell_margin_deals(self):
        await self.__cleanup_tables([UserStocks, UserStockBuy, UserStockSell])

        await usm.add_deals(self.testuser, [{"time":"2023-05-26","sid":"s_001","code":"SH510050","tradeType":"B","price":"2.567","count":"3800"}])
        await usm.add_deals(self.testuser, [{"time":"2023-05-26","sid":"s_002","code":"SH510050","tradeType":"S","price":"2.549","count":"4000"}])
        await usm.add_deals(self.testuser, [{"time":"2023-06-07","sid":"s_003","code":"SH510050","tradeType":"B","price":"2.539","count":"4000"}])

        await self.__check_table_row(UserStocks, {'code': "SH510050"}, {'cost_hold':9648.2, 'portion_hold':3800, 'aver_price':2.539})


    async def test_save_strategy(self):
        code = 'SZ000096'
        strobj = {
            "grptype":"GroupStandard","strategies":
            {"0":{"key":"StrategyGE","enabled":True,"stepRate":0.075,"kltype":"30","guardPrice":"1.342","inCritical":False}},
            "transfers":{"0":{"transfer":"-1"}},
            "buydetail":[{"date":"0","count":7800,"price":"1.444","type":"B"},{"date":"2024-11-29","count":"7400","price":"1.336000","sid":"390813","type":"B"}],
            "buydetail_full":[{"date":"0","count":7800,"price":"1.444","type":"B"},{"date":"2024-11-29","count":"7400","price":"1.336000","sid":"390813","type":"B"}],
            "count0":7400,"amount":10000
        }

        await self.__cleanup_tables([UserStrategy, UserOrders, UserFullOrders])

        await usm.save_strategy(self.testuser, code, strobj)
        await self.__check_table_row(UserStrategy, {'code': code}, {'id': 0, 'skey': 'StrategyGE', 'trans': -1})
        await self.__check_table_row(UserOrders, {'code': code, "sid":"390813"}, {"time":"2024-11-29","count":7400})
        await self.__check_table_row(UserFullOrders, {'code': code, "sid":"390813"}, {"time":"2024-11-29","count":7400})

    async def test_load_strategy(self):
        code = 'SZ000096'
        strobj = {
            "grptype":"GroupStandard","strategies":
            {"0":{"key":"StrategyGE","enabled":True,"stepRate":0.075,"kltype":"30","guardPrice":"1.342","inCritical":False}},
            "transfers":{"0":{"transfer":"-1"}},
            "buydetail":[{"date":"0","count":7800,"price":"1.444","type":"B"},{"date":"2024-11-29","count":"7400","price":"1.336000","sid":"390813","type":"B"}],
            "buydetail_full":[{"date":"0","count":7800,"price":"1.444","type":"B"},{"date":"2024-11-29","count":"7400","price":"1.336000","sid":"390813","type":"B"}],
            "count0":7400,"amount":10000
        }

        await self.__cleanup_tables([UserStrategy, UserOrders, UserFullOrders])

        await usm.save_strategy(self.testuser, code, strobj)
        strdata = await usm.load_strategy(self.testuser, code)
        assert strdata['grptype'] == 'GroupStandard', 'grptype wrong!'
        assert 0 in strdata['strategies'], 'strategy id error'
        assert 'StrategyGE' == strdata['strategies'][0]['key'], 'strategy key error'
        assert 'buydetail' in strdata, 'no buydetail'
        assert 'buydetail_full' in strdata, 'no buydetail_full'
        assert 10000 == strdata['amount'], f'''amount is not expected: 10000, actual: {strdata['amount']}'''

    async def test_update_strategy(self):
        code = 'SZ000096'
        strobj = {
            "grptype": "GroupStandard",
            "strategies": {"0": {"key": "StrategySellELS", "enabled": False, "cutselltype": "all", "selltype": "all", "topprice": 19.76, "guardPrice": 17.88}},
            "transfers": {"0": {"transfer": "-1"}}, "amount": "5000"
        }

        await self.__cleanup_tables([UserStrategy, UserOrders, UserFullOrders])

        await usm.save_strategy(self.testuser, code, strobj)
        strobj2 = {
            "grptype":"GroupStandard",
            "strategies":{
                "0":{"key":"StrategySellELS","enabled":True,"cutselltype":"all","selltype":"all","topprice":"5.25","guardPrice":4.9},
                "1":{"key":"StrategySellBE","enabled":True,"upRate":-0.03,"selltype":"single","sell_conds":"4"}},
            "transfers":{"0":{"transfer":"-1"},"1":{"transfer":"-1"}},
            "buydetail":[{"date":"2025-03-14","count":"1300","price":"4.990000","sid":"35329","type":"B"}],
            "buydetail_full":[{"date":"2025-03-14","count":"1300","price":"4.990000","sid":"35329","type":"B"}],"count0":1300,
            "amount":"5000",
            "uramount":{"key":"hotrank0","id":69}
        }

        await usm.save_strategy(self.testuser, code, strobj2)
        strdata = await usm.load_strategy(self.testuser, code)
        assert strdata['grptype'] == 'GroupStandard', 'grptype wrong!'
        assert 1 in strdata['strategies'], 'strategy id error'
        assert 'StrategySellBE' == strdata['strategies'][1]['key'], 'strategy key error'
        assert strdata['strategies'][0]['enabled'], 'strategy key error'
        assert 'uramount' in strdata, 'uramount not exists'
        assert 'hotrank0' == strdata['uramount']['key'], 'uramount key wrong!'
        assert 1 == len(strdata['buydetail']), 'buydetail length wrong'

    async def test_update_strategy_more(self):
        code = 'SZ000096'
        strobj = {
            "grptype": "GroupStandard",
            "strategies": {"0": {"key": "StrategySellELS", "enabled": False, "cutselltype": "all", "selltype": "all", "topprice": 19.76, "guardPrice": 17.88}},
            "transfers": {"0": {"transfer": "-1"}},
            "buydetail":[{"date":"2025-03-14","count":"1300","price":"4.990000","sid":"35329","type":"B"}],
            "buydetail_full":[{"date":"2025-03-14","count":"1300","price":"4.990000","sid":"35329","type":"B"}],"count0":1300,
            "amount": "5000"
        }

        await self.__cleanup_tables([UserStrategy, UserOrders, UserFullOrders])

        await usm.save_strategy(self.testuser, code, strobj)
        strobj2 = {
            "grptype":"GroupStandard",
            "strategies":{
                "0":{"key":"StrategySellELS","enabled":True,"cutselltype":"all","selltype":"all","topprice":"5.25","guardPrice":4.9},
                "1":{"key":"StrategySellBE","enabled":True,"upRate":-0.03,"selltype":"single","sell_conds":"4"}},
            "transfers":{"0":{"transfer":"-1"},"1":{"transfer":"-1"}},
            "buydetail":[{'code': '600744', 'type': 'B', 'price': 4.41, 'count': 600, 'date': '2025-10-09', 'sid': '1584731'}, {"date":"2025-03-14","count":"1300","price":"4.990000","sid":"35329","type":"B"}],
            "buydetail_full":[{"date":"2025-03-14","count":"1300","price":"4.990000","sid":"35329","type":"B"}, {'code': '600744', 'type': 'B', 'price': 4.41, 'count': 600, 'date': '2025-10-09', 'sid': '1584731'}],"count0":1300,
            "amount":"5000"
        }

        await usm.save_strategy(self.testuser, code, strobj2)
        strdata = await usm.load_strategy(self.testuser, code)
        assert strdata['grptype'] == 'GroupStandard', 'grptype wrong!'
        assert 1 in strdata['strategies'], 'strategy id error'
        assert 'StrategySellBE' == strdata['strategies'][1]['key'], 'strategy key error'
        assert strdata['strategies'][0]['enabled'], 'strategy key error'
        assert 'uramount' in strdata, 'uramount not exists'

    async def test_update_strategy_less(self):
        code = 'SZ000096'
        strobj = {
            "grptype":"GroupStandard",
            "strategies":{
                "0":{"key":"StrategySellELS","enabled":True,"cutselltype":"all","selltype":"all","topprice":"5.25","guardPrice":4.9},
                "1":{"key":"StrategySellBE","enabled":True,"upRate":-0.03,"selltype":"single","sell_conds":"4"}},
            "transfers":{"0":{"transfer":"-1"},"1":{"transfer":"-1"}},
            "amount":"5000",
            "uramount":{"key":"hotrank0","id":69}
        }
        code1 = 'SH601611'

        await self.__cleanup_tables([UserStrategy, UserOrders, UserFullOrders])

        await usm.save_strategy(self.testuser, code, strobj)
        await usm.save_strategy(self.testuser, code1, strobj)

        strobj2 = {
            "grptype": "GroupStandard",
            "strategies": {"1":{"key":"StrategySellBE","enabled":True,"upRate":-0.03,"selltype":"single","sell_conds":"4"}},
            "transfers": {"1": {"transfer": "-1"}}, "amount": "5000"
        }
        await usm.save_strategy(self.testuser, code, strobj2)
        strdata = await usm.load_strategy(self.testuser, code)
        assert strdata['grptype'] == 'GroupStandard', 'grptype wrong!'
        assert 'StrategySellBE' == strdata['strategies'][1]['key'], 'strategy key error'
        assert 1 == len(strdata['strategies']), 'buydetail length wrong'
        strdata1 = await usm.load_strategy(self.testuser, code1)
        assert strdata1['grptype'] == 'GroupStandard', 'grptype wrong!'
        assert len(strdata1['strategies']) == len(strobj['strategies']), 'strategies not equal'

    async def test_delete_strategies(self):
        code = 'SZ000096'
        strobj = {
            "grptype": "GroupStandard",
            "strategies": {"0": {"key": "StrategySellELS", "enabled": False, "cutselltype": "all", "selltype": "all", "topprice": 19.76, "guardPrice": 17.88}},
            "transfers": {"0": {"transfer": "-1"}}, "amount": "5000"
        }

        await self.__cleanup_tables([UserStrategy, UserOrders, UserFullOrders])
        await usm.save_strategy(self.testuser, code, strobj)
        await usm.remove_strategy(self.testuser, code)

        sex = await query_aggregate('count', UserStrategy, UserStrategy.code == code, UserStrategy.user_id == self.testuser.id)
        self.assertEqual(0, sex, 'strategy not removed')
        oex = await query_aggregate('count', UserOrders, UserOrders.code == code, UserOrders.user_id == self.testuser.id)
        self.assertEqual(0, oex, 'orders not removed')
        foex = await query_aggregate('count', UserFullOrders, UserFullOrders.code == code, UserFullOrders.user_id == self.testuser.id)
        self.assertEqual(0, foex, 'full orders not removed')

    async def test_user_get_deals(self):
        deals = await usm.get_deals(self.testuser)
        assert len(deals) > 0, 'deals not found'


if __name__ == '__main__':
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(TestUserStock('test_user_get_deals'))
    unittest.TextTestRunner().run(suite)
