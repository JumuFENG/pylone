import unittest
from unittest.mock import patch
import sys
import os

sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..')))
from app.lofig import Config
dbcfg = Config.database_config()
dbcfg['dbname'] = 'testdb'
patcher = patch('app.lofig.Config.database_config')
mockdbcfg = patcher.start()
mockdbcfg.return_value = dbcfg

from app.stock.models import MdlAllStock
from app.db import upsert_one, upsert_many, query_one_value, query_aggregate, query_values, delete_records


@unittest.skip("skip it!")
class TestDatabaseOperations(unittest.IsolatedAsyncioTestCase):
    """测试数据库操作函数"""
    test_code = "test0001"
    test_code1 = "test0002"
    test_code2 = "test0003"
    test_code3 = "test0004"

    async def test_upsert_one_insert(self):
        """测试 upsert_one 插入新记录"""
        # 清理测试数据
        await delete_records(MdlAllStock, MdlAllStock.code == self.test_code)

        data = {
            "code": self.test_code,
            "name": "平安银行",
            "typekind": "stock",
            "setup_date": "1991-04-03",
            "quit_date": None
        }

        await upsert_one(MdlAllStock, data, unique_fields=["code"])

        # 验证插入成功
        result = await query_one_value(MdlAllStock, "name", MdlAllStock.code == self.test_code)
        self.assertEqual(result, "平安银行")

        # 清理
        await delete_records(MdlAllStock, MdlAllStock.code == self.test_code)

    async def test_upsert_one_update(self):
        """测试 upsert_one 更新已存在的记录"""
        # 清理测试数据
        await delete_records(MdlAllStock, MdlAllStock.code == self.test_code)

        # 先插入一条记录
        data = {
            "code": self.test_code,
            "name": "平安银行",
            "typekind": "stock",
            "setup_date": "1991-04-03",
            "quit_date": None
        }
        await upsert_one(MdlAllStock, data, unique_fields=["code"])

        # 更新记录
        updated_data = {
            "code": self.test_code,
            "name": "平安银行更新",
            "typekind": "stock",
            "setup_date": "1991-04-03",
            "quit_date": "2025-01-01"
        }
        await upsert_one(MdlAllStock, updated_data, unique_fields=["code"])

        # 验证更新成功
        result = await query_one_value(MdlAllStock, "name", MdlAllStock.code == self.test_code)
        self.assertEqual(result, "平安银行更新")

        quit_date = await query_one_value(MdlAllStock, "quit_date", MdlAllStock.code == self.test_code)
        self.assertEqual(quit_date, "2025-01-01")

        # 清理
        await delete_records(MdlAllStock, MdlAllStock.code == self.test_code)

    async def test_upsert_many(self):
        """测试 upsert_many 批量插入和更新"""
        # 清理测试数据
        await delete_records(MdlAllStock, MdlAllStock.code.in_([self.test_code, self.test_code2, self.test_code1, self.test_code3]))

        data_list = [
            {
                "code": self.test_code,
                "name": "平安银行",
                "typekind": "stock",
                "setup_date": "1991-04-03",
                "quit_date": None
            },
            {
                "code": self.test_code2,
                "name": "万科A",
                "typekind": "stock",
                "setup_date": "1991-01-29",
                "quit_date": None
            },
            {
                "code": self.test_code3,
                "name": "浦发银行",
                "typekind": "stock",
                "setup_date": "1999-11-10",
                "quit_date": None
            }
        ]

        added, updated = await upsert_many(MdlAllStock, data_list, unique_fields=["code"])
        self.assertEqual(added, 3)
        self.assertEqual(updated, 0)

        # 更新部分记录
        updated_data_list = [
            {
                "code": self.test_code,
                "name": "平安银行更新",
                "typekind": "stock",
                "setup_date": "1991-04-03",
                "quit_date": None
            },
            {
                "code": self.test_code1,
                "name": "新股票",
                "typekind": "stock",
                "setup_date": "2025-01-01",
                "quit_date": None
            }
        ]

        added, updated = await upsert_many(MdlAllStock, updated_data_list, unique_fields=["code"])
        self.assertEqual(added, 1)  # 新增 000003
        self.assertEqual(updated, 1)  # 更新 000001

        # 验证更新
        result = await query_one_value(MdlAllStock, "name", MdlAllStock.code == self.test_code)
        self.assertEqual(result, "平安银行更新")

        # 清理
        await delete_records(MdlAllStock, MdlAllStock.code.in_([self.test_code, self.test_code2, self.test_code1, self.test_code3]))

    async def test_upsert_many_arr(self):
        """测试 upsert_many 批量插入和更新"""
        # 清理测试数据
        await delete_records(MdlAllStock, MdlAllStock.code.in_([self.test_code, self.test_code2, self.test_code1, self.test_code3]))

        data_list = [
            {
                "code": self.test_code,
                "name": "平安银行",
                "typekind": "stock",
                "setup_date": "1991-04-03",
                "quit_date": None
            },
            {
                "code": self.test_code2,
                "name": "万科A",
                "typekind": "stock",
                "setup_date": "1991-01-29",
                "quit_date": None
            },
            {
                "code": self.test_code3,
                "name": "浦发银行",
                "typekind": "stock",
                "setup_date": "1999-11-10",
                "quit_date": None
            }
        ]

        added, updated = await upsert_many(MdlAllStock, data_list, unique_fields=["code"])
        self.assertEqual(added, 3)
        self.assertEqual(updated, 0)

        # 更新部分记录
        updated_data_list = [[self.test_code, "平安银行更新", "stock", "1991-04-03", None],
                             [self.test_code1, "新股票", "stock", "2025-01-01", None]]
        cols = [c.name for c in MdlAllStock.__table__.columns if c.name != MdlAllStock.__table__.autoincrement_column.name]
        updated_data_list = [dict(zip(cols, row)) for row in updated_data_list]

        added, updated = await upsert_many(MdlAllStock, updated_data_list, unique_fields=["code"])
        self.assertEqual(added, 1)  # 新增 000003
        self.assertEqual(updated, 1)  # 更新 000001

        # 验证更新
        result = await query_one_value(MdlAllStock, "name", MdlAllStock.code == self.test_code)
        self.assertEqual(result, "平安银行更新")

        # 清理
        await delete_records(MdlAllStock, MdlAllStock.code.in_([self.test_code, self.test_code2, self.test_code1, self.test_code3]))

    async def test_query_one_value(self):
        """测试 query_one_value 查询单个字段值"""
        # 清理测试数据
        await delete_records(MdlAllStock, MdlAllStock.code == self.test_code)

        data = {
            "code": self.test_code,
            "name": "平安银行",
            "typekind": "stock",
            "setup_date": "1991-04-03",
            "quit_date": None
        }
        await upsert_one(MdlAllStock, data, unique_fields=["code"])

        # 查询 name 字段
        name = await query_one_value(MdlAllStock, "name", MdlAllStock.code == self.test_code)
        self.assertEqual(name, "平安银行")

        # 查询 typekind 字段
        typekind = await query_one_value(MdlAllStock, "typekind", MdlAllStock.code == self.test_code)
        self.assertEqual(typekind, "stock")

        # 查询不存在的记录
        result = await query_one_value(MdlAllStock, "name", MdlAllStock.code == "999999")
        self.assertIsNone(result)

        # 清理
        await delete_records(MdlAllStock, MdlAllStock.code == self.test_code)

    async def test_query_aggregate(self):
        """测试 query_aggregate 聚合查询"""
        # 清理测试数据
        await delete_records(MdlAllStock, MdlAllStock.code.in_([self.test_code, self.test_code2, self.test_code3]))

        data_list = [
            {
                "code": self.test_code,
                "name": "平安银行",
                "typekind": "stock",
                "setup_date": "1991-04-03",
                "quit_date": None
            },
            {
                "code": self.test_code2,
                "name": "万科A",
                "typekind": "stock",
                "setup_date": "1991-01-29",
                "quit_date": None
            },
            {
                "code": self.test_code3,
                "name": "浦发银行",
                "typekind": "stock",
                "setup_date": "1999-11-10",
                "quit_date": None
            }
        ]
        await upsert_many(MdlAllStock, data_list, unique_fields=["code"])

        # 测试 count
        count = await query_aggregate("count", MdlAllStock, "id", MdlAllStock.code.in_([self.test_code, self.test_code2, self.test_code3]))
        self.assertEqual(count, 3)

        # 测试带条件的 count
        count_stock = await query_aggregate("count", MdlAllStock, "id", MdlAllStock.typekind == "stock", MdlAllStock.code.in_([self.test_code, self.test_code2, self.test_code3]))
        self.assertEqual(count_stock, 3)

        # 测试 max
        max_code = await query_aggregate("max", MdlAllStock, "code", MdlAllStock.code.in_([self.test_code, self.test_code2, self.test_code3]))
        self.assertEqual(max_code, self.test_code3)

        # 测试 min
        min_code = await query_aggregate("min", MdlAllStock, "code", MdlAllStock.code.in_([self.test_code, self.test_code2, self.test_code3]))
        self.assertEqual(min_code, self.test_code)

        # 清理
        await delete_records(MdlAllStock, MdlAllStock.code.in_([self.test_code, self.test_code2, self.test_code3]))

    async def test_query_values(self):
        """测试 query_values 查询多个字段"""
        # 清理测试数据
        await delete_records(MdlAllStock, MdlAllStock.code.in_([self.test_code, self.test_code2]))

        data_list = [
            {
                "code": self.test_code,
                "name": "平安银行",
                "typekind": "stock",
                "setup_date": "1991-04-03",
                "quit_date": None
            },
            {
                "code": self.test_code2,
                "name": "万科A",
                "typekind": "stock",
                "setup_date": "1991-01-29",
                "quit_date": None
            }
        ]
        await upsert_many(MdlAllStock, data_list, unique_fields=["code"])

        # 查询指定字段
        results = await query_values(MdlAllStock, [MdlAllStock.code, MdlAllStock.name], MdlAllStock.code.in_([self.test_code, self.test_code2]))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0][0], self.test_code)
        self.assertEqual(results[0][1], "平安银行")
        self.assertEqual(results[1][0], self.test_code2)
        self.assertEqual(results[1][1], "万科A")

        # 带条件查询
        results = await query_values(
            MdlAllStock,
            MdlAllStock.name,
            MdlAllStock.code == self.test_code
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], "平安银行")

        # 清理
        await delete_records(MdlAllStock, MdlAllStock.code.in_([self.test_code, self.test_code2]))

    async def test_delete_records(self):
        """测试 delete_records 删除记录"""
        # 清理测试数据
        await delete_records(MdlAllStock, MdlAllStock.code.in_([self.test_code, self.test_code2, self.test_code3]))

        data_list = [
            {
                "code": self.test_code,
                "name": "平安银行",
                "typekind": "stock",
                "setup_date": "1991-04-03",
                "quit_date": None
            },
            {
                "code": self.test_code2,
                "name": "万科A",
                "typekind": "stock",
                "setup_date": "1991-01-29",
                "quit_date": None
            },
            {
                "code": self.test_code3,
                "name": "浦发银行",
                "typekind": "bond",
                "setup_date": "1999-11-10",
                "quit_date": None
            }
        ]
        await upsert_many(MdlAllStock, data_list, unique_fields=["code"])

        # 删除单条记录
        deleted_count = await delete_records(MdlAllStock, MdlAllStock.code == self.test_code)
        self.assertEqual(deleted_count, 1)

        # 验证删除成功
        result = await query_one_value(MdlAllStock, "name", MdlAllStock.code == self.test_code)
        self.assertIsNone(result)

        # 删除多条记录（typekind == "stock" 且在测试范围内）
        deleted_count = await delete_records(MdlAllStock, MdlAllStock.typekind == "stock", MdlAllStock.code.in_([self.test_code2, self.test_code3]))
        self.assertEqual(deleted_count, 1)  # 只剩 000002

        # 验证剩余记录
        count = await query_aggregate("count", MdlAllStock, "code", MdlAllStock.code.in_([self.test_code2, self.test_code3]))
        self.assertEqual(count, 1)

        # 删除剩余测试记录
        deleted_count = await delete_records(MdlAllStock, MdlAllStock.code.in_([self.test_code2, self.test_code3]))
        self.assertEqual(deleted_count, 1)


if __name__ == '__main__':
    unittest.main()
    # suite = unittest.TestSuite()
    # suite.addTest(TestDatabaseOperations('test_query_values'))
    # unittest.TextTestRunner().run(suite)
