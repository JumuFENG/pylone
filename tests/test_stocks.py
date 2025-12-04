#!/usr/bin/env python3
import unittest
from unittest.mock import Mock, ANY, patch, AsyncMock
import numpy as np
import json
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.lofig import Config
from app.stock.manager import AllStocks
from app.stock.history import Khistory as khis, FflowHistory as fhis

class TestStocks(unittest.IsolatedAsyncioTestCase):
    async def test_load_index(self):
        await AllStocks.remove('sz399001')

    async def test_get_all_stock_info(self):
        stocks = await AllStocks.read_all()
        print(f'Total stocks: {len(stocks)}')
        for stock in stocks:
            print(f'Code: {stock.code}, Name: {stock.name}, Type: {stock.typekind}')

    async def test_update_kline_data(self):
        await AllStocks.update_kline_data(sectype='Index')
        # AllStocks.update_klines_by_code(['sh000001'])

    async def test_read_kline(self):
        data = await khis.read_kline('sh000001', 'd')
        print(json.dumps(data.tolist(), indent=4))

    async def test_np_convert(self):
        from app.stock.h5 import KLineStorage
        kls = KLineStorage(2, 0)
        arr = [['2023-10-01 00:00', 1.00, 1.05, .95, 1.02, 1000],
               ['2023-10-02 15:00', 1.02, 1.08, 1.00, 1.07, 1200],
               ['2023-10-03 09:30', 1.07, 1.10, 1.05, 1.09, 1500],
               ['2023-10-04 11:30:30', 1.09, 1.15, 1.08, 1.13, 1300],
               ['2023-10-05', 1.13, 1.20, 1.10, 1.18, 1400]]
        dtypes = [('time', 'U20'), ('open', 'float64'), ('high', 'float64'), ('low', 'float64'), ('close', 'float64'), ('volume', 'int64')]
        odata = kls.prepare_data(np.array([tuple(r) for r in arr], dtype=dtypes))
        print(odata.dtype)
        rdata = kls.restore_data(odata)
        print(rdata.dtype)

    @patch('app.stock.manager.srt')
    @patch('app.stock.manager.khis')
    @patch('app.stock.manager.fhis')
    @patch('app.stock.manager.TradingDate')
    async def test_update_stock_daily_kline_and_fflow(self, mock_trading_date, mock_fhis, mock_khis, mock_srt):
        # Mock TradingDate
        mock_trading_date.max_trading_date.return_value = '2024-12-04'
        mock_trading_date.prev_trading_date.return_value = '2024-12-03'

        # Mock srt.stock_list
        mock_srt.stock_list.return_value = {
            'all': [
                {
                    'code': 'sh600000',
                    'name': '浦发银行',
                    'time': '2024-12-04',
                    'open': 10.0,
                    'high': 10.5,
                    'low': 9.8,
                    'close': 10.2,
                    'lclose': 10.0,
                    'volume': 1000000,
                    'amount': 10200000,
                    'change': 2.0,
                    'change_px': 0.2,
                    'amplitude': 0.07,
                    'main': 5000000,
                    'small': 1000000,
                    'middle': 2000000,
                    'big': 3000000,
                    'super': 4000000,
                    'mainp': 50.0,
                    'smallp': 10.0,
                    'middlep': 20.0,
                    'bigp': 30.0,
                    'superp': 40.0
                },
                {
                    'code': 'sz000001',
                    'name': '平安银行',
                    'time': '2024-12-04',
                    'open': 15.0,
                    'high': 15.8,
                    'low': 14.5,
                    'close': 15.5,
                    'lclose': 15.0,
                    'volume': 2000000,
                    'amount': 31000000,
                    'change': 3.33,
                    'change_px': 0.5
                },
                {
                    'code': 'sz000002',
                    'name': '万科A',
                    'time': '2024-12-04',
                    'open': 8.0,
                    'high': 8.3,
                    'low': 7.9,
                    'close': 8.1,
                    'lclose': 8.0,
                    'volume': 3000000,
                    'amount': 24300000,
                    'change': 1.25,
                    'change_px': 0.1
                }
            ]
        }

        # Mock khis.max_date - 第三只股票数据不连续
        def mock_max_date(code, period):
            if code == '000002':
                return '2024-11-30'  # 数据不连续
            return '2024-12-03'

        mock_khis.max_date.side_effect = mock_max_date
        mock_khis.save_kline = Mock()

        # Mock fhis.max_date
        mock_fhis.max_date.side_effect = lambda code: '2024-12-03'
        mock_fhis.save_fflow = Mock()

        # 执行测试
        unconfirmed = await AllStocks.update_stock_daily_kline_and_fflow()

        # 验证 srt.stock_list 被调用
        mock_srt.stock_list.assert_called_once()

        # 验证 khis.save_kline 被调用了2次（只有连续的两只股票）
        self.assertEqual(mock_khis.save_kline.call_count, 2)

        # 验证第一只股票的K线数据
        call_args = mock_khis.save_kline.call_args_list[0]
        self.assertEqual(call_args[0][0], '600000')  # code
        self.assertEqual(call_args[0][1], 'd')  # period
        kline_data = call_args[0][2]
        self.assertEqual(kline_data[0]['time'], '2024-12-04')
        self.assertEqual(kline_data[0]['close'], 10.2)

        # 验证 fhis.save_fflow 只被调用1次（只有第一只股票有资金流数据）
        self.assertEqual(mock_fhis.save_fflow.call_count, 1)
        fflow_call_args = mock_fhis.save_fflow.call_args_list[0]
        self.assertEqual(fflow_call_args[0][0], '600000')

        # 验证返回的unconfirmed列表包含数据不连续的股票
        self.assertEqual(unconfirmed, ['000002'])

if __name__ == '__main__':
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(TestStocks('test_update_kline_data'))
    unittest.TextTestRunner().run(suite)
