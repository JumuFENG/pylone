#!/usr/bin/env python3
import unittest
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.stock.date import TradingDate


class TestTradingDate(unittest.TestCase):
    def setUp(self):
        TradingDate.holidays = ['2025-01-01','2025-01-28','2025-01-29','2025-01-30','2025-01-31','2025-02-03','2025-02-04','2025-04-04','2025-05-01','2025-05-02','2025-05-05']

    def test_is_holiday(self):
        self.assertTrue(TradingDate.is_holiday('2025-01-01')) 
        self.assertTrue(TradingDate.is_holiday('2025-01-28')) 
        self.assertTrue(TradingDate.is_holiday('2025-04-04')) 
        self.assertFalse(TradingDate.is_holiday('2025-12-01'))
        self.assertFalse(TradingDate.is_holiday('2025-12-02'))
        self.assertFalse(TradingDate.is_holiday('2025-12-05'))

    def test_is_trading_date(self):
        self.assertFalse(TradingDate.is_trading_date('2025-05-02')) 
        self.assertFalse(TradingDate.is_trading_date('2025-12-13')) 
        self.assertFalse(TradingDate.is_trading_date('2025-12-14')) 
        self.assertTrue(TradingDate.is_trading_date('2025-12-15'))
        self.assertTrue(TradingDate.is_trading_date('2025-12-19'))

    def test_calc_trading_days(self):
        self.assertEqual(TradingDate.calc_trading_days('2025-05-01', '2025-05-31'), 19)
        self.assertEqual(TradingDate.calc_trading_days('2025-11-01', '2025-11-30'), 20)

    def test_prev_trading_date(self):
        self.assertEqual(TradingDate.prev_trading_date('2025-12-21'), '2025-12-19')
        self.assertEqual(TradingDate.prev_trading_date('2025-12-20'), '2025-12-19')
        self.assertEqual(TradingDate.prev_trading_date('2025-12-19'), '2025-12-18')
        self.assertEqual(TradingDate.prev_trading_date('2025-12-15'), '2025-12-12')

    def test_next_trading_date(self):
        self.assertEqual(TradingDate.next_trading_date('2025-12-14'), '2025-12-15')
        self.assertEqual(TradingDate.next_trading_date('2025-12-13'), '2025-12-15')
        self.assertEqual(TradingDate.next_trading_date('2025-12-12'), '2025-12-15')
        self.assertEqual(TradingDate.next_trading_date('2025-12-11'), '2025-12-12')


if __name__ == '__main__':
    unittest.main()
