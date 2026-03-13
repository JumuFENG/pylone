#!/usr/bin/env python3
import unittest
from unittest.mock import Mock, ANY, patch
import json
import sys
import os
import numpy as np
import h5py

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.lofig import Config
from app.stock.storage.h5 import KLineStorage as kls, TransactionStorage as sts

class TestH5file(unittest.TestCase):
    def test_get_kline(self):
        kline = kls.read_kline_data('BJ920950', 'day', length=30)
        self.assertIsNotNone(kline)

    @unittest.skip("skip, file will be overwritten")
    def test_save_transaction(self):
        trans = [['09:15', 11.54, 0, 0, 8], ['09:15', 11.52, 0, 0, 8], ['09:25', 11.51, 211100, 176, 2],]

        trans = [['2025-12-18 ' + t[0]] + t[1:] for t in trans]
        sts.save_dataset('sz000001', np.array([tuple(t) for t in trans], dtype=[('time', 'U20'), ('price', 'float'), ('volume', 'int64'), ('num', 'int32'), ('bs', 'int32')]))

    def test_read_transaction(self):
        trans2 = sts.read_saved_data('sz000001', length=5000)
        t_count2 = {}
        for t in trans2.tolist():
            k = t[0].split(' ')[1]
            t_count2[k] = t_count2.get(k, 0) + 1

    def test_remove_data_later_than(self):
        """Verify that slicing with a boolean mask works after resizing."""
        import tempfile
        # create temporary h5 file and populate with simple kline data
        tmp = tempfile.NamedTemporaryFile(suffix='.h5', delete=False)
        tmp_path = tmp.name
        tmp.close()

        # override path resolution to use our temp file
        with patch.object(kls, 'h5_saved_path', return_value=tmp_path):
            # prepare a small dataset where time values are simple integers
            # build a numpy dtype from the saved_dtype dict
            dtype = [(k, v) for k, v in kls.saved_dtype.items()]
            arr = np.zeros(5, dtype=dtype)
            arr['time'] = np.arange(1, 6)
            with h5py.File(tmp_path, 'a') as f:
                grp = f.create_group('data')
                grp.create_dataset('TEST', data=arr, maxshape=(None,))

            # patch the date_converter to simply convert string to int
            with patch.object(kls.date_converter, 'time_to_int', lambda s: int(s)):
                # remove entries later than '3', i.e. keep 1,2,3
                kls.remove_data_later_than('TEST', 101, date_str='3')

            # verify dataset now only contains expected rows
            with h5py.File(tmp_path, 'r') as f:
                result = f['data']['TEST'][:]
            self.assertTrue(np.array_equal(result['time'], np.array([1, 2, 3])))
        os.remove(tmp_path)


if __name__ == '__main__':
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(TestH5file('test_read_transaction'))
    unittest.TextTestRunner().run(suite)
