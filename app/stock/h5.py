import os
import h5py
import numpy as np
from typing import Union
from functools import lru_cache
from datetime import datetime
from stockrt.sources.rtbase import rtbase
from app.lofig import Config, logger
from app.hu import classproperty, FixedPointConverter


# https://github.com/JumuFENG/hikyuu/blob/master/hikyuu/data/common_h5.py
# https://github.com/JumuFENG/hikyuu/blob/master/hikyuu/data/pytdx_to_h5.py

class TimeConverter:
    def __init__(self, time_fmt='%Y-%m-%d %H:%M:%S'):
        self.time_fmt = time_fmt

    def time_to_int(self, date_strs: np.ndarray):
        """将字符串数组转为int时间（如20231129093000）"""
        if isinstance(date_strs, str):
            date_strs = np.array([date_strs])
        date_strs = np.where(np.char.str_len(date_strs) == 10, np.char.add(date_strs, ' 00:00:00'), date_strs)
        date_strs = np.where(np.char.str_len(date_strs) == 16, np.char.add(date_strs, ':00'), date_strs)
        dt = np.vectorize(lambda s: int(datetime.strptime(s, self.time_fmt).strftime('%Y%m%d%H%M%S')))
        return dt(date_strs)

    def int_to_time(self, date_ints: np.ndarray):
        """将int时间数组转为字符串"""
        if np.isscalar(date_ints):
            date_ints = np.array([date_ints])
        dt = np.vectorize(lambda i: datetime.strptime(str(i), '%Y%m%d%H%M%S').strftime(self.time_fmt))
        return dt(date_ints)


class H5Storage:
    hdf5_compress_level = 9
    saved_dtype = {}
    saved_kline_types = [101]
    price_cols = []
    amount_cols = []
    @classproperty
    def price_converter(cls):
        return FixedPointConverter(4)

    @classproperty
    def volume_converter(cls):
        return FixedPointConverter(0)

    @classproperty
    def date_converter(cls):
        return TimeConverter()

    @classmethod
    def prepare_data(cls, df: np.ndarray):
        """准备整数化数据"""
        dtypes = [(col, dtype) for col, dtype in cls.saved_dtype.items() if col in df.dtype.names]
        for col in df.dtype.names:
            if col not in cls.saved_dtype:
                logger.warning(f'Unknown column {col} in kline data, will be ignored!')
        df_int = np.empty(len(df), dtype=dtypes)

        if 'time' in df.dtype.names:
            df_int['time'] = cls.date_converter.time_to_int(df['time'])

        for col in cls.price_cols:
            if col in df.dtype.names:
                df_int[col] = cls.price_converter.float_to_int(df[col])

        for col in cls.amount_cols:
            if col in df.dtype.names:
                df_int[col] = cls.volume_converter.float_to_int(df[col], dtype='int64')

        return df_int

    @classmethod
    def restore_data(cls, df_int):
        """还原为浮点数"""
        dtdict = {'time': 'U20','volume': 'int64'}
        dtypes = [(col, dtdict.get(col, 'float64')) for col in df_int.dtype.names]
        df_float = np.empty(len(df_int), dtype=dtypes)

        if 'time' in df_int.dtype.names:
            df_float['time'] = cls.date_converter.int_to_time(df_int['time'])

        for col in cls.price_cols:
            if col not in df_int.dtype.names:
                continue
            df_float[col] = cls.price_converter.int_to_float(df_int[col])

        for col in cls.amount_cols:
            if col not in df_int.dtype.names:
                continue
            df_float[col] = cls.volume_converter.int_to_float(df_int[col])

        return df_float

    @classmethod
    def h5_saved_path(cls, fcode: str, kline_type: int=101) -> str:
        histroy_dir = Config.h5_history_dir()
        h5_file_post = f'{kline_type}min' if kline_type < 100 else 'day'
        return f"{histroy_dir}/{fcode[:2].lower()}_{h5_file_post}.h5"

    @classmethod
    def h5_saved_group(cls, kline_type: int=101) -> str:
        return 'data'

    @classmethod
    def save_dataset(cls, fcode: str, ds_data: np.ndarray = None, kline_type: Union[int, str] = 101):
        """新数据连续且按时间排序"""
        if ds_data is None or len(ds_data) == 0:
            return

        dset_int = cls.prepare_data(ds_data)
        file_path = cls.h5_saved_path(fcode, kline_type)
        group = cls.h5_saved_group(kline_type)
        ds_name = fcode

        with h5py.File(file_path, 'a') as f:
            if group not in f:
                grp = f.create_group(group)
            else:
                grp = f[group]

            if ds_name not in grp:
                grp.create_dataset(
                    ds_name, data=dset_int, maxshape=(None,), compression='gzip', compression_opts=cls.hdf5_compress_level)
                return

            dset = grp[ds_name]

            if len(dset) == 0:
                dset.resize((len(dset_int),))
                dset[:] = dset_int
                return

            last_saved_time = dset[-1]['time']
            new_first_time = dset_int[0]['time']

            newer_mask = dset_int['time'] >= last_saved_time
            newer_data = dset_int[newer_mask]
            new_size = len(newer_data) if new_first_time > last_saved_time else len(newer_data) - 1
            if new_size > 0:
                dset.resize((len(dset) + new_size,))
            dset[-len(newer_data):] = newer_data

    @classmethod
    @lru_cache(maxsize=2000)
    def read_saved_data(cls, fcode: str, length: int=0, kline_type: int=101) -> np.ndarray:
        '''从HDF5文件中读取数据'''
        if kline_type not in cls.saved_kline_types:
            logger.error(f'kline_type {kline_type} not in (1, 5, 15, 101, 102, 103, 104, 105, 106)')
            return
        file_path = cls.h5_saved_path(fcode, kline_type)
        if not os.path.isfile(file_path):
            return
        group = cls.h5_saved_group(kline_type)
        with h5py.File(file_path, 'r') as f:
            if group not in f or fcode not in f[group]:
                return
            final_len = length if length > 0 else len(f[group][fcode])
            klines = cls.restore_data(f[group][fcode][-final_len:])
            if kline_type > 100 and kline_type % 15 != 0:
                klines['time'] = np.vectorize(lambda x: x.split(' ')[0])(klines['time'])
            return klines

    @classmethod
    def max_date(cls, fcode: str, kline_type: int=101):
        """获取最大日期"""
        file_path = cls.h5_saved_path(fcode, kline_type)
        if not os.path.isfile(file_path):
            return ''
        group = cls.h5_saved_group(kline_type)
        with h5py.File(file_path, 'r') as f:
            if group not in f or fcode not in f[group]:
                return ''
            dset = f[group][fcode]
            if len(dset) == 0:
                return ''
            last_time_str = cls.date_converter.int_to_time(np.array([dset[-1]['time']]))[0]
            if kline_type > 100 and kline_type % 15 != 0:
                last_time_str = last_time_str.split(' ')[0]
            return last_time_str

    @classmethod
    def delete_dataset(cls, fcode: str, kline_type: int=101):
        """删除K线数据"""
        file_path = cls.h5_saved_path(fcode, kline_type)
        if not os.path.isfile(file_path):
            return
        with h5py.File(file_path, 'a') as f:
            group = cls.h5_saved_group(kline_type)
            if group in f and fcode in f[group]:
                del f[group][fcode]

class KLineStorage(H5Storage):
    saved_dtype = {
            'time': 'int64',
            'open': 'int32',
            'close': 'int32',
            'high': 'int32',
            'low': 'int32',
            'volume': 'int64',
            'amount': 'int64',
            'change': 'int32',
            'change_px': 'int32',
            'amplitude': 'int32',
            'turnover': 'int32'
        }

    hdf5_compress_level = 9
    saved_kline_types = [1, 5, 15, 101, 102, 103, 104, 105, 106]
    price_cols = ['open', 'high', 'low', 'close', 'change', 'change_px', 'amplitude', 'turnover']
    amount_cols = ['amount', 'volume']

    @staticmethod
    def default_kline_cache_size(kltype: int=101) -> int:
        if not isinstance(kltype, int):
            logger.error(f'kltype must be int here!')
            raise ValueError('invalid kltype')
        return {1: 240, 5: 96, 15: 240, 30: 120, 60: 60, 120: 30, 101: 200, 102: 100, 103: 64, 104: 32, 105: 32, 106: 32}[kltype]

    @classmethod
    def h5_saved_group(cls, kline_type: int=101) -> str:
        groups = {
            102: 'week', 103: 'month', 104: 'quarter', 105: 'halfyear', 106: 'year'
        }
        return groups.get(kline_type, 'data')

    @staticmethod
    def extend_kline_data(klines: np.ndarray, ktype: int, out_type: int) -> np.ndarray:
        """
        合并K线数据
        """
        if ktype == out_type:
            return klines

        window = int(out_type / ktype)
        n_out = len(klines) // window
        if n_out == 0:
            return np.array([], dtype=klines.dtype)

        klines = klines[-n_out*window:]  # 保证整除
        klines_reshaped = klines.reshape(n_out, window)

        merged = np.empty(n_out, dtype=klines.dtype)
        merged['time'] = klines_reshaped[:, -1]['time']
        merged['open'] = klines_reshaped[:, 0]['open']
        merged['close'] = klines_reshaped[:, -1]['close']
        merged['high'] = np.max(klines_reshaped['high'], axis=1)
        merged['low'] = np.min(klines_reshaped['low'], axis=1)
        merged['volume'] = np.sum(klines_reshaped['volume'], axis=1)
        merged['amount'] = np.sum(klines_reshaped['amount'], axis=1)

        if 'change_px' in klines.dtype.names and 'change' in klines.dtype.names:
            prev_closes = np.empty(n_out)
            prev_closes[0] = klines_reshaped[0, 0]['close'] - klines_reshaped[0, 0]['change_px']  # 第一根的前收盘
            prev_closes[1:] = merged['close'][:-1]  # 后续K线的前收盘就是前一根的收盘

            merged['change_px'] = merged['close'] - prev_closes
            with np.errstate(divide='ignore', invalid='ignore'):
                merged['change'] = np.where(prev_closes != 0, merged['change_px'] / prev_closes, 0)

        return merged

    @classmethod
    def read_kline_data(cls, fcode: str, kline_type: Union[int, str]=101, length: int=0):
        kline_type = rtbase.to_int_kltype(kline_type)
        if kline_type not in cls.saved_kline_types:
            if kline_type % 15 == 0:
                saved_klines = cls.read_saved_data(fcode, length*kline_type/15, 15)
                return cls.extend_kline_data(saved_klines, 15, kline_type)
            if kline_type % 5 == 0:
                saved_klines = cls.read_saved_data(fcode, length*kline_type/5, 5)
                return cls.extend_kline_data(saved_klines, 5, kline_type)
            saved_klines = cls.read_saved_data(fcode, length*kline_type, 1)
            return cls.extend_kline_data(saved_klines, 1, kline_type)
        return cls.read_saved_data(fcode, length, kline_type)

class KLineTsStorage(KLineStorage):
    @classmethod
    def h5_saved_path(cls, fcode: str=None, kline_type: int=101) -> str:
        histroy_dir = Config.h5_history_dir()
        return f"{histroy_dir}/ts/klines.h5"

    @classmethod
    def h5_saved_group(cls, kline_type: int=101) -> str:
        groups = {
            101:"day", 102: 'week', 103: 'month', 104: 'quarter', 105: 'halfyear', 106: 'year'
        }
        return f"{kline_type}min" if kline_type < 100 else groups.get(kline_type, 'data')

class FflowStorage(H5Storage):
    saved_dtype = {
            'time': 'int64',
            'main': 'int64',
            'small': 'int64',
            'middle': 'int64',
            'big': 'int64',
            'super': 'int64',
            'mainp': 'int32',
            'smallp': 'int32',
            'middlep': 'int32',
            'bigp': 'int32',
            'superp': 'int32',
        }

    price_cols = ['mainp', 'smallp', 'middlep', 'bigp', 'superp']
    amount_cols = ['main', 'small', 'middle', 'big', 'super']
    saved_kline_types = [101]

    @classmethod
    def h5_saved_path(cls, fcode: str=None, kline_type: int=101) -> str:
        histroy_dir = Config.h5_history_dir()
        return f"{histroy_dir}/fflow.h5"

    @classmethod
    def save_fflow(cls, code, fflow):
        dtypes = [
            ('time', 'U10'), ('main', 'int64'), ('small', 'int64'), ('middle', 'int64'), ('big', 'int64'), ('super', 'int64'),
            ('mainp', 'float'), ('smallp', 'float'), ('middlep', 'float'), ('bigp', 'float'), ('superp', 'float')]
        values = np.array([(
            f[0], int(float(f[1])), int(float(f[2])), int(float(f[3])), int(float(f[4])), int(float(f[5])),
            float(f[6])/100, float(f[7])/100, float(f[8])/100, float(f[9])/100, float(f[10])/100)
            for f in fflow],
            dtype=dtypes
        )
        cls.save_dataset(code, np.array(values, dtype=cls.saved_dtype))
