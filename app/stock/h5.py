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

class KLineStorage:
    saved_dtype = {
            'time': 'int64',
            'close': 'int32',
            'open': 'int32',
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

    @classproperty
    def price_converter(cls):
        return FixedPointConverter(4)
    
    @classproperty
    def volume_converter(cls):
        return FixedPointConverter(0)
    
    @classproperty
    def date_converter(cls):
        return TimeConverter()

    @staticmethod
    def default_kline_cache_size(kltype: int=101) -> int:
        if not isinstance(kltype, int):
            logger.error(f'kltype must be int here!')
            raise ValueError('invalid kltype')
        return {1: 240, 5: 96, 15: 240, 30: 120, 60: 60, 120: 30, 101: 200, 102: 100, 103: 64, 104: 32, 105: 32, 106: 32}[kltype]

    @staticmethod
    def h5_saved_path(fcode: str, kline_type: int=101) -> str:
        histroy_dir = Config.client_config().get('histroy_dir', './data')
        h5_file_post = f'{kline_type}min' if kline_type < 100 else 'day'
        return f"{histroy_dir}/{fcode[:2].lower()}_{h5_file_post}.h5"

    @staticmethod
    def h5_saved_group(kline_type: int=101) -> str:
        groups = {
            102: 'week', 103: 'month', 104: 'quarter', 105: 'halfyear', 106: 'year'
        }
        return groups.get(kline_type, 'data')

    @classmethod
    def prepare_data(cls, df: np.ndarray):
        """准备整数化数据"""
        df_int = np.empty(len(df), dtype=[(c, cls.saved_dtype[c] if c in cls.saved_dtype else 'int64') for c in df.dtype.names ])

        if 'time' in df.dtype.names:
            df_int['time'] = cls.date_converter.time_to_int(df['time'])

        price_cols = ['open', 'high', 'low', 'close', 'change', 'change_px', 'amplitude', 'turnover']
        for col in price_cols:
            if col in df.dtype.names:
                df_int[col] = cls.price_converter.float_to_int(df[col])

        if 'volume' in df.dtype.names:
            df_int['volume'] = cls.volume_converter.float_to_int(df['volume'])
        if 'amount' in df.dtype.names:
            df_int['amount'] = cls.volume_converter.float_to_int(df['amount'])

        return df_int

    @classmethod
    def restore_data(cls, df_int):
        """还原为浮点数"""
        dtdict = {'time': 'U20','volume': 'int64'}
        dtypes = [(col, dtdict.get(col, 'float64')) for col in df_int.dtype.names]
        df_float = np.empty(len(df_int), dtype=dtypes)

        if 'time' in df_int.dtype.names:
            df_float['time'] = cls.date_converter.int_to_time(df_int['time'])

        price_cols = ['open', 'high', 'low', 'close', 'change', 'change_px', 'amplitude', 'turnover']
        for col in price_cols:
            if col not in df_int.dtype.names:
                continue
            df_float[col] = cls.price_converter.int_to_float(df_int[col])

        if 'volume' in df_int.dtype.names:
            df_float['volume'] = cls.volume_converter.int_to_float(df_int['volume'])
        if 'amount' in df_int.dtype.names:
            df_float['amount'] = cls.volume_converter.int_to_float(df_int['amount'])

        return df_float

    @classmethod
    def save_kline_data(cls, fcode: str, kline_type: Union[int, str] = 101, klines: np.ndarray = None):
        """新数据连续且按时间排序"""
        if klines is None or len(klines) == 0:
            return

        klines_int = cls.prepare_data(klines)
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
                    ds_name, data=klines_int, maxshape=(None,), compression='gzip', compression_opts=cls.hdf5_compress_level)
                return

            dset = grp[ds_name]

            if len(dset) == 0:
                dset.resize((len(klines_int),))
                dset[:] = klines_int
                return

            last_saved_time = dset[-1]['time']
            new_first_time = klines_int[0]['time']

            newer_mask = klines_int['time'] >= last_saved_time
            newer_data = klines_int[newer_mask]
            new_size = len(newer_data) if new_first_time > last_saved_time else len(newer_data) - 1
            if new_size > 0:
                dset.resize((len(dset) + new_size,))
            dset[-len(newer_data):] = newer_data

    @classmethod
    @lru_cache(maxsize=2000)
    def read_saved_kline_data(cls, fcode: str, kline_type: int=101, length: int=0) -> np.ndarray:
        '''从HDF5文件中读取K线数据'''
        if kline_type not in cls.saved_kline_types:
            logger.error(f'kline_type {kline_type} not in (1, 5, 15, 101, 102, 103, 104, 105, 106)')
            return
        file_path = cls.h5_saved_path(fcode, kline_type)
        if not os.path.isfile(file_path):
            return
        group = cls.h5_saved_group(kline_type)
        with h5py.File(file_path, 'r') as f:
            final_len = length if length > 0 else len(f[group][fcode])
            klines = cls.restore_data(f[group][fcode][-final_len:])
            if kline_type > 100 and kline_type % 15 != 0:
                klines['time'] = np.vectorize(lambda x: x.split(' ')[0])(klines['time'])
            return klines

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
                saved_klines = cls.read_saved_kline_data(fcode, 15, length*kline_type/15)
                return cls.extend_kline_data(saved_klines, 15, kline_type)
            if kline_type % 5 == 0:
                saved_klines = cls.read_saved_kline_data(fcode, 5, length*kline_type/5)
                return cls.extend_kline_data(saved_klines, 5, kline_type)
            saved_klines = cls.read_saved_kline_data(fcode, 1, length*kline_type)
            return cls.extend_kline_data(saved_klines, 1, kline_type)
        return cls.read_saved_kline_data(fcode, kline_type, length)

def explore_hdf5(file_path):
    """探索HDF5文件结构"""
    def print_structure(name, obj):
        indent = name.count('/') * '  '
        if isinstance(obj, h5py.Dataset):
            # print(f"{indent}📊 Dataset: {name.split('/')[-1]} - Shape: {obj.shape} - Dtype: {obj.dtype}")
            return
        elif isinstance(obj, h5py.Group):
            print(f"{indent}📁 Group: {name.split('/')[-1]}")

        # 打印属性
        # if obj.attrs:
        #     for attr_name, attr_value in obj.attrs.items():
        #         print(f"{indent}  🏷️  {attr_name}: {attr_value}")

    with h5py.File(file_path, 'r') as f:
        f.visititems(print_structure)