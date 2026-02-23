import os
import h5py
import numpy as np
from typing import Union, List, Dict, Any
from functools import lru_cache
from datetime import datetime
import stockrt as srt
from app.lofig import Config, logger
from app.hu import classproperty


# https://github.com/JumuFENG/hikyuu/blob/master/hikyuu/data/common_h5.py
# https://github.com/JumuFENG/hikyuu/blob/master/hikyuu/data/pytdx_to_h5.py

class FixedPointConverter:
    def __init__(self, precision=4):
        self.precision = precision
        self.scale = 10 ** precision

    def float_to_int(self, float_array, dtype='int32'):
        """将浮点数转换为整数"""
        return (float_array * self.scale).astype(dtype)

    def int_to_float(self, int_array):
        """将整数转换回浮点数"""
        return int_array.astype('float64') / self.scale

class TimeConverter:
    def __init__(self, time_fmt='%Y-%m-%d %H:%M:%S'):
        self.time_fmt = time_fmt

    def time_to_int(self, date_strs: np.ndarray):
        """将字符串数组转为int时间（如20231129093000）"""
        if isinstance(date_strs, str):
            date_strs = np.array([date_strs])
        date_strs = np.where(np.char.str_len(date_strs) == 10, np.char.add(date_strs, ' 00:00:00'), date_strs)
        date_strs = np.where(np.char.str_len(date_strs) == 16, np.char.add(date_strs, ':00'), date_strs)
        fmt = self.time_fmt if '%S' in self.time_fmt else self.time_fmt + ':%S'
        dt = np.vectorize(lambda s: int(datetime.strptime(s, fmt).strftime('%Y%m%d%H%M%S')))
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
    restore_dtype = {'time': 'U20','volume': 'int64'}
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
    def numpy_to_list_of_dicts(cls, np_array: np.ndarray) -> list:
        """
        将numpy结构化数组转换为list-of-dict格式

        Args:
            np_array: numpy结构化数组

        Returns:
            list-of-dict格式的数据
        """
        if np_array is None or len(np_array) == 0:
            return []

        result = []
        for row in np_array:
            row_dict = {}
            for field_name in np_array.dtype.names:
                value = row[field_name]
                # 处理numpy类型，转换为Python原生类型
                if hasattr(value, 'item'):
                    value = value.item()
                row_dict[field_name] = value
            result.append(row_dict)

        return result

    @classmethod
    def list_of_dicts_to_numpy(cls, data_list: list, dtype_def: dict) -> np.ndarray:
        """
        将list-of-dict格式转换为numpy结构化数组

        Args:
            data_list: list-of-dict格式的数据
            dtype_def: 数据类型定义

        Returns:
            numpy结构化数组
        """
        if not data_list:
            # 构建numpy dtype
            dtype_list = []
            for col_name, col_type in dtype_def.items():
                if col_type == 'str':
                    numpy_type = 'U20'  # Unicode字符串
                elif col_type == 'float':
                    numpy_type = 'float64'
                elif col_type == 'int':
                    numpy_type = 'int64'
                else:
                    numpy_type = 'float64'
                dtype_list.append((col_name, numpy_type))

            return np.array([], dtype=dtype_list)

        # 转换数据
        result_data = []
        for row_dict in data_list:
            row = []
            for col_name, _ in dtype_def.items():
                value = row_dict.get(col_name)
                # 设置默认值
                if value is None:
                    if col_name in ['open', 'high', 'low', 'close', 'change', 'change_px', 'amplitude', 'turnover']:
                        value = 0.0
                    elif col_name in ['volume', 'amount']:
                        value = 0
                    else:
                        value = ""
                row.append(value)
            result_data.append(tuple(row))

        return np.array(result_data, dtype=[(col, dtype) for col, dtype in dtype_def.items()])

    @classproperty
    def known_cols(cls):
        return ['time'] + cls.price_cols + cls.amount_cols

    @classmethod
    def time_only_date(cls, kline_type: int=101):
        return kline_type > 100 and kline_type % 15 != 0

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

        for col in df.dtype.names:
            if col not in cls.saved_dtype:
                continue
            if col not in cls.known_cols:
                df_int[col] = df[col].astype(cls.saved_dtype[col])

        return df_int

    @classmethod
    def restore_data(cls, df_int):
        """还原为浮点数"""
        dtypes = [(col, cls.restore_dtype.get(col, 'float64')) for col in df_int.dtype.names]
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

        for col in df_int.dtype.names:
            if col not in cls.known_cols:
                df_float[col] = df_int[col]

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
    def save_dataset(cls, fcode: str, ds_data = None, kline_type: Union[int, str] = 101):
        """新数据连续且按时间排序"""
        if ds_data is None or len(ds_data) == 0:
            return

        # 处理数据格式：如果是list-of-dict，转换为numpy数组
        if isinstance(ds_data, list):
            dtypes = cls.saved_dtype.copy()
            dtypes.update(cls.restore_dtype)
            ds_data = cls.list_of_dicts_to_numpy(ds_data, dtypes)
        elif ds_data is None:
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
            last_time_count = (dset['time'] == last_saved_time).sum()
            new_size = len(newer_data) if new_first_time > last_saved_time else len(newer_data) - last_time_count
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
            if cls.time_only_date(kline_type):
                klines['time'] = np.vectorize(lambda x: x.split(' ')[0])(klines['time'])
            return klines

    @classmethod
    def _min_max_date(cls, max_or_min: bool, fcode: str,  kline_type: int=101):
        """获取最大/最小日期"""
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
            idx = -1 if max_or_min else 0
            fl_time_str = cls.date_converter.int_to_time(np.array([dset[idx]['time']]))[0]
            if kline_type and kline_type > 100 and kline_type % 15 != 0:
                fl_time_str = fl_time_str.split(' ')[0]
            return fl_time_str

    @classmethod
    def max_date(cls, fcode: str, kline_type: int=101):
        """获取最大/最小日期"""
        return cls._min_max_date(True, fcode, kline_type)

    @classmethod
    def min_date(cls, fcode: str, kline_type: int=101):
        """获取最小日期"""
        return cls._min_max_date(False, fcode, kline_type)

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
        kline_type = srt.to_int_kltype(kline_type)
        if kline_type not in cls.saved_kline_types:
            if kline_type % 15 == 0:
                saved_klines = cls.read_saved_data(fcode, length*kline_type/15, 15)
                extended_klines = cls.extend_kline_data(saved_klines, 15, kline_type)
                return cls.numpy_to_list_of_dicts(extended_klines)
            if kline_type % 5 == 0:
                saved_klines = cls.read_saved_data(fcode, length*kline_type/5, 5)
                extended_klines = cls.extend_kline_data(saved_klines, 5, kline_type)
                return cls.numpy_to_list_of_dicts(extended_klines)
            saved_klines = cls.read_saved_data(fcode, length*kline_type, 1)
            extended_klines = cls.extend_kline_data(saved_klines, 1, kline_type)
            return cls.numpy_to_list_of_dicts(extended_klines)
        klines = cls.read_saved_data(fcode, length, kline_type)
        return cls.numpy_to_list_of_dicts(klines)

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
        cls.save_dataset(code, values)

    @classmethod
    def read_fflow(cls, code, date=None, date1=None):
        fflow = cls.read_saved_data(code)
        if fflow is None:
            return []
        if date is not None:
            fflow = fflow[fflow['time'] >= date]
        if date1 is not None:
            fflow = fflow[fflow['time'] <= date1]
        return cls.numpy_to_list_of_dicts(fflow)


class TransactionStorage(H5Storage):
    saved_dtype = {
        'time': 'int64', # 成交时间
        'price': 'int32', # 成交价
        'volume': 'int64', # 成交量
        'num': 'int32', # 成交笔数
        'bs': 'u1', # 1: buy, 2: sell 0: 中性盘 / 不明, 8: 集合竞价
    }

    restore_dtype = {
        'time': 'U20', # 成交时间
        'price': 'float', # 成交价
        'volume': 'int64', # 成交量
        'num': 'int32', # 成交笔数
        'bs': 'int32'
    }

    price_cols = ['price']

    @classproperty
    def date_converter(cls):
        return TimeConverter('%Y-%m-%d %H:%M')

    @classmethod
    def time_only_date(cls, kline_type: int=101):
        return False

    @classmethod
    def _min_max_date(cls, max_or_min, fcode, kline_type = None):
        return super()._min_max_date(max_or_min, fcode, None)

    @classmethod
    def h5_saved_path(cls, fcode: str=None, kline_type: int=101) -> str:
        histroy_dir = Config.h5_history_dir()
        return f"{histroy_dir}/trans_{fcode[2:5]}.h5"

    @classmethod
    def read_transaction(cls, fcode: str, date=None, date1=None, limit: int = None) -> List[Dict[str, Any]]:
        data = cls.read_saved_data(fcode)
        if data is None:
            return []
        if date is not None:
            data = data[data['time'] >= date]
        if date1 is not None:
            data = data[data['time'] <= date1]
        if limit is not None:
            data = data[-limit:]
        return cls.numpy_to_list_of_dicts(data)
