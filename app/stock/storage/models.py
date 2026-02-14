"""股票数据存储模型定义"""
from sqlalchemy import Column, String, Integer, Float, Index, Table, MetaData


# 创建独立的元数据对象
KLineMetaData = MetaData()
FflowMetaData = MetaData()
TransactionMetaData = MetaData()


def create_kline_table(table_name):
    """动态创建K线数据表"""
    return Table(
        table_name,
        KLineMetaData,
        Column('time', String(20), primary_key=True, comment="时间"),
        Column('open', Float, nullable=False, default=0.0, comment="开盘价"),
        Column('close', Float, nullable=False, default=0.0, comment="收盘价"),
        Column('high', Float, nullable=False, default=0.0, comment="最高价"),
        Column('low', Float, nullable=False, default=0.0, comment="最低价"),
        Column('volume', Integer, nullable=False, default=0, comment="成交量"),
        Column('amount', Float, nullable=False, default=0.0, comment="成交额"),
        Column('change', Float, nullable=False, default=0.0, comment="涨跌额"),
        Column('change_px', Float, nullable=False, default=0.0, comment="涨跌幅"),
        Column('amplitude', Float, nullable=False, default=0.0, comment="振幅"),
        Column('turnover', Float, nullable=False, default=0.0, comment="换手率"),
        Index(f'idx_{table_name}_time', 'time'),
    )


def create_fflow_table(table_name):
    """动态创建资金流数据表"""
    return Table(
        table_name,
        FflowMetaData,
        Column('time', String(20), primary_key=True, comment="时间"),
        Column('main', Integer, nullable=False, default=0, comment="主力净流入"),
        Column('small', Integer, nullable=False, default=0, comment="小单净流入"),
        Column('middle', Integer, nullable=False, default=0, comment="中单净流入"),
        Column('big', Integer, nullable=False, default=0, comment="大单净流入"),
        Column('super', Integer, nullable=False, default=0, comment="超大单净流入"),
        Column('mainp', Float, nullable=False, default=0.0, comment="主力净流入占比"),
        Column('smallp', Float, nullable=False, default=0.0, comment="小单净流入占比"),
        Column('middlep', Float, nullable=False, default=0.0, comment="中单净流入占比"),
        Column('bigp', Float, nullable=False, default=0.0, comment="大单净流入占比"),
        Column('superp', Float, nullable=False, default=0.0, comment="超大单净流入占比"),
        Index(f'idx_{table_name}_time', 'time'),
    )


def create_transaction_table(table_name):
    """动态创建交易数据表"""
    return Table(
        table_name,
        TransactionMetaData,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('time', String(20), nullable=False, comment="成交时间"),
        Column('price', Float, nullable=False, default=0.0, comment="成交价"),
        Column('volume', Integer, nullable=False, default=0, comment="成交量"),
        Column('num', Integer, nullable=False, default=0, comment="成交笔数"),
        Column('bs', Integer, nullable=False, default=0, comment="买卖方向：1:buy, 2:sell, 0:中性/不明, 8:集合竞价"),
        Index(f'idx_{table_name}_time', 'time')
    )
