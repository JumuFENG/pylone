from pydantic import BaseModel, Field
from typing import Optional, List


# ========== 买卖条件模型 ==========

class BuyCondition(BaseModel):
    type: str = Field(default="ma_cross")
    fast_period: int = Field(default=5)
    slow_period: int = Field(default=10)
    direction: str = Field(default="up")
    period: Optional[int] = Field(default=20, description="周期数(用于突破等)")
    price_type: str = Field(default="next_open", description="价格类型: close(收盘价), next_open(次日开盘), high(当日最高), low(当日最低), trigger(触发价)")
    formula: Optional[str] = Field(default="", description="公式字符串")


class SellCondition(BaseModel):
    type: str
    fast_period: Optional[int] = None
    slow_period: Optional[int] = None
    direction: Optional[str] = None
    pct: Optional[float] = None
    days: Optional[int] = None
    period: Optional[int] = None
    price_type: str = Field(default="next_open", description="价格类型: close(收盘价), next_open(次日开盘), high(当日最高), low(当日最低), trigger(触发价)")
    formula: Optional[str] = Field(default="", description="公式字符串")


class SignalRequest(BaseModel):
    start_date: str
    end_date: str
    stock_pool: str = Field(default="all")
    buy_condition: BuyCondition
    sell_conditions: List[SellCondition]


class SignalItem(BaseModel):
    code: str
    name: str
    reason: str
    price: Optional[float] = None


class DailySignals(BaseModel):
    buy: List[SignalItem] = Field(default_factory=list)
    sell: List[SignalItem] = Field(default_factory=list)


class SignalResponse(BaseModel):
    dates: List[str]
    signals: dict


class FormulaCondition(BaseModel):
    formula: str
    name: str


class FormulaSignalRequest(BaseModel):
    start_date: str
    end_date: str
    stock_pool: str = Field(default="all")
    buy_conditions: List[BuyCondition]
    sell_conditions: List[SellCondition]


# ========== 回测参数模型 (保留旧版兼容) ==========

class BacktestParams(BaseModel):
    stock_pool: str = Field(default="all")
    strategy: str = Field(default="zt0")
    start_date: str
    end_date: str
    init_capital: float = Field(default=100000)
    fee_rate: float = Field(default=0.0003)
    slippage: float = Field(default=0.001)
    stop_loss: Optional[float] = Field(default=-5)
    stop_gain: Optional[float] = Field(default=20)
    max_positions: int = Field(default=5)
    hold_days: int = Field(default=3)
    buy_limit: int = Field(default=1)


class TradeRecord(BaseModel):
    date: str
    code: str
    name: str
    action: str
    price: float
    quantity: int
    amount: float
    fee: float
    reason: str
    profit: Optional[float] = None
    profit_pct: Optional[float] = None


class Position(BaseModel):
    code: str
    name: str
    buy_date: str
    buy_price: float
    quantity: int
    current_price: float = 0
    profit: float = 0
    profit_pct: float = 0


class DailyStats(BaseModel):
    date: str
    capital: float
    positions: int
    trades: int
    daily_profit: float


class BacktestResult(BaseModel):
    params: BacktestParams
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0
    total_profit: float = 0
    total_profit_pct: float = 0
    max_drawdown: float = 0
    avg_profit: float = 0
    avg_hold_days: float = 0
    daily_stats: List[DailyStats] = []
    trades: List[TradeRecord] = []
    final_positions: List[Position] = []


class BacktestStatus(BaseModel):
    task_id: str
    status: str
    progress: int = 0
    message: str = ""
