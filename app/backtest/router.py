from fastapi import APIRouter, HTTPException, Query
from .schemas import (
    BacktestParams, BacktestResult, 
    SignalRequest, SignalResponse
)
from .engine import run_backtest
from .formula_generator import generate_formula_signals
from .formula_engine import FormulaSignalDetector
from app.stock.history import Khistory as khis


router = APIRouter(
    prefix="/api/backtest",
    tags=["backtest"],
    responses={404: {"description": "Not found"}},
)


@router.post("/run", response_model=BacktestResult)
async def start_backtest(params: BacktestParams):
    try:
        result = await run_backtest(params)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/signals", response_model=SignalResponse)
async def get_signals(request: SignalRequest):
    try:
        result = await generate_formula_signals(request)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategies")
async def get_strategies():
    return {
        "strategies": [
            {"id": "zt0", "name": "涨停股选股", "description": "选择当日涨停股"},
            {"id": "zt1", "name": "涨停1板", "description": "选择首板涨停股"},
            {"id": "dt", "name": "跌停股选股", "description": "选择当日跌停股"},
            {"id": "dt3", "name": "跌停3板", "description": "选择连续跌停股"},
            {"id": "eu", "name": "三连阳", "description": "选择连续上涨股"},
        ]
    }


@router.get("/stock-pools")
async def get_stock_pools():
    return {
        "pools": [
            {"id": "all", "name": "全部股票"},
            {"id": "sh", "name": "上海主板"},
            {"id": "sz", "name": "深圳主板"},
            {"id": "cy", "name": "创业板"},
            {"id": "ke", "name": "科创板"},
            {"id": "bj", "name": "北交所"},
        ]
    }


@router.get("/conditions")
async def get_conditions():
    return {
        "buy_conditions": [
            {"id": "ma_cross", "name": "MA均线交叉", "params": ["fast_period", "slow_period", "direction"]},
            {"id": "consecutive_up", "name": "连续上涨", "params": ["period"]},
            {"id": "consecutive_down", "name": "连续下跌", "params": ["period"]},
            {"id": "break_high", "name": "突破高点", "params": ["period"]},
            {"id": "break_low", "name": "跌破低点", "params": ["period"]},
            {"id": "volume_spike", "name": "量能突增", "params": ["period"]},
            {"id": "ma_angle", "name": "MA角度", "params": ["period", "min_angle"]},
        ],
        "sell_conditions": [
            {"id": "ma_cross", "name": "MA均线死叉", "params": ["fast_period", "slow_period"]},
            {"id": "profit_target", "name": "止盈", "params": ["pct"]},
            {"id": "stop_loss", "name": "止损", "params": ["pct"]},
            {"id": "hold_days", "name": "持仓天数", "params": ["days"]},
        ]
    }


@router.get("/validate")
async def validate_formula(
    code: str = Query(..., min_length=6, description="股票代码"),
    buy_formula: str = Query(..., description="买入公式"),
    sell_formula: str = Query("", description="卖出公式"),
    kltype: str = Query("101", description="K线类型: 101=日K, 102=周K, 103=月K"),
    length: int = Query(250, ge=10, le=1000, description="获取K线数量")
):
    try:
        klines = await khis.read_kline(code, kltype, 0, length)
        if not klines or len(klines) < 20:
            raise HTTPException(status_code=400, detail="K线数据不足")
        
        dates = [kl.get('time', '') for kl in klines]
        
        detector = FormulaSignalDetector(
            buy_formulas=[buy_formula] if buy_formula else [],
            sell_formulas=[sell_formula] if sell_formula else []
        )
        
        signals = detector.detect(klines, dates)
        
        buy_signals_list = []
        sell_signals_list = []
        
        for date, sigs in signals.items():
            for price in sigs.get('buy', []):
                buy_signals_list.append({
                    "date": date,
                    "price": price,
                    "reason": buy_formula
                })
            for price in sigs.get('sell', []):
                sell_signals_list.append({
                    "date": date,
                    "price": price,
                    "reason": sell_formula
                })
        
        buy_signals_list.sort(key=lambda x: x['date'])
        sell_signals_list.sort(key=lambda x: x['date'])
        
        return {
            "code": code,
            "klines": klines,
            "buy_signals": buy_signals_list,
            "sell_signals": sell_signals_list,
            "buy_count": len(buy_signals_list),
            "sell_count": len(sell_signals_list)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
