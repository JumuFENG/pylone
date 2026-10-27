import asyncio
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
from app.stock.date import TradingDate
from app.stock.history import Khistory
from app.stock.manager import AllStocks
from app.db import query_values
from app.stock.models import MdlAllStock
from .schemas import (
    SignalRequest, SignalResponse, SignalItem, DailySignals,
    BuyCondition, SellCondition
)
from .formula_engine import FormulaSignalDetector, parse_condition_to_formula


class FormulaSignalGenerator:
    def __init__(self, request: SignalRequest):
        self.request = request
        self.stock_names = {}
        self._all_stocks_cache = None
        self._positions = {}
        self._buy_formulas = []
        self._sell_formulas = []
        self._sell_conditions_dynamic = []
        self._init_formulas()
    
    def _init_formulas(self):
        buy_cond = self.request.buy_condition
        
        if hasattr(buy_cond, 'formula') and buy_cond.formula:
            self._buy_formulas.append(buy_cond.formula)
        else:
            formula = self._condition_to_formula(buy_cond)
            if formula:
                self._buy_formulas.append(formula)
        
        for cond in self.request.sell_conditions:
            # Dynamic conditions: stop_loss, profit_target, hold_days - don't convert to formulas
            if cond.type in ('stop_loss', 'profit_target', 'hold_days'):
                self._sell_conditions_dynamic.append(cond)
                continue
            
            # Formula-based conditions
            if hasattr(cond, 'formula') and cond.formula:
                self._sell_formulas.append(cond.formula)
            else:
                formula = self._condition_to_formula(cond)
                if formula:
                    self._sell_formulas.append(formula)
    
    def _condition_to_formula(self, cond) -> Tuple[str, str]:
        ctype = cond.type
        params = {}
        
        if hasattr(cond, 'fast_period') and cond.fast_period:
            params['fast'] = cond.fast_period
        if hasattr(cond, 'slow_period') and cond.slow_period:
            params['slow'] = cond.slow_period
        if hasattr(cond, 'period') and cond.period:
            params['period'] = cond.period
        if hasattr(cond, 'days') and cond.days:
            params['days'] = cond.days
        if hasattr(cond, 'pct') and cond.pct:
            params['pct'] = cond.pct
        
        direction = getattr(cond, 'direction', 'up')
        
        formula_map = {
            'ma_cross': {
                'up': (f"CROSS(MA(C,{params.get('fast', 5)}), MA(C,{params.get('slow', 10)}))", 
                       f"MA{params.get('fast', 5)}金叉MA{params.get('slow', 10)}"),
                'down': (f"CROSS(MA(C,{params.get('slow', 10)}), MA(C,{params.get('fast', 5)}))",
                        f"MA{params.get('fast', 5)}死叉MA{params.get('slow', 10)}"),
            },
            'consecutive_up': (
                f"EVERY(C>REF(C,1), {params.get('period', 2)})",
                f"连续{params.get('period', 2)}天上涨"
            ),
            'consecutive_down': (
                f"EVERY(C<REF(C,1), {params.get('period', 2)})",
                f"连续{params.get('period', 2)}天下跌"
            ),
            'break_high': (
                f"C>HHV(H,{params.get('period', 20)})",
                f"突破{params.get('period', 20)}日高点"
            ),
            'break_low': (
                f"C<LLV(L,{params.get('period', 20)})",
                f"跌破{params.get('period', 20)}日低点"
            ),
            'volume_spike': (
                f"V>HHV(V,{params.get('period', 20)})*2",
                f"量能突增{params.get('period', 20)}日"
            ),
            'ma_angle': (
                f"EMA(C,{params.get('fast', 5)})>REF(EMA(C,{params.get('fast', 5)}),1)",
                f"MA{params.get('fast', 5)}向上"
            ),
            'stop_loss': (
                f"C<REF(C,1)*{1+params.get('pct', -5)/100}",
                f"止损{params.get('pct', -5)}%"
            ),
            'profit_target': (
                f"C>REF(C,1)*{1+params.get('pct', 20)/100}",
                f"止盈{params.get('pct', 20)}%"
            ),
            'hold_days': (
                "TRUE",
                f"持仓{params.get('days', 3)}天"
            ),
        }
        
        if ctype == 'ma_cross':
            return formula_map.get('ma_cross', ('', '')).get(direction, ('', ''))
        
        return formula_map.get(ctype, ('', ''))
    
    async def run(self) -> SignalResponse:
        trading_dates = self._get_trading_dates()
        await self._load_stock_names()
        
        all_stocks = await self._get_all_stocks()
        
        signals_dict = defaultdict(lambda: {"buy": [], "sell": []})
        self._positions = {}
        
        detector = FormulaSignalDetector(self._buy_formulas, self._sell_formulas)
        
        for idx, code in enumerate(all_stocks):
            try:
                stock_signals = await self._check_stock_signals(code, trading_dates, detector)
                for date, buy_list, sell_list in stock_signals:
                    signals_dict[date]["buy"].extend(buy_list)
                    signals_dict[date]["sell"].extend(sell_list)
            except Exception:
                continue
            
            if idx % 50 == 0:
                await asyncio.sleep(0)
        
        signals = {}
        for date in trading_dates:
            if date in signals_dict:
                buy_list = signals_dict[date]["buy"]
                sell_list = signals_dict[date]["sell"]
                if buy_list or sell_list:
                    signals[date] = DailySignals(buy=buy_list, sell=sell_list)
        
        return SignalResponse(
            dates=trading_dates,
            signals=signals
        )
    
    def _get_trading_dates(self) -> List[str]:
        dates = []
        current = self.request.start_date
        while not TradingDate.is_trading_date(current):
            current = TradingDate.next_trading_date(current, 1)
            if current > self.request.end_date:
                break
        end = self.request.end_date
        while current <= end:
            dates.append(current)
            if current == TradingDate.max_trading_date():
                break
            current = TradingDate.next_trading_date(current, 1)
        return dates
    
    async def _load_stock_names(self):
        stocks = await AllStocks.read_all()
        self.stock_names = {s.code: s.name for s in stocks}
    
    async def _get_all_stocks(self) -> List[str]:
        if self._all_stocks_cache is not None:
            return self._all_stocks_cache
        
        pool = self.request.stock_pool
        if pool == 'sh':
            stocks = await query_values(MdlAllStock, [MdlAllStock.code], MdlAllStock.code.startswith('sh6'))
        elif pool == 'sz':
            stocks = await query_values(MdlAllStock, [MdlAllStock.code], MdlAllStock.code.startswith('sz00'))
        elif pool == 'cy':
            stocks = await query_values(MdlAllStock, [MdlAllStock.code], MdlAllStock.code.startswith('sz30'))
        elif pool == 'ke':
            stocks = await query_values(MdlAllStock, [MdlAllStock.code], MdlAllStock.code.startswith('sh68'))
        elif pool == 'bj':
            stocks = await query_values(MdlAllStock, [MdlAllStock.code], MdlAllStock.code.startswith('bj'))
        else:
            stocks = await query_values(MdlAllStock, [MdlAllStock.code])
            stocks = [s for s in stocks if s.typekind != 'ETF' and s.typekind != 'LOF']
        
        self._all_stocks_cache = [s[0] for s in stocks]
        return self._all_stocks_cache
    
    async def _check_stock_signals(self, code: str, trading_dates: List[str], detector: FormulaSignalDetector) -> List[tuple]:
        if not trading_dates:
            return []
        
        start_date = trading_dates[0]
        kline = await Khistory.read_kline(code, 'd', length=120, start=start_date)
        
        if not kline or len(kline) < 30:
            return []
        
        kline_map = {bar.get('time'): bar for bar in kline}
        
        signals_dict = detector.detect(kline, trading_dates)
        
        results = []
        name = self.stock_names.get(code, code)
        
        positions = {}
        
        for date_idx, date in enumerate(trading_dates):
            if date not in kline_map:
                continue
            
            idx = list(kline_map.keys()).index(date)
            if idx < 30:
                continue
            
            current_kline = kline[:idx+1]
            if len(current_kline) < 30:
                continue
            
            today_bar = current_kline[-1]
            close_price = float(today_bar.get('close', 0))
            high_price = float(today_bar.get('high', 0))
            low_price = float(today_bar.get('low', 0))
            
            day_signals = signals_dict.get(date, {})
            formula_sell_prices = day_signals.get('sell', [])
            
            sell_list = []
            
            positions_to_remove = []
            
            for buy_date, buy_info in list(positions.items()):
                buy_price = buy_info['price']
                hold_days = TradingDate.calc_trading_days(buy_date, date)
                
                for cond in self.request.sell_conditions:
                    if cond.type == 'stop_loss' and cond.pct is not None:
                        stop_price = buy_price * (1 + cond.pct / 100)
                        if low_price <= stop_price:
                            sell_price = self._get_sell_price(cond.price_type, stop_price, today_bar, date_idx, trading_dates, kline_map)
                            sell_list.append(SignalItem(
                                code=code,
                                name=name,
                                reason=f"止损{cond.pct}%",
                                price=sell_price
                            ))
                            positions_to_remove.append(buy_date)
                            break
                    elif cond.type == 'profit_target' and cond.pct is not None:
                        target_price = buy_price * (1 + cond.pct / 100)
                        if high_price >= target_price:
                            sell_price = self._get_sell_price(cond.price_type, target_price, today_bar, date_idx, trading_dates, kline_map)
                            sell_list.append(SignalItem(
                                code=code,
                                name=name,
                                reason=f"止盈{cond.pct}%",
                                price=sell_price
                            ))
                            positions_to_remove.append(buy_date)
                            break
                    elif cond.type == 'hold_days' and cond.days is not None:
                        if hold_days >= cond.days:
                            sell_price = self._get_sell_price(cond.price_type, close_price, today_bar, date_idx, trading_dates, kline_map)
                            sell_list.append(SignalItem(
                                code=code,
                                name=name,
                                reason=f"持仓{cond.days}天",
                                price=sell_price
                            ))
                            positions_to_remove.append(buy_date)
                            break
            
            for bp in positions_to_remove:
                del positions[bp]
            
            for price in formula_sell_prices:
                sell_list.append(SignalItem(
                    code=code,
                    name=name,
                    reason=self._get_sell_reason(),
                    price=price
                ))
            
            day_buy_prices = day_signals.get('buy', [])
            buy_list = []
            
            if day_buy_prices:
                for price in day_buy_prices:
                    buy_list.append(SignalItem(
                        code=code,
                        name=name,
                        reason=self._get_buy_reason(),
                        price=price
                    ))
                    positions[date] = {'price': price, 'date': date}
            
            if buy_list or sell_list:
                results.append((date, buy_list, sell_list))
        
        return results
    
    def _get_sell_price(self, price_type: str, trigger_price: float, today_bar: dict, date_idx: int, trading_dates: List[str], kline_map: dict) -> float:
        if price_type == 'trigger':
            return trigger_price
        close_price = float(today_bar.get('close', 0))
        if price_type == 'close':
            return close_price
        elif price_type == 'high':
            return float(today_bar.get('high', 0))
        elif price_type == 'low':
            return float(today_bar.get('low', 0))
        elif price_type == 'next_open':
            if date_idx + 1 < len(trading_dates):
                next_date = trading_dates[date_idx + 1]
                if next_date in kline_map:
                    next_bar = kline_map[next_date]
                    return float(next_bar.get('open', close_price))
            return close_price
        return close_price
    
    def _get_buy_reason(self) -> str:
        cond = self.request.buy_condition
        if cond.type == 'ma_cross':
            f = cond.fast_period or 5
            s = cond.slow_period or 10
            return f"MA{f}金叉MA{s}" if cond.direction == 'up' else f"MA{f}死叉MA{s}"
        elif cond.type == 'consecutive_up':
            return f"连续{cond.period}天上涨"
        elif cond.type == 'break_high':
            return f"突破{cond.period}日高点"
        elif cond.type == 'volume_spike':
            return f"量能突增"
        return cond.type
    
    def _get_sell_reason(self) -> str:
        if not self.request.sell_conditions:
            return ""
        cond = self.request.sell_conditions[0]
        if cond.type == 'ma_cross':
            f = cond.fast_period or 5
            s = cond.slow_period or 10
            return f"MA{f}死叉MA{s}"
        elif cond.type == 'stop_loss':
            return f"止损{cond.pct}%"
        elif cond.type == 'profit_target':
            return f"止盈{cond.pct}%"
        elif cond.type == 'hold_days':
            return f"持仓{cond.days}天"
        return cond.type


async def generate_formula_signals(request: SignalRequest) -> SignalResponse:
    generator = FormulaSignalGenerator(request)
    return await generator.run()
