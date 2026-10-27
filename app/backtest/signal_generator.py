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


class SignalGenerator:
    def __init__(self, request: SignalRequest):
        self.request = request
        self.stock_names = {}
        self._all_stocks_cache = None
        self._positions = {}
    
    async def run(self) -> SignalResponse:
        trading_dates = self._get_trading_dates()
        await self._load_stock_names()
        
        all_stocks = await self._get_all_stocks()
        
        signals_dict = defaultdict(lambda: {"buy": [], "sell": []})
        self._positions = {}
        
        for idx, code in enumerate(all_stocks):
            try:
                stock_signals = await self._check_stock_signals(code, trading_dates)
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
        
        self._all_stocks_cache = [s[0] for s in stocks]
        return self._all_stocks_cache
    
    async def _check_stock_signals(self, code: str, trading_dates: List[str]) -> List[tuple]:
        if not trading_dates:
            return []
        
        start_date = trading_dates[0]
        kline = await Khistory.read_kline(code, 'd', length=120, start=start_date)
        
        if not kline or len(kline) < 30:
            return []
        
        kline_map = {bar.get('time'): bar for bar in kline}
        
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
            
            sell_list = []
            
            for buy_date, buy_info in list(positions.items()):
                buy_price = buy_info['price']
                hold_days = TradingDate.calc_trading_days(buy_date, date)
                profit_pct = (close_price - buy_price) / buy_price * 100 if buy_price > 0 else 0
                
                should_sell = False
                
                for cond in self.request.sell_conditions:
                    if cond.type == 'stop_loss' and cond.pct is not None:
                        stop_price = buy_price * (1 + cond.pct / 100)
                        if low_price <= stop_price:
                            should_sell = True
                            trigger_price = stop_price
                            sell_price = self._get_sell_price(cond.price_type, trigger_price, today_bar, date_idx, trading_dates, kline_map)
                            break
                    elif cond.type == 'profit_target' and cond.pct is not None:
                        target_price = buy_price * (1 + cond.pct / 100)
                        if high_price >= target_price:
                            should_sell = True
                            trigger_price = target_price
                            sell_price = self._get_sell_price(cond.price_type, trigger_price, today_bar, date_idx, trading_dates, kline_map)
                            break
                    elif cond.type == 'hold_days' and cond.days is not None:
                        if hold_days >= cond.days:
                            should_sell = True
                            sell_price = self._get_sell_price(cond.price_type, close_price, today_bar, date_idx, trading_dates, kline_map)
                            reason = f"hold_days_{cond.days}"
                            sell_list.append(SignalItem(
                                code=code,
                                name=name,
                                reason=reason,
                                price=sell_price
                            ))
                            del positions[buy_date]
                            should_sell = False
                            break
                
                if should_sell:
                    reason = f"stop_loss_{cond.pct}" if cond.type == 'stop_loss' else f"profit_target_{cond.pct}"
                    sell_list.append(SignalItem(
                        code=code,
                        name=name,
                        reason=reason,
                        price=sell_price
                    ))
                    del positions[buy_date]
            
            for cond in self.request.sell_conditions:
                if cond.type in ['ma_cross', 'break_low', 'consecutive_down']:
                    if code in positions:
                        buy_price = positions[code]['price']
                    else:
                        buy_price = close_price
                    result = self._check_tech_sell_condition(current_kline, date, cond, buy_price)
                    if result:
                        tech_reason, trigger_price = result
                        sell_price = self._get_sell_price(cond.price_type, trigger_price, today_bar, date_idx, trading_dates, kline_map)
                        sell_list.append(SignalItem(
                            code=code,
                            name=name,
                            reason=tech_reason,
                            price=sell_price
                        ))
            
            buy_result = self._check_buy_condition(current_kline, date)
            if buy_result:
                buy_reason, trigger_price = buy_result
                buy_price = self._get_buy_price(
                    self.request.buy_condition.price_type,
                    trigger_price, today_bar, date_idx, trading_dates, kline_map
                )
                positions[date] = {'price': buy_price, 'reason': buy_reason}
                buy_list = [SignalItem(code=code, name=name, reason=buy_reason, price=buy_price)]
            else:
                buy_list = []
            
            if buy_list or sell_list:
                results.append((date, buy_list, sell_list))
        
        return results
    
    def _get_price(self, price_type: str, today_bar: dict, date_idx: int, trading_dates: List[str], kline_map: dict) -> float:
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
    
    def _get_buy_price(self, price_type: str, trigger_price: float, today_bar: dict, date_idx: int, trading_dates: List[str], kline_map: dict) -> float:
        if price_type == 'trigger':
            return trigger_price
        return self._get_price(price_type, today_bar, date_idx, trading_dates, kline_map)
    
    def _get_sell_price(self, price_type: str, trigger_price: float, today_bar: dict, date_idx: int, trading_dates: List[str], kline_map: dict) -> float:
        if price_type == 'trigger':
            return trigger_price
        return self._get_price(price_type, today_bar, date_idx, trading_dates, kline_map)
    
    def _check_buy_condition(self, kline: List[dict], date: str) -> Optional[Tuple[str, float]]:
        cond = self.request.buy_condition
        ctype = cond.type
        
        if ctype == 'ma_cross':
            return self._check_ma_cross(kline, cond.fast_period, cond.slow_period, cond.direction)
        elif ctype == 'consecutive_up':
            return self._check_consecutive(kline, 'up', cond.period)
        elif ctype == 'consecutive_down':
            return self._check_consecutive(kline, 'down', cond.period)
        elif ctype == 'break_high':
            return self._check_break_high(kline, cond.period)
        elif ctype == 'break_low':
            return self._check_break_low(kline, cond.period)
        elif ctype == 'volume_spike':
            return self._check_volume_spike(kline, cond.period)
        elif ctype == 'ma_angle':
            return self._check_ma_angle(kline, cond.fast_period, 10)
        
        return None
    
    def _check_tech_sell_condition(self, kline: List[dict], date: str, cond: SellCondition = None, buy_price: float = None) -> Optional[Tuple[str, float]]:
        if cond is None:
            return None
        
        ctype = cond.type
        
        if ctype == 'ma_cross':
            result = self._check_ma_cross(kline, cond.fast_period or 5, cond.slow_period or 10, 'down')
            if result and 'down' in result[0]:
                return result
        elif ctype == 'break_low':
            result = self._check_break_low(kline, cond.period or 20)
            if result:
                return result
        elif ctype == 'consecutive_down':
            result = self._check_consecutive(kline, 'down', cond.period or 3)
            if result:
                return result
        elif ctype == 'stop_loss' and cond.pct is not None and buy_price:
            trigger_price = buy_price * (1 + cond.pct / 100)
            return (f"stop_loss_{cond.pct}", trigger_price)
        elif ctype == 'profit_target' and cond.pct is not None and buy_price:
            trigger_price = buy_price * (1 + cond.pct / 100)
            return (f"profit_target_{cond.pct}", trigger_price)
        
        return None
    
    def _calc_ma(self, kline: List[dict], period: int) -> float:
        if len(kline) < period:
            return 0
        closes = [float(bar.get('close', 0)) for bar in kline[-period:]]
        return sum(closes) / period if closes else 0
    
    def _check_ma_cross(self, kline: List[dict], fast: int, slow: int, direction: str) -> Optional[Tuple[str, float]]:
        if len(kline) < slow + 2:
            return None
        
        ma_fast_prev = self._calc_ma(kline[:-1], fast)
        ma_slow_prev = self._calc_ma(kline[:-1], slow)
        ma_fast_curr = self._calc_ma(kline, fast)
        ma_slow_curr = self._calc_ma(kline, slow)
        
        today_close = float(kline[-1].get('close', 0))
        
        if direction == 'up':
            if ma_fast_prev <= ma_slow_prev and ma_fast_curr > ma_slow_curr:
                return (f"MA{fast}_cross_MA{slow}_up", today_close)
        elif direction == 'down':
            if ma_fast_prev >= ma_slow_prev and ma_fast_curr < ma_slow_curr:
                return (f"MA{fast}_cross_MA{slow}_down", today_close)
        elif direction == 'both':
            if ma_fast_prev <= ma_slow_prev and ma_fast_curr > ma_slow_curr:
                return (f"MA{fast}_cross_MA{slow}_up", today_close)
            elif ma_fast_prev >= ma_slow_prev and ma_fast_curr < ma_slow_curr:
                return (f"MA{fast}_cross_MA{slow}_down", today_close)
        
        return None
    
    def _check_consecutive(self, kline: List[dict], direction: str, days: int) -> Optional[Tuple[str, float]]:
        if len(kline) < days + 1:
            return None
        
        count = 0
        for i in range(-days, 0):
            change = float(kline[i].get('change_px', 0))
            if direction == 'up' and change > 0:
                count += 1
            elif direction == 'down' and change < 0:
                count += 1
            else:
                break
        
        if count >= days:
            today_close = float(kline[-1].get('close', 0))
            return (f"consecutive_{direction}_{days}", today_close)
        return None
    
    def _check_break_high(self, kline: List[dict], period: int) -> Optional[Tuple[str, float]]:
        if len(kline) < period + 1:
            return None
        
        today_close = float(kline[-1].get('close', 0))
        prev_highs = [float(bar.get('high', 0)) for bar in kline[-period-1:-1]]
        max_high = max(prev_highs) if prev_highs else 0
        
        if today_close > max_high:
            return (f"break_high_{period}", max_high)
        return None
    
    def _check_break_low(self, kline: List[dict], period: int) -> Optional[Tuple[str, float]]:
        if len(kline) < period + 1:
            return None
        
        today_close = float(kline[-1].get('close', 0))
        prev_lows = [float(bar.get('low', 0)) for bar in kline[-period-1:-1]]
        min_low = min(prev_lows) if prev_lows else 0
        
        if today_close < min_low:
            return (f"break_low_{period}", min_low)
        return None
    
    def _check_volume_spike(self, kline: List[dict], period: int) -> Optional[Tuple[str, float]]:
        if len(kline) < period + 1:
            return None
        
        vols = [float(bar.get('volume', 0)) for bar in kline[-period-1:-1]]
        avg_vol = sum(vols) / len(vols) if vols else 1
        current_vol = float(kline[-1].get('volume', 0))
        
        if current_vol > avg_vol * 2:
            today_close = float(kline[-1].get('close', 0))
            return (f"volume_spike_{period}", today_close)
        return None
    
    def _check_ma_angle(self, kline: List[dict], period: int, min_angle: float) -> Optional[Tuple[str, float]]:
        if len(kline) < period + 5:
            return None
        
        ma_prev = self._calc_ma(kline[-6:-1], period)
        ma_curr = self._calc_ma(kline[-5:], period)
        
        if ma_prev > 0:
            angle = ((ma_curr - ma_prev) / ma_prev) * 100
            if angle > min_angle:
                today_close = float(kline[-1].get('close', 0))
                return (f"ma{period}_angle_up", today_close)
            elif angle < -min_angle:
                today_close = float(kline[-1].get('close', 0))
                return (f"ma{period}_angle_down", today_close)
        
        return None


async def generate_signals(request: SignalRequest) -> SignalResponse:
    generator = SignalGenerator(request)
    return await generator.run()
