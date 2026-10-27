import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from app.stock.date import TradingDate
from app.stock.history import Khistory
from app.selectors import SelectorsFactory
from app.stock.manager import AllStocks
from app.db import query_values
from app.stock.models import MdlAllStock
from .schemas import (
    BacktestParams, BacktestResult, TradeRecord, 
    Position, DailyStats
)


class BacktestEngine:
    def __init__(self, params: BacktestParams):
        self.params = params
        self.positions: Dict[str, Position] = {}
        self.trades: List[TradeRecord] = []
        self.daily_stats: List[DailyStats] = []
        self.cash = params.init_capital
        self.capital = params.init_capital
        self.max_capital = params.init_capital
        self._stock_cache = {}
        
    async def run(self) -> BacktestResult:
        trading_dates = self._get_trading_dates()
        
        if self.params.custom_strategy and self.params.custom_strategy.conditions:
            pass
        else:
            self.selector = self._get_selector()
        
        stock_names = await self._get_stock_names()
        
        total_dates = len(trading_dates)
        
        for idx, date in enumerate(trading_dates):
            progress = int((idx / total_dates) * 100)
            
            await self._process_day(date, stock_names, progress)
            
            if idx % 10 == 0:
                await asyncio.sleep(0)
        
        return self._build_result()
    
    def _get_trading_dates(self) -> List[str]:
        dates = []
        current = self.params.start_date
        while not TradingDate.is_trading_date(current):
            current = TradingDate.next_trading_date(current, 1)
        end = self.params.end_date
        while current <= end:
            dates.append(current)
            if current == end:
                break
            current = TradingDate.next_trading_date(current, 1)
        return dates
    
    def _get_selector(self):
        strategy_map = {
            'zt0': 'StockHotStocksRetryZt0Selector',
            'zt1': 'StockZt1WbSelector',
            'dt': 'StockDtMap',
            'dt3': 'StockDt3Selector',
            'eu': 'StockTrippleBullSelector',
        }
        selector_name = strategy_map.get(self.params.strategy, 'StockHotStocksRetryZt0Selector')
        return SelectorsFactory.get(selector_name)
    
    async def _get_stock_names(self) -> Dict[str, str]:
        stocks = await AllStocks.read_all()
        stock_pool = await self._get_all_stocks()
        return {s.code: s.name for s in stocks if s.code in stock_pool}
    
    async def _get_all_stocks(self) -> List[str]:
        if hasattr(self, '_all_stocks_cache'):
            return self._all_stocks_cache
        
        pool_filter = None
        if self.params.stock_pool == 'sh':
            pool_filter = MdlAllStock.code.startswith('sh6')
        elif self.params.stock_pool == 'sz':
            pool_filter = MdlAllStock.code.startswith('sz00')
        elif self.params.stock_pool == 'cy':
            pool_filter = MdlAllStock.code.startswith('sz30')
        elif self.params.stock_pool == 'ke':
            pool_filter = MdlAllStock.code.startswith('sh68')
        elif self.params.stock_pool == 'bj':
            pool_filter = MdlAllStock.code.startswith('bj')
        
        if pool_filter is not None:
            stocks = await query_values(MdlAllStock, [MdlAllStock.code], pool_filter)
        else:
            stocks = await query_values(MdlAllStock, [MdlAllStock.code])
        
        self._all_stocks_cache = [s[0] for s in stocks]
        return self._all_stocks_cache
    
    async def _process_day(self, date: str, stock_names: Dict[str, str], progress: int):
        buy_signals = await self._get_buy_signals(date)
        
        self._close_positions(date, stock_names)
        
        if buy_signals and len(self.positions) < self.params.max_positions:
            available_slots = self.params.max_positions - len(self.positions)
            signals_to_buy = buy_signals[:min(available_slots, self.params.buy_limit)]
            
            for code in signals_to_buy:
                await self._buy(code, date, stock_names.get(code, code))
        
        self._record_daily_stats(date)
    
    async def _get_buy_signals(self, date: str) -> List[str]:
        if self.params.custom_strategy and self.params.custom_strategy.conditions:
            return await self._get_custom_signals(date)
        
        try:
            if self.params.strategy == 'zt0':
                stocks = await self.selector.get_main_stocks_zt0(date)
                return [s['code'] for s in stocks] if isinstance(stocks, list) else []
            elif self.params.strategy == 'zt1':
                stocks = await self.selector.select(date)
                return [s['code'] for s in stocks] if isinstance(stocks, list) else []
            elif self.params.strategy == 'dt':
                stocks = await self.selector.get_dt_stocks(date)
                return stocks if isinstance(stocks, list) else []
            elif self.params.strategy == 'dt3':
                stocks = await self.selector.select(date)
                return stocks if isinstance(stocks, list) else []
            elif self.params.strategy == 'eu':
                stocks = await self.selector.select(date)
                return [s['code'] for s in stocks] if isinstance(stocks, list) else []
            return []
        except Exception:
            return []
    
    async def _get_custom_signals(self, date: str) -> List[str]:
        conditions = [c for c in self.params.custom_strategy.conditions if c.enabled]
        if not conditions:
            return []
        
        all_stocks = await self._get_all_stocks()
        
        max_check = 500
        stocks_to_check = all_stocks[:max_check]
        
        signals = []
        
        for code in stocks_to_check:
            try:
                kline = await Khistory.read_kline(code, 'd', length=30, start=date)
                if not kline or len(kline) < 2:
                    continue
                
                today = kline[-1]
                yesterday = kline[-2] if len(kline) > 1 else today
                
                if not self._check_conditions(conditions, today, yesterday, kline):
                    continue
                
                signals.append(code)
                
                if len(signals) >= 30:
                    break
            except Exception:
                continue
        
        return signals
    
    def _check_conditions(self, conditions: list, today: dict, yesterday: dict, kline: List[dict]) -> bool:
        for cond in conditions:
            if cond.type == 'price_change':
                value = float(today.get('change', 0))
            elif cond.type == 'change_pct':
                value = float(today.get('change_px', 0))
            elif cond.type == 'volume':
                value = float(today.get('volume', 0))
            elif cond.type == 'amount':
                value = float(today.get('amount', 0))
            elif cond.type == 'turnover':
                value = float(today.get('turnover', 0))
            elif cond.type == 'amplitude':
                value = float(today.get('amplitude', 0))
            elif cond.type == 'high':
                value = float(today.get('high', 0))
            elif cond.type == 'low':
                value = float(today.get('low', 0))
            elif cond.type == 'close':
                value = float(today.get('close', 0))
            elif cond.type == 'open':
                value = float(today.get('open', 0))
            elif cond.type == 'consecutive_rise':
                value = self._count_consecutive(kline, 'rise')
            elif cond.type == 'consecutive_fall':
                value = self._count_consecutive(kline, 'fall')
            elif cond.type == 'ma5_cross_ma10':
                value = self._check_ma_cross(kline, 5, 10)
            elif cond.type == 'volume_ratio':
                value = self._calc_volume_ratio(kline)
            elif cond.type == 'price_vs_ma5':
                close = float(today.get('close', 0))
                ma5 = self._calc_ma(kline, 5)
                value = (close - ma5) / ma5 * 100 if ma5 else 0
            elif cond.type == 'price_vs_ma10':
                close = float(today.get('close', 0))
                ma10 = self._calc_ma(kline, 10)
                value = (close - ma10) / ma10 * 100 if ma10 else 0
            elif cond.type == 'price_vs_ma20':
                close = float(today.get('close', 0))
                ma20 = self._calc_ma(kline, 20)
                value = (close - ma20) / ma20 * 100 if ma20 else 0
            elif cond.type == 'limit_up':
                change = float(today.get('change', 0))
                value = 1 if change >= 9.9 else 0
            elif cond.type == 'limit_down':
                change = float(today.get('change', 0))
                value = 1 if change <= -9.9 else 0
            elif cond.type == 'volume_increase':
                if len(kline) < 2:
                    value = 0
                else:
                    vol_today = float(today.get('volume', 0))
                    vol_yesterday = float(yesterday.get('volume', 1))
                    value = (vol_today - vol_yesterday) / vol_yesterday * 100 if vol_yesterday else 0
            else:
                continue
            
            if cond.min_value is not None and value < cond.min_value:
                return False
            if cond.max_value is not None and value > cond.max_value:
                return False
        
        return True
    
    def _count_consecutive(self, kline: List[dict], direction: str) -> int:
        count = 0
        for bar in reversed(kline):
            change = float(bar.get('change_px', 0))
            if direction == 'rise' and change > 0:
                count += 1
            elif direction == 'fall' and change < 0:
                count += 1
            else:
                break
        return count
    
    def _calc_ma(self, kline: List[dict], period: int) -> float:
        if len(kline) < period:
            return 0
        closes = [float(bar.get('close', 0)) for bar in kline[-period:]]
        return sum(closes) / period if closes else 0
    
    def _calc_volume_ratio(self, kline: List[dict]) -> float:
        if len(kline) < 5:
            return 1
        vols = [float(bar.get('volume', 0)) for bar in kline[-5:-1]]
        avg_vol = sum(vols) / len(vols) if vols else 1
        current_vol = float(kline[-1].get('volume', 0))
        return current_vol / avg_vol if avg_vol else 1
    
    def _check_ma_cross(self, kline: List[dict], period1: int, period2: int) -> int:
        if len(kline) < period2 + 1:
            return 0
        
        ma1_prev = self._calc_ma(kline[:-1], period1)
        ma2_prev = self._calc_ma(kline[:-1], period2)
        ma1_curr = self._calc_ma(kline, period1)
        ma2_curr = self._calc_ma(kline, period2)
        
        if ma1_prev <= ma2_prev and ma1_curr > ma2_curr:
            return 1
        elif ma1_prev >= ma2_prev and ma1_curr < ma2_curr:
            return -1
        return 0
    
    async def _buy(self, code: str, date: str, name: str):
        next_date = TradingDate.next_trading_date(date, 1)
        if not next_date:
            return
        
        kline = await Khistory.read_kline(code, 'd', length=1, start=next_date)
        if not kline:
            return
        
        buy_price = float(kline[0]['open']) * (1 + self.params.slippage)
        max_shares = int(self.cash / (buy_price * 100)) * 100
        
        if max_shares < 100:
            return
        
        quantity = max_shares
        amount = buy_price * quantity
        fee = amount * self.params.fee_rate
        
        if amount + fee > self.cash:
            quantity = int((self.cash - fee) / buy_price / 100) * 100
            if quantity < 100:
                return
            amount = buy_price * quantity
            fee = amount * self.params.fee_rate
        
        self.cash -= (amount + fee)
        
        self.positions[code] = Position(
            code=code,
            name=name,
            buy_date=next_date,
            buy_price=buy_price,
            quantity=quantity,
            current_price=buy_price,
        )
        
        self.trades.append(TradeRecord(
            date=next_date,
            code=code,
            name=name,
            action='buy',
            price=buy_price,
            quantity=quantity,
            amount=amount,
            fee=fee,
            reason='signal',
        ))
    
    def _close_positions(self, date: str, stock_names: Dict[str, str]):
        positions_to_close = []
        
        for code, pos in self.positions.items():
            hold_days = TradingDate.calc_trading_days(pos.buy_date, date)
            
            should_close = False
            reason = ''
            
            if self.params.hold_days > 0 and hold_days >= self.params.hold_days:
                should_close = True
                reason = 'hold_days'
            
            if not should_close and (self.params.stop_loss is not None or self.params.stop_gain is not None):
                profit_pct = (pos.current_price - pos.buy_price) / pos.buy_price * 100
                
                if self.params.stop_loss is not None and profit_pct <= self.params.stop_loss:
                    should_close = True
                    reason = 'stop_loss'
                elif self.params.stop_gain is not None and profit_pct >= self.params.stop_gain:
                    should_close = True
                    reason = 'stop_gain'
            
            if should_close:
                positions_to_close.append((code, reason))
        
        for code, reason in positions_to_close:
            asyncio.create_task(self._sell(code, date, stock_names.get(code, code), reason))
    
    async def _sell(self, code: str, date: str, name: str, reason: str):
        if code not in self.positions:
            return
        
        pos = self.positions[code]
        
        kline = await Khistory.read_kline(code, 'd', length=1, start=date)
        if not kline:
            return
        
        sell_price = float(kline[0]['open']) * (1 - self.params.slippage)
        amount = sell_price * pos.quantity
        fee = amount * self.params.fee_rate
        profit = amount - (pos.buy_price * pos.quantity) - fee
        profit_pct = profit / (pos.buy_price * pos.quantity) * 100
        
        self.cash += (amount - fee)
        
        self.trades.append(TradeRecord(
            date=date,
            code=code,
            name=name,
            action='sell',
            price=sell_price,
            quantity=pos.quantity,
            amount=amount,
            fee=fee,
            reason=reason,
            profit=profit,
            profit_pct=profit_pct,
        ))
        
        del self.positions[code]
    
    def _record_daily_stats(self, date: str):
        positions_value = sum(
            pos.current_price * pos.quantity 
            for pos in self.positions.values()
        )
        capital = self.cash + positions_value
        
        self.capital = capital
        if capital > self.max_capital:
            self.max_capital = capital
        
        today_trades = len([t for t in self.trades if t.date == date and t.action == 'sell'])
        
        self.daily_stats.append(DailyStats(
            date=date,
            capital=capital,
            positions=len(self.positions),
            trades=today_trades,
            daily_profit=capital - self.params.init_capital,
        ))
    
    def _build_result(self) -> BacktestResult:
        sell_trades = [t for t in self.trades if t.action == 'sell']
        
        total_trades = len(sell_trades)
        winning_trades = len([t for t in sell_trades if t.profit and t.profit > 0])
        losing_trades = len([t for t in sell_trades if t.profit and t.profit <= 0])
        
        win_rate = winning_trades / total_trades * 100 if total_trades > 0 else 0
        
        total_profit = sum(t.profit for t in sell_trades if t.profit)
        total_profit_pct = total_profit / self.params.init_capital * 100
        
        avg_profit = total_profit / total_trades if total_trades > 0 else 0
        
        hold_days_list = []
        for t in sell_trades:
            buy_t = next((b for b in reversed(self.trades) 
                         if b.code == t.code and b.action == 'buy'), None)
            if buy_t:
                hd = TradingDate.calc_trading_days(buy_t.date, t.date)
                hold_days_list.append(hd)
        avg_hold_days = sum(hold_days_list) / len(hold_days_list) if hold_days_list else 0
        
        max_drawdown = self._calc_max_drawdown()
        
        return BacktestResult(
            params=self.params,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            total_profit=total_profit,
            total_profit_pct=total_profit_pct,
            max_drawdown=max_drawdown,
            avg_profit=avg_profit,
            avg_hold_days=avg_hold_days,
            daily_stats=self.daily_stats,
            trades=self.trades,
            final_positions=list(self.positions.values()),
        )
    
    def _calc_max_drawdown(self) -> float:
        if not self.daily_stats:
            return 0
        
        peak = self.params.init_capital
        max_dd = 0
        
        for stats in self.daily_stats:
            if stats.capital > peak:
                peak = stats.capital
            dd = (peak - stats.capital) / peak * 100
            if dd > max_dd:
                max_dd = dd
        
        return max_dd


async def run_backtest(params: BacktestParams) -> BacktestResult:
    engine = BacktestEngine(params)
    return await engine.run()
