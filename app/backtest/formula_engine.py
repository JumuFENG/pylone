import re
import numpy as np
from typing import List, Dict, Tuple
from collections import defaultdict
from MyTT import MA, EMA, HHV, LLV, REF, CROSS, EVERY, COUNT
from app.lofig import logger


CLOSE = None
OPEN = None
HIGH = None
LOW = None
VOL = None


def _init_globals(kline: list):
    global CLOSE, OPEN, HIGH, LOW, VOL
    CLOSE = np.array([float(bar.get('close', 0)) for bar in kline])
    OPEN = np.array([float(bar.get('open', 0)) for bar in kline])
    HIGH = np.array([float(bar.get('high', 0)) for bar in kline])
    LOW = np.array([float(bar.get('low', 0)) for bar in kline])
    VOL = np.array([float(bar.get('volume', 0)) for bar in kline])


class FormulaEvaluator:
    def __init__(self, formula: str):
        self.formula = formula.upper().replace(' ', '')
    
    def evaluate(self, n: int) -> np.ndarray:
        formula = self.formula
        
        formula = re.sub(r'MA\(C,(\d+)\)', r'MA(CLOSE,\1)', formula)
        formula = re.sub(r'EMA\(C,(\d+)\)', r'EMA(CLOSE,\1)', formula)
        formula = re.sub(r'HHV\(([COHLV]),(\d+)\)', r'HHV(\1,\2)', formula)
        formula = re.sub(r'LLV\(([COHLV]),(\d+)\)', r'LLV(\1,\2)', formula)
        formula = re.sub(r'REF\(C,(\d+)\)', r'REF(CLOSE,\1)', formula)
        
        formula = re.sub(r'(?<![A-Z])C(?![A-Z(,])', 'CLOSE', formula)
        
        formula = self._replace_every(formula)
        
        try:
            result = eval(formula)
            if isinstance(result, (int, float)):
                return np.array([result] * n)
            return result
        except Exception as e:
            logger.error(f"[FormulaEval] Error evaluating '{self.formula}' -> '{formula}': {e}")
            return np.zeros(n)
    
    def _replace_every(self, formula: str) -> str:
        """处理 EVERY 函数，支持嵌套括号"""
        pattern = r'EVERY\((.+),(\d+)\)'
        
        def replace_func(match):
            condition = match.group(1)
            period = match.group(2)
            return f'EVERY({condition},{period})'
        
        while True:
            new_formula = re.sub(pattern, replace_func, formula)
            if new_formula == formula:
                break
            formula = new_formula
        
        return formula


class FormulaSignalDetector:
    def __init__(self, buy_formulas: List[str], sell_formulas: List[str]):
        self.buy_evaluators = [FormulaEvaluator(f) for f in buy_formulas if f]
        self.sell_evaluators = [FormulaEvaluator(f) for f in sell_formulas if f]
    
    def detect(self, kline: list, dates: List[str]) -> Dict[str, Dict[str, list]]:
        if not kline or len(kline) < 20:
            return {}
        
        _init_globals(kline)
        n = len(kline)
        
        buy_results = []
        for ev in self.buy_evaluators:
            try:
                result = ev.evaluate(n)
                if result is not None and len(result) > 0:
                    buy_results.append(result)
                    logger.info(f"[DEBUG] Buy formula evaluated, sum={np.sum(result)}, first 5 values={result[:5]}")
            except Exception as e:
                logger.error(f"[DEBUG] Buy formula error: {e}")
        
        sell_results = []
        for ev in self.sell_evaluators:
            try:
                result = ev.evaluate(n)
                if result is not None and len(result) > 0:
                    sell_results.append(result)
                    logger.info(f"[DEBUG] Sell formula evaluated, sum={np.sum(result)}, first 5 values={result[:5]}")
            except Exception as e:
                logger.error(f"[DEBUG] Sell formula error: {e}")
        
        if buy_results:
            buy_signal = np.any(buy_results, axis=0)
            logger.info(f"[DEBUG] Buy signal, sum={np.sum(buy_signal)}")
        else:
            buy_signal = np.zeros(n)
            logger.warning("[DEBUG] No buy results, buy_signal all zeros")
        
        if sell_results:
            sell_signal = np.any(sell_results, axis=0)
        else:
            sell_signal = np.zeros(n)
        
        signals = defaultdict(lambda: {"buy": [], "sell": []})
        
        prev_buy = np.roll(buy_signal, 1)
        prev_buy[0] = 0
        buy_change = (buy_signal == 1) & (prev_buy == 0)
        
        for idx in np.where(buy_change)[0]:
            if idx < len(kline):
                date = kline[idx].get('time')
                if date in dates:
                    price = float(kline[idx].get('close', 0))
                    signals[date]["buy"].append(price)
        
        prev_sell = np.roll(sell_signal, 1)
        prev_sell[0] = 0
        sell_change = (sell_signal == 1) & (prev_sell == 0)
        
        for idx in np.where(sell_change)[0]:
            if idx < len(kline):
                date = kline[idx].get('time')
                if date in dates:
                    price = float(kline[idx].get('close', 0))
                    signals[date]["sell"].append(price)
        
        return dict(signals)


def parse_condition_to_formula(condition_type: str, params: dict) -> Tuple[str, str]:
    formulas = {
        'ma_cross_up': (
            f"CROSS(MA(C,{params.get('fast', 5)}), MA(C,{params.get('slow', 10)}))",
            f"MA{params.get('fast', 5)}金叉MA{params.get('slow', 10)}"
        ),
        'ma_cross_down': (
            f"CROSS(MA(C,{params.get('slow', 10)}), MA(C,{params.get('fast', 5)}))",
            f"MA{params.get('fast', 5)}死叉MA{params.get('slow', 10)}"
        ),
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
    }
    
    if condition_type in formulas:
        return formulas[condition_type]
    
    return ('', '')
