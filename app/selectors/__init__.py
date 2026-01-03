from .stock_zt0_selector import StockHotStocksRetryZt0Selector
from .stock_zt1_selector import StockZt1WbSelector
from .stock_ztlead_selector import StockZtDaily, StockZdtEmotion, StockHotStocksOpenSelector, StockZtLeadingSelector
from .stock_dt_selector import StockDtMap
from .stock_dt3_selector import StockDt3Selector
from .stock_eu_selector import StockTrippleBullSelector


class SelectorsFactory:
    @staticmethod
    def get(selector_name: str):
        selectors = {
            'StockZtDaily': StockZtDaily,
            'StockDtMap': StockDtMap,
            'StockDt3Selector': StockDt3Selector,
            'StockHotStocksRetryZt0Selector': StockHotStocksRetryZt0Selector,
            'StockZt1WbSelector': StockZt1WbSelector,
            'StockZdtEmotion': StockZdtEmotion,
            'StockHotStocksOpenSelector': StockHotStocksOpenSelector,
            'StockZtLeadingSelector': StockZtLeadingSelector,
            'StockTrippleBullSelector': StockTrippleBullSelector,
        }
        selector_class = selectors.get(selector_name)
        if selector_class:
            return selector_class()
        else:
            raise ValueError(f"Selector '{selector_name}' not found.")
