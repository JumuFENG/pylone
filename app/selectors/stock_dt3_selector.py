from typing import Optional
from app.stock.models import MdlDayDtMap
from app.stock.date import TradingDate
from app.db import query_values, upsert_many
from .models import MdlDt3
from .stock_base_selector import StockBaseSelector


class StockDt3Selector(StockBaseSelector):
    @property
    def db(self):
        return MdlDt3

    async def task_prepare(self, date: Optional[str] = None) -> None:
        cds = []
        if date is not None:
            cds.append(MdlDayDtMap.time > date)
        self.dthis = await query_values(MdlDayDtMap, None, *cds)
        self.wkstocks = [[d, c] for d,c,s,suc in self.dthis if s == 3 and suc == 1]
        self.wkselected = []

    async def task_processing(self, item):
        d3, code = item
        if d3 == TradingDate.today():
            return
        dates = [d for d,c,s,suc in self.dthis if s == 1 and code == c and d < d3]
        if len(dates) == 0:
            return
        d1 = max(dates)
        self.wkselected.append([code, d1, d3])

    async def post_process(self) -> None:
        if len(self.wkselected) == 0:
            return
        values = []
        uniques = []
        for c, d1, d3 in self.wkselected:
            if (c, d1) in uniques:
                continue
            uniques.append((c, d1))
            values.append({'code': c, 'date': d1, 'date3': d3})
        await upsert_many(self.db, values, ['code', 'date'])
