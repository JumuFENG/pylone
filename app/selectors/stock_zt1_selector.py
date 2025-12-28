from typing import Optional
from app.stock.history import srt
from app.db import query_values, insert_many, query_aggregate
from .models import MdlZt1wb
from .stock_base_selector import StockBaseSelector
from .stock_ztlead_selector import StockZtDaily


class StockZt1WbSelector(StockBaseSelector):
    '''首板烂板'''
    @property
    def db(self):
        return MdlZt1wb

    async def update_pickups(self):
        date = await query_aggregate('max', self.db, 'date')
        szi = StockZtDaily()
        zt = await szi.dump_main_stocks_zt0(date)
        if not zt:
            return

        tlines = srt.tlines([x[0].lower() for x in zt])
        picked = []
        for c, tls in tlines.items():
            high = max([tl[1] for tl in tls])
            firstidx = next((idx for idx, tl in enumerate(tls) if tl[1] == high), None)
            countless = len([tl for tl in tls[firstidx + 1:] if tl[1] < high])
            if firstidx > 215 or countless > 5:
                picked.append(c.upper())

        if len(picked) == 0:
            return
        await insert_many(self.db, [{'date': zt['date'], 'code': c} for c in picked])

    async def dumpDataByDate(self, date: Optional[str] = None):
        if date is None:
            date = await query_aggregate('max', self.db, 'date')

        rows = await query_values(self.db, None, self.db.date == date)
        return rows

