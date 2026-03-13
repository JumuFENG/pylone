import os
import sys
import json
import csv
import asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.users.usmanager import UserStockManager as usm
from app.admin.router import admin_user_list
from app.selectors import SelectorsFactory as sfac
from app.stock.date import TradingDate
from app.db import query_values, query_aggregate


async def dump_watchings_to_json():
    users = await admin_user_list()
    watchings = {}
    deals = {}
    for u in users:
        if u.realcash == 0:
            continue

        uw = await usm.watchings_with_strategy(u)
        if len(uw) > 0:
            watchings[f'{u.id}'] = uw
        ud = await usm.get_deals(u)
        if len(ud) > 0:
            deals[f'{u.id}'] = ud


    path = os.path.realpath(os.path.dirname(os.path.abspath(__file__)))

    with open(os.path.join(path, 'user_stocks.json'), 'w') as f:
        json.dump(watchings, f, indent=2)

    with open(os.path.join(path, 'user_deals.json'), 'w') as f:
        json.dump(deals, f, indent=2)


async def dump_selectors_to_csv():
    selectors = ['StockZt1WbSelector', 'StockHotStocksRetryZt0Selector',]
    path = os.path.realpath(os.path.dirname(os.path.abspath(__file__)))
    for s in selectors:
        sel = sfac.get(s)
        if s == 'StockHotStocksRetryZt0Selector':
            date = await query_aggregate('max', sel.db, 'date')
            if date is None:
                rows = []
            ldate = TradingDate.prev_trading_date(date, 2)
            rows = await query_values(sel.db, None, sel.db.date < ldate, sel.db.remdays > 0)
        else:
            rows = await sel.dumpDataByDate()

        with open(os.path.join(path, f'{sel.db.__tablename__}.csv'), 'w') as f:
            cols = [c.name for c in sel.db.__table__.columns]
            writer = csv.DictWriter(f, fieldnames=cols)
            writer.writeheader()
            for row_data in rows:
                row_dict = dict(zip(cols, row_data))
                writer.writerow(row_dict)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(dump_selectors_to_csv())
