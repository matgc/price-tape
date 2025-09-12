from api import FxApi
import pandas as pd
import json
from pathlib import Path
import math
import os
from dateutil.parser import parse


if __name__ == '__main__':

# /// USER INPUT /////////////////////////////////////////////////////////
# ------------------------------------------------------------------------

    fx_api = FxApi()
    chosen_ids=[
        'Forex',
        'Crypto',
        'CFD 00-01',
        'US Stocks'
    ]

    fx_api.filter_instruments(chosen_ids)

    symbol_lst      = fx_api.filtered_inst_lst
    granularity_lst = ['D1', 'H1']
    date_start      = '2019-01-01T00:00:00'
    date_end        = '2025-09-10T00:00:00'

    able_pct = 0.6 #Pct of candles to consider dataset able to backtest


# /// CONSTANTS //////////////////////////////////////////////////////////
# ------------------------------------------------------------------------

    QUOTES = Path(__file__).parent / "hist_quotes"
    if not os.path.exists(QUOTES):
        os.makedirs(QUOTES)
    REFS = QUOTES / "refs"
    if not os.path.exists(REFS):
        os.makedirs(REFS)

    DAILY_TRADING_HOURS = 6.5
    YEARLY_TRADING_DAYS = 252
    DAILY_CANDLES = {
        'M1' : 60 * DAILY_TRADING_HOURS, 
        'M5' : (60/5) * DAILY_TRADING_HOURS, 
        'M15': (60/15) * DAILY_TRADING_HOURS, 
        'M30': (60/30) * DAILY_TRADING_HOURS, 
        'H1' : math.ceil(DAILY_TRADING_HOURS), 
        'H4' : math.ceil(DAILY_TRADING_HOURS/4), 
        'D1' : 1
    }


# /// CALCULATIONS ///////////////////////////////////////////////////////
# ------------------------------------------------------------------------

    start_date = parse(date_start)
    end_date = parse(date_end)
    t_delta = end_date - start_date
    y_delta = t_delta.days / 365.25
    
    expected_candles = {}
    for gran in granularity_lst:
        expected_candles[gran]=(
            DAILY_CANDLES[gran]*YEARLY_TRADING_DAYS*y_delta*able_pct
        )

    has_candles = {}
    for symbol in symbol_lst:
        has_candles[symbol] = {}
        for gran in granularity_lst:
            try:
                df = pd.read_pickle(QUOTES/f'{symbol}_{gran}.pkl')
                candles = len(df)
            except:
                candles = 0
            has_candles[symbol][gran] = candles >= expected_candles[gran]

    able_inst_lst = []
    for symbol in symbol_lst:
        data_complete = True
        for gran in granularity_lst:
            if not has_candles[symbol][gran]:
                data_complete = False
        if data_complete:
            able_inst_lst.append(symbol)

    with open(REFS/'has_candles.json', 'w') as f:
        json.dump(has_candles, f, indent=4)

    with open(REFS/'able_inst_lst.json', 'w') as f:
        json.dump(able_inst_lst, f, indent=4)