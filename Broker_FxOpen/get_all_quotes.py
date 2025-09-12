from get_quotes import get_hist_quotes
from api import FxApi


if __name__ == '__main__':

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

    get_hist_quotes(symbol_lst, granularity_lst, date_start, date_end, fx_api)