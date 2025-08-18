from get_quotes import get_hist_quotes
from api import OandaApi


if __name__ == '__main__':
    api = OandaApi()

    symbol_lst      = ['EUR_USD']           
    granularity_lst = ['H1']
    date_start      = '2025-03-25T00:00:00'
    date_end        = '2025-03-29T00:00:00'

    get_hist_quotes(symbol_lst, granularity_lst, date_start, date_end, api)