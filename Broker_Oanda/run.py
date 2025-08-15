from get_quotes import get_hist_quotes
from api import OandaApi


if __name__ == '__main__':
    api = OandaApi()

    symbol_lst      = ['EUR_USD']           #Change as needed
    granularity_lst = ['H1']                #Change as needed
    date_start      = '2025-03-26T00:00:00' #Change as needed
    date_end        = '2025-03-28T00:00:00' #Change as needed

    get_hist_quotes(symbol_lst, granularity_lst, date_start, date_end, api)