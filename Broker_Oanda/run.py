from get_quotes import get_hist_quotes
from api import OandaApi


if __name__ == '__main__':
    api = OandaApi()

    symbol_lst      = ['EUR_USD']           
    granularity_lst = ['D','H4', 'H2', 'H1', 'M30', 'M15', 'M5', 'M1']      
    date_start      = '2000-01-01T00:00:00' 
    date_end        = '2025-08-01T00:00:00' 

    get_hist_quotes(symbol_lst, granularity_lst, date_start, date_end, api)