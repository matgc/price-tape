import requests
import os
import json
import time
import datetime as dt
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_result


LABEL_MAP = {  'Open'   : 'o'
             , 'High'   : 'h'
             , 'Low'    : 'l'
             , 'Close'  : 'c'
             }

THROTTLE_TIME = 0.25


def fxopen_timestamp_now():
    dt_obj  = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    curr_ts = int(pd.Timestamp(dt_obj).timestamp() * 1000)
    return curr_ts


class FxApi:

    def __init__(self):
        self.get_credentials()
        self.make_auth_header()
        self.session = requests.Session()
        self.session.headers.update(self.AUTH_HEADER)
        self.last_req_time = dt.datetime.now()

# /////////////////////////////////////////////////////////////////////////
# /// AUTHENTICATION /////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////

    def get_credentials(self):
        load_dotenv(Path(__file__).parent.parent / '.env')
        
        self.login_basic       = os.getenv(f'FX_LOGIN')
        self.api_id_basic      = os.getenv(f'FX_API_ID')
        self.api_key_basic     = os.getenv(f'FX_API_KEY')
        self.api_secret_basic  = os.getenv(f'FX_API_SECRET')
        
        self.fxopen_url        = os.getenv(f'FX_URL')


# /// BASIC AUTH /////////////////////////////////////////////////////////
# ------------------------------------------------------------------------
    
    def make_auth_header(self):
        authorization = f'Basic {self.api_id_basic}:{self.api_key_basic}:{self.api_secret_basic}'
        self.AUTH_HEADER = {  'Authorization':authorization
                            , 'Content-Type' :'application/json'
                            , 'Accept'       :'application/json'
                            }
    

# /////////////////////////////////////////////////////////////////////////
# /// THROTTLE HANDLER ///////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////
    
    def throttle(self):
        el_s = (dt.datetime.now() - self.last_req_time).total_seconds()
        if el_s < THROTTLE_TIME:
            time.sleep(THROTTLE_TIME - el_s)
        self.last_req_time = dt.datetime.now()


# /////////////////////////////////////////////////////////////////////////
# /// REQUESTS ///////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_result(lambda result: result[0] is False)
    )
    def make_request(self
                     , url_sufix
                     , verb='get'
                     , success_code=200
                     , params=None
                     , data=None
                     , headers=None
                     ):

        self.throttle()
        full_url = f"{self.fxopen_url}/{url_sufix}"

        if data is not None:
            data = json.dumps(data)

        try:
            response = None
            if verb == "get":
                response = self.session.get(full_url
                                            , params=params
                                            , data=data
                                            , headers=headers)
            if verb == "post":
                response = self.session.post(full_url
                                             , params=params
                                             , data=data
                                             , headers=headers)
            if verb == "put":
                response = self.session.put(full_url
                                            , params=params
                                            , data=data
                                            , headers=headers)
            if verb == "delete":
                response = self.session.delete(full_url
                                               , params=params
                                               , data=data
                                               , headers=headers)
            
            if response == None:
                return False, {'error': 'verb not found'}

            if response.status_code == success_code:
                return True, response.json()
            else:
                return False, response.json()
            
        except Exception as error:
            return False, {'Exception': error}
        

# /// CANDLES ////////////////////////////////////////////////////////////
# ------------------------------------------------------------------------

    def fetch_candles(self
                      , symbol : str
                      , count = -10
                      , granularity = "M1"
                      , timestamp_from = None # In FxOpen format
                      ):
        url_symbol = symbol.replace('#', '%23')
        if timestamp_from is None:
            timestamp_from = fxopen_timestamp_now()
            
        params = dict(timestamp=timestamp_from
                      , count=count
                      )

        if count < 0:
            params['count']=count+1

        base_url_sufix = f"quotehistory/{url_symbol}/{granularity}/bars/"

        ok_bid, bid_data = self.make_request(base_url_sufix+"bid", params=params)
        ok_ask, ask_data = self.make_request(base_url_sufix+"ask", params=params)

        if ok_ask and ok_bid:
            return True, [ask_data, bid_data]
        print(
            f'fetch_candles() failed. bid_ok: {ok_bid}, ask_ok: {ok_ask}. '
            f'symbol {symbol}, count {count}, granularity {granularity}, '
            f'timestamp_from {timestamp_from}')
        return False, None
    
    def make_bar_dict(self, price_label: str, item):
        data = dict(time=pd.to_datetime(item['Timestamp'], unit='ms'))
        for ohlc in LABEL_MAP.keys():
            data[f"{price_label}_{LABEL_MAP[ohlc]}"]=item[ohlc]
        return data
    

    def fetch_candles_as_df(self
                            , symbol
                            , count = -10
                            , granularity = "M1"
                            , date_start = None
                            ):

        if date_start is not None:
            timestamp_from = int(pd.Timestamp(date_start).timestamp() * 1000)
        else:
            timestamp_from = fxopen_timestamp_now()

        ok, data_list = self.fetch_candles(symbol, count, granularity, timestamp_from)

        if ok == False:
            print(f'fetch_candles_as_df() got no candles.')
            return None
        
        data_ask, data_bid = data_list

        if (data_ask is None) or (data_bid is None):
            print(f'fetch_candles_as_df() data_ask is None: {data_ask is None}, '
                      f'data_bid is None: {data_bid is None}')
            return None
        
        if ("Bars" in data_ask == False) or ("Bars" in data_bid == False):
            print(f'fetch_candles_as_df() Bars in data_ask: {"Bars" in data_ask}, '
                      f'Bars in data_bid: {"Bars" in data_bid}')
            return pd.DataFrame()
        
        ask_bars = data_ask["Bars"]
        bid_bars = data_bid["Bars"]

        if len(ask_bars) == 0 or len(bid_bars) == 0:
            print(f'fetch_candles_as_df() len(ask_bars): {len(ask_bars)}, '
                      f'len(bid_bars): {len(bid_bars)}')
            return pd.DataFrame()

        AvailableTo = pd.to_datetime(data_bid['AvailableTo'], unit='ms')

        bids = [self.make_bar_dict('bid', item) for item in bid_bars]
        asks = [self.make_bar_dict('ask', item) for item in ask_bars]

        df_bid = pd.DataFrame.from_dict(bids)
        df_ask = pd.DataFrame.from_dict(asks)
        df_merged = pd.merge(left=df_bid, right=df_ask, on='time')    

        for i in ['_o', '_h', '_l', '_c']:
            df_merged[f'mid{i}'] = (df_merged[f'ask{i}'] + df_merged[f'bid{i}']) / 2

        if df_merged.shape[0] > 0 and df_merged.iloc[-1].time == AvailableTo:
            df_merged = df_merged[:-1]  

        return df_merged
        

# /// INTRUMENTS /////////////////////////////////////////////////////////
# ------------------------------------------------------------------------

    def filter_instruments(
            self,
            chosen_ids=[
                'Forex',
                'Crypto',
                'CFD 00-01',
                'US Stocks'
            ]
    ):

        self.get_tradables_dict()

        filtered_inst_set = set()
        for key in self.tradables_dict.keys():
            if self.tradables_dict[key]['StatusGroupId'] in chosen_ids:
                filtered_inst_set.add(key)
        self.filtered_inst_lst = list(filtered_inst_set)

        with open(Path(__file__).parent / 'hist_quotes/refs/filtered_inst_lst.json', 'w') as f:
            json.dump(self.filtered_inst_lst, f, indent=4)


    def get_tradables_dict(self):

        INST_KEYS =['Symbol',
                    'ContractSize',
                    'MarginHedged',
                    'MarginFactor',
                    'Description', 
                    'StatusGroupId', 
                    'Precision',
                    'MinTradeAmount',
                    'MaxTradeAmount',                  
                    'TradeAmountStep',
                    'CommissionType',
                    'CommissionChargeType',
                    'Commission',
                    'DefaultSlippage',
                    'SlippageType'
                    ]
        
        self.get_tradables()
        if self.tradables is not None:
            self.tradables_dict = {}
            for instrument in self.tradables:
                key = instrument['Symbol']
                self.tradables_dict[key] = {k: instrument[k] for k in INST_KEYS}
        else:
            raise ValueError('Unable to fetch instruments from broker')
        
        hist_folder = Path(__file__).parent / 'hist_quotes'
        refs_folder = hist_folder / 'refs'
        if not os.path.exists(hist_folder):
            os.makedirs(hist_folder)
        if not os.path.exists(refs_folder):
            os.makedirs(refs_folder)

        with open(refs_folder / 'tradables_dict.json', "w") as f:
            json.dump(self.tradables_dict, f, indent=4)


    def get_tradables(self):
        self.get_all_instruments()
        self.get_instruments_with_hist()
        self.filter_tradables()


    def get_all_instruments(self):
        url_sufix = "symbol"
        _, self.all_inst = self.make_request(url_sufix)


    def get_instruments_with_hist(self): # inst with hist quotes available
        url_sufix = f"quotehistory/symbols"
        _, self.hist_inst = self.make_request(url_sufix)


    def filter_tradables(self):
        try:
            self.std_inst   = [x for x in self.all_inst if not x['Symbol'][-2:]=='_L']
            self.tradables  = [x for x in self.std_inst if x['Symbol'] in self.hist_inst]
        except:
            self.std_inst   = None
            self.tradables  = None
    

    def get_periodicities(self, instrument):
        url_sufix = f'quotehistory/{instrument}/periodicities'
        _, periodicities = self.make_request(url_sufix)