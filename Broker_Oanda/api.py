import requests
import os
import pandas as pd
from dateutil import parser
from datetime import datetime as dt
from dotenv import load_dotenv
from pathlib import Path


class OandaApi:

    def __init__(self):
        self.session = requests.Session()
        self.get_credentials()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
            })
    


# /////////////////////////////////////////////////////////////////////////
# /// AUTHENTICATION /////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////


    def get_credentials(self):
        load_dotenv(Path(__file__).parent.parent / '.env')
        
        self.api_key    = os.getenv(f'OANDA_API_KEY')
        self.account_id = os.getenv(f'OANDA_ACCOUNT_ID')
        self.oanda_url  = os.getenv(f'OANDA_URL')



# /////////////////////////////////////////////////////////////////////////
# /// REQUESTS ///////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////


    def make_request(self, url, requestType='get', succes_code=200, params=None, data=None, headers=None):
        full_url = f'{self.oanda_url}/{url}'
        try:
            response = None
            
            if requestType == 'get':
                response = self.session.get(full_url, params=params, data=data, headers=headers)
            
            if response == None:
                return False, {'error': 'action returned empty'}
            
            if response.status_code == succes_code:
                return True, response.json()
            else:
                return False, response.json()


        except Exception as error:
            return False, {'Exception': error}


    def get_account_endpoint(self, endpoint, data_key):
        url = f'accounts/{self.account_id}/{endpoint}'
        requestWorked, data = self.make_request(url)
        
        if requestWorked == True and data_key in data:
            return data[data_key]
        else:
            print('ERROR get_account_endpoint()', data)
            return None
    

    def get_account_summary(self):
        return self.get_account_endpoint('summary', 'account')
    

    def get_account_instruments(self):
        return self.get_account_endpoint('instruments', 'instruments')



# /// CANDLES ////////////////////////////////////////////////////////////
# ------------------------------------------------------------------------


    def fetch_candles(
            self, 
            symbol, 
            count=10, 
            granularity='H1', 
            price='MBA', 
            date_f=None, 
            date_t=None
            ):
        url = f'instruments/{symbol}/candles'
        params = dict(
            granularity = granularity,
            price = price
        )

        if date_f is not None and date_t is not None: #date_f = date from, date_t = date to
            date_format = '%Y-%m-%dT%H:%M:%SZ'
            params['from'] = dt.strftime(date_f, date_format)
            params['to'] = dt.strftime(date_t, date_format)
        else:
            params['count'] = count
        
        requestWorked, data = self.make_request(url, params=params)

        if requestWorked == True and 'candles' in data:
            return data['candles']
        else:
            print('ERROR fetch_candles()', params, data)
            return None


    def get_candles_df(self, symbol, **kwargs):

        data = self.fetch_candles(symbol, **kwargs)

        if data is None:
            return None 
        if len(data) == 0:
            return pd.DataFrame() #make empty dataframe

        prices = ['mid', 'bid', 'ask']
        ohlc = ['o', 'h', 'l', 'c']
        
        final_data = []
        for candle in data:
            if candle['complete'] == False:
                continue
            new_dict = {}
            new_dict['time'] = parser.parse(candle['time'])
            new_dict['volume'] = candle['volume']

            for price in prices:
                if price in candle:
                    for item in ohlc:
                        new_dict[f'{price}_{item}'] = float(candle[price][item])

            final_data.append(new_dict)
        df = pd.DataFrame.from_dict(final_data)
        return df