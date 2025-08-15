import pandas as pd
import datetime as dt
import time
import os
from dateutil import parser
from api import FxApi
from pathlib import Path


CANDLE_REQUEST_LIMIT = 900
LOCAL_FOLDER = Path(__file__).parent / "hist_quotes"

INCREMENTS = {  'M1' : 1    * CANDLE_REQUEST_LIMIT
              , 'M5' : 5    * CANDLE_REQUEST_LIMIT
              , 'M15': 15   * CANDLE_REQUEST_LIMIT
              , 'M30': 30   * CANDLE_REQUEST_LIMIT
              , 'H1' : 60   * CANDLE_REQUEST_LIMIT
              , 'H4' : 240  * CANDLE_REQUEST_LIMIT
              , 'D1' : 1440 * CANDLE_REQUEST_LIMIT
              }

SLEEP = 0.25


# /////////////////////////////////////////////////////////////////////////
# /// COLLECT CANDLES ////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////

def fetch_candles_df(symbol
                    , granularity
                    , date_start: dt.datetime
                    , api: FxApi 
                    ):

    candles_df = api.fetch_candles_as_df(symbol
                                        , granularity = granularity
                                        , count = CANDLE_REQUEST_LIMIT
                                        , date_start = date_start
                                        )

    if candles_df is not None and candles_df.empty == False:
        return candles_df
    else:
        print(f'fetch_candles_df() got no candles .')
        return None
    

def collect_candles(symbol
                    , granularity
                    , date_start
                    , date_end
                    , api: FxApi
                    , print_to_console = False
                    ):
    
    time_step = INCREMENTS[granularity]
    today = dt.date.today()
    yesterday = today - dt.timedelta(days=1)
    last_allowed_date = f'{yesterday.strftime("%Y-%m-%d")}T00:00:00'
    lad = parser.parse(last_allowed_date)

    final_date = parser.parse(date_end)
    from_date  = parser.parse(date_start)

    candles_df_list = []

    while (from_date < final_date) and (from_date < lad):
        to_date = from_date + dt.timedelta(minutes=time_step)
        to_date = lad if to_date > lad else to_date

        candles_df = fetch_candles_df(symbol
                                    , granularity
                                    , from_date
                                    , api
                                    )

        if candles_df is not None:
            candles_df_list.append(candles_df)

            msg = f"{symbol} {granularity}   >> "\
                  f"fetching {CANDLE_REQUEST_LIMIT} candles since: {from_date}   >> "\
                  f"got {candles_df.time.min()}  until  {candles_df.time.max()}   >> "\
                  f"total: {candles_df.shape[0]} candles"
            print(msg) if print_to_console else print(msg)

            if candles_df.time.max() > to_date:
                from_date = candles_df.time.max()
            else:
                from_date = to_date
        else:
            from_date = to_date
            msg = f"collect_candles() {symbol} {granularity} >> from: {from_date} to: {to_date} --> NO CANDLES"
            print(msg) if print_to_console else print(msg)
            
        time.sleep(SLEEP)

    if len(candles_df_list) > 0:
        complete_df = pd.concat(candles_df_list)
        complete_df = drop_extra_candles(complete_df, date_start, date_end)
        complete_df = drop_sort_df(complete_df)
        return True, complete_df

    else:
        msg = f'collect_candles() {symbol} {granularity} --> NO DATA RETURNED!'
        print(msg) if print_to_console else print(msg)
        return False, None


def collect_and_save_candles(symbol
                             , granularity
                             , date_start
                             , date_end
                             , api : FxApi
                             , print_to_console = False
                             ):
    ok, complete_df = collect_candles(symbol
                                      , granularity
                                      , date_start
                                      , date_end
                                      , api
                                      , print_to_console
                                      )
    if ok:
        saved = save_candles(complete_df
                             , symbol
                             , granularity
                             , print_to_console
                             )
        if saved:
            return True
    return False


def save_candles(complete_df
                 , symbol
                 , granularity
                 , print_to_console=False
                 ):
    msg = f'collect_candles() saving candles locally.'
    print(msg) if print_to_console else print(msg)
    saved = save_to_file(complete_df, granularity, symbol, print_to_console)
    if saved:
        return True
    else:
        return False


# /////////////////////////////////////////////////////////////////////////
# /// STORE LOCALLY //////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////

def save_to_file(complete_df: pd.DataFrame
                 , granularity
                 , symbol
                 , print_to_console = False
                 , local_folder = LOCAL_FOLDER
                 ):
    filename = f"{local_folder}/{symbol}_{granularity}.pkl"
    try:
        make_local_folder(local_folder)
        delete_previous_file(filename)
        complete_df.to_pickle(filename)

        s1 = f"*** SAVED {symbol}_{granularity} hist quotes   >> "\
            f"from: {complete_df.time.min()}   >> to: {complete_df.time.max()}"
        msg = f"{s1} --> total: {complete_df.shape[0]} candles ***"
        print(msg) if print_to_console else print(msg)
        return True
    except Exception as error:
        msg = f'Failed to save {symbol}_{granularity} to {filename}  --  Error: {error}'
        print(msg) if print_to_console else print(msg)
        return False


def load_from_file(symbol, granularity, local_folder = LOCAL_FOLDER):
    df = pd.read_pickle(f"{local_folder}/{symbol}_{granularity}.pkl")
    return df


def make_local_folder(local_folder = LOCAL_FOLDER):
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)


def delete_previous_file(filename):
    if os.path.exists(filename):
        os.remove(filename)


# /////////////////////////////////////////////////////////////////////////
# /// LOCAL STORAGE //////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////

# /// BATCH GET CANDLES //////////////////////////////////////////////////
# ------------------------------------------------------------------------


def get_hist_quotes(
    symbol_lst,
    granularity_lst,
    date_start,
    date_end,
    api
):

    for symbol in symbol_lst:

        print(f'Fetching data for {symbol}')
        for granularity in granularity_lst:
            start_time = time.time()
            print(f'Granularity: {granularity}')

            ok = collect_and_save_candles(
                symbol              = symbol, 
                granularity         = granularity, 
                date_start          = date_start, 
                date_end            = date_end, 
                api                 = api,
                print_to_console    = True
            )

            if ok:
                min_to_complete = (time.time() - start_time)/60
                print(f'Quotes saved for {symbol}_{granularity}, took {min_to_complete:.0f} minutes.')
            else:
                print(f'Error on {symbol}_{granularity}')


# /////////////////////////////////////////////////////////////////////////
# /// AUX FUNCTIONS //////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////

def drop_sort_df(df):
    df.drop_duplicates(subset=['time'], inplace=True)
    df.sort_values(by='time', inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def drop_extra_candles(df, date_start, date_end):
    start_date = parser.parse(date_start)
    end_date   = parser.parse(date_end)
    df = df[ (df['time'] >= start_date) & (df['time'] <= end_date)].copy()
    return df
