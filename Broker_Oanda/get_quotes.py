import pandas as pd
import datetime as dt
import pytz
import time
import os
from dateutil import parser
from api import OandaApi
from pathlib import Path


CANDLE_REQUEST_LIMIT = 3000
LOCAL_FOLDER = Path(__file__).parent / "hist_quotes"

INCREMENTS = {  'M1' : 1    * CANDLE_REQUEST_LIMIT
              , 'M5' : 5    * CANDLE_REQUEST_LIMIT
              , 'M15': 15   * CANDLE_REQUEST_LIMIT
              , 'M30': 30   * CANDLE_REQUEST_LIMIT
              , 'H1' : 60   * CANDLE_REQUEST_LIMIT
              , 'H2' : 120  * CANDLE_REQUEST_LIMIT
              , 'H4' : 240  * CANDLE_REQUEST_LIMIT
              , 'D'  : 1440 * CANDLE_REQUEST_LIMIT
              }

SLEEP = 0.25


# /////////////////////////////////////////////////////////////////////////
# /// COLLECT CANDLES ////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////

def attempt_fetching_candles_as_df(symbol
                                   , granularity
                                   , date_f: dt.datetime
                                   , date_t: dt.datetime
                                   , api: OandaApi 
                                   ):
    ATTEMPTS_LIMIT = 3
    attempts_made = 0

    while attempts_made < ATTEMPTS_LIMIT:

        candles_df = api.get_candles_df(
            symbol = symbol,
            granularity = granularity,
            date_f = date_f,
            date_t = date_t
        )

        if candles_df is not None:
            break
        attempts_made += 1

    if candles_df is not None and candles_df.empty == False:
        return candles_df
    else:
        print(f'attempt_fetching_candles_as_df() got no candles after {ATTEMPTS_LIMIT} attemps.')
        return None
    

def collect_candles(symbol
                    , granularity
                    , date_start
                    , date_end
                    , api: OandaApi
                    , print_to_console = False
                    ):
    
    time_step = INCREMENTS[granularity]

    date_end = parser.parse(date_end).replace(tzinfo=pytz.UTC)
    date_start  = parser.parse(date_start).replace(tzinfo=pytz.UTC)

    candles_df_list = []

    from_date = date_start
    while from_date < date_end:
        to_date = from_date + dt.timedelta(minutes=time_step)
        candles_df = attempt_fetching_candles_as_df(symbol
                                                    , granularity
                                                    , from_date
                                                    , to_date
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
                             , api : OandaApi
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


def get_hist_quotes(symbol_lst,
                      granularity_lst,
                      date_start,
                      date_end,
                      api
                      ):

    for symbol in symbol_lst:

        print(f'Fetching data for {symbol}')
        for granularity in granularity_lst:
            print(f'Granularity: {granularity}')

            ok = collect_and_save_candles(
                                            symbol             = symbol, 
                                            granularity        = granularity, 
                                            date_start         = date_start, 
                                            date_end           = date_end, 
                                            api                = api,
                                            print_to_console   = True
                                            )

            if ok:
                print(f'Quotes saved for {symbol}_{granularity}')
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
    df = df[ (df['time'] >= date_start) & (df['time'] <= date_end)].copy()
    return df
