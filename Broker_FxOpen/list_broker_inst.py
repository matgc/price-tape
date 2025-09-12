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