## PriceTape: A Python Financial Data Downloader

PriceTape is a simple and efficient Python tool designed to download and save financial market price data. It uses your chosen broker's API to fetch historical data for tickers and saves the information in compressed pickle (`.pkl`) files.

The goal of this project is to provide a reliable way for quantitative analysts, data scientists, and students to build a local cache of market data for backtesting, research, and analysis. By storing the data in `.pkl` format, you can easily load it into pandas DataFrames for later use, saving time and avoiding repeated API calls.

### Features
* **Easy to Use**: Simple functions to download data for a list of tickers.
* **Fast Storage**: Saves data in the efficient pickle format.
* **Organized**: Stores each ticker's data in its own compressed file.

Get started by cloning the repository and running the main script to build your local data vault.