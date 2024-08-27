import asyncio
import aiohttp
import yfinance as yf
import pandas as pd
from functools import lru_cache
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from datetime import datetime

MAX_EXPIRATION_DATES = 100
CACHE_SIZE = 128

@lru_cache(maxsize=CACHE_SIZE)
def get_stock_options(ticker):
    stock = yf.Ticker(ticker)
    return stock, stock.options[:MAX_EXPIRATION_DATES]


def get_current_price(stock):
    return stock.history(period="1d")['Close'].iloc[-1]


async def fetch_option_data(session, stock, date, current_price, ticker):
    try:
        option_chain = await asyncio.to_thread(stock.option_chain, date)
        calls = option_chain.calls[
            ['contractSymbol', 'strike', 'lastPrice', 'bid', 'ask', 'volume', 'openInterest', 'impliedVolatility']]
        calls['expirationDate'] = date
        calls['ticker'] = ticker
        calls['currentPrice'] = current_price  # Add current stock price column
        calls['breakeven'] = calls['strike'] + calls['lastPrice']  # Add breakeven column
        calls['breakeven_increase'] = calls['breakeven'] / current_price - 1  # Add breakeven increase column

        # Filter strike prices
        lower_bound = current_price * 0.7
        upper_bound = current_price * 1.05
        filtered_calls = calls[(calls['strike'] >= lower_bound) & (calls['strike'] <= upper_bound)]

        return filtered_calls
    except Exception as e:
        print(f"Error fetching data for {ticker} on date {date}: {e}")
        return pd.DataFrame()


async def get_high_volume_call_options(ticker):
    stock, expiration_dates = get_stock_options(ticker)
    current_price = get_current_price(stock)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_option_data(session, stock, date, current_price, ticker) for date in expiration_dates]
        calls_data = await asyncio.gather(*tasks)

    calls_df = pd.concat([df for df in calls_data if not df.empty], ignore_index=True)

    if calls_df.empty:
        print(f"No valid data found for {ticker}")
        return pd.DataFrame()

    # Filter out strike prices with less than 50% of expiration dates
    min_expiration_dates = len(expiration_dates) // 2
    strike_counts = calls_df.groupby('strike')['expirationDate'].nunique()
    valid_strikes = strike_counts[strike_counts >= min_expiration_dates].index
    filtered_calls_df = calls_df[calls_df['strike'].isin(valid_strikes)]

    sorted_calls = filtered_calls_df.sort_values(by='volume', ascending=False)
    return sorted_calls


async def process_ticker(ticker):
    top_call_options = await get_high_volume_call_options(ticker)

    if not top_call_options.empty:
        print(f"\nTop 10 call options for {ticker}:")
        #print(top_call_options.head(10))
    else:
        print(f"No data available for {ticker}")

    return top_call_options


async def main():
    tickers = ["GOOG", "AAPL", "AMZN", "NVDA", "NU", "ASTS", "RKLB", "CRWD", "PLTR", "AVGO", "PDD", "INTC", "MNTS", "MSTR", "TSLA", "AMD", "MARA", "META", "PTON", "V", "MA"]
    output_file = "all_call_options.xlsx"

    start_time = asyncio.get_event_loop().time()

    tasks = [process_ticker(ticker) for ticker in tickers]
    results = await asyncio.gather(*tasks)

    all_call_options = pd.concat(results, ignore_index=True)
    all_call_options.sort_values(by=['ticker', 'volume'], ascending=[True, False], inplace=True)

    # Find the latest common expiration date
    all_call_options['expirationDate'] = pd.to_datetime(all_call_options['expirationDate'])
    latest_common_date = all_call_options.groupby('ticker')['expirationDate'].max().min()

    # Filter options for the latest common expiration date
    latest_options = all_call_options[all_call_options['expirationDate'] == latest_common_date]

    # Create a new workbook and add the first sheet with all data
    wb = Workbook()
    ws1 = wb.active
    ws1.title = "All Call Options"
    for r in dataframe_to_rows(all_call_options, index=False, header=True):
        ws1.append(r)

    # Create the second sheet with closest-to-current-price contracts for the latest common expiration date
    ws2 = wb.create_sheet(title=f"Closest to Current Price ({latest_common_date.strftime('%Y-%m-%d')})")
    closest_options = latest_options.loc[latest_options.groupby('ticker')['strike'].apply(lambda x: (x - latest_options.loc[x.index, 'currentPrice']).abs().idxmin())]
    for r in dataframe_to_rows(closest_options, index=False, header=True):
        ws2.append(r)

    # Save the workbook
    wb.save(output_file)
    print(f"\nAll data saved to {output_file}")

    end_time = asyncio.get_event_loop().time()
    elapsed_time = end_time - start_time

    print(f"\nExecution time: {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())