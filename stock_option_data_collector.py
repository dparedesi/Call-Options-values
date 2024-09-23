import yfinance as yf
import pandas as pd
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
import requests

# Set up logging
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=getattr(logging, log_level), format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Read tickers from the CSV file
script_dir = os.path.dirname(os.path.abspath(__file__))
watchlist_path = os.path.join(script_dir, 'inputs', 'watchlist.csv')
watchlist_df = pd.read_csv(watchlist_path)
top_100_tickers = watchlist_df['tickers'].tolist()

def create_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100))
    return session

def fetch_batch_data(tickers):
    logger.info('Starting...')
    session = create_session()
    return yf.Tickers(tickers, session=session)

def process_stock_data(ticker, yf_data, max_common_date):
    try:
        stock = yf_data.tickers[ticker]
        stock_price = stock.history(period='1d')['Close'].iloc[-1]
        
        # Add a delay between API calls
        time.sleep(0.1)
        
        company_info = stock.info
        
        company_description = company_info.get('longBusinessSummary', 'Description not available')
        pe_ratio = company_info.get('trailingPE', 'N/A')
        dividend_yield = company_info.get('dividendYield', 'N/A')
        if dividend_yield != 'N/A':
            dividend_yield = f"{dividend_yield:.2%}"
        
        forward_pe = company_info.get('forwardPE', 'N/A')
        market_cap = company_info.get('marketCap', 'N/A')
        if market_cap != 'N/A':
            if market_cap >= 1_000_000_000:
                market_cap = f"{market_cap / 1_000_000_000:.1f} B"
            elif market_cap >= 1_000_000:
                market_cap = f"{market_cap / 1_000_000:.1f} M"
            else:
                market_cap = f"{market_cap:,}"
        fifty_two_week_high = company_info.get('fiftyTwoWeekHigh', 'N/A')
        one_year_target = company_info.get('targetMeanPrice', 'N/A')

        if fifty_two_week_high != 'N/A' and stock_price:
            fifty_two_week_upside = (fifty_two_week_high / stock_price) - 1
            fifty_two_week_upside_formatted = f"{fifty_two_week_upside:.1%}"
        else:
            fifty_two_week_upside_formatted = 'N/A'

        if one_year_target != 'N/A' and stock_price:
            one_year_target_upside = (one_year_target / stock_price) - 1
            one_year_target_upside_formatted = f"{one_year_target_upside:.1%}"
        else:
            one_year_target_upside_formatted = 'N/A'

        expiration_dates = stock.options
        if max_common_date and max_common_date in expiration_dates:
            expiration_date = max_common_date
        else:
            expiration_date = max(expiration_dates)
        
        option_chain = stock.option_chain(expiration_date)
        calls = option_chain.calls
        
        calls['diff'] = abs(calls['strike'] - stock_price)
        closest_call = calls.loc[calls['diff'].idxmin()]
        
        breakeven_increase = (closest_call['lastPrice'] + closest_call['strike']) / stock_price - 1
        
        attractiveness = (fifty_two_week_upside > breakeven_increase and
                          one_year_target_upside > breakeven_increase)
        
        return {
            'Ticker': ticker,
            'Stock Price': stock_price,
            'Call Contract Price': closest_call['lastPrice'],
            'Strike Price': closest_call['strike'],
            'Expiration Date': expiration_date,
            'Breakeven increase': breakeven_increase,
            'Company Description': company_description,
            'P/E Ratio': pe_ratio,
            'Forward P/E': forward_pe,
            'Market Cap': market_cap,
            '52 Week High': fifty_two_week_high,
            '52-week-upside': fifty_two_week_upside_formatted,
            '1y Target Est': one_year_target,
            '1y-target-upside': one_year_target_upside_formatted,
            'Dividend Yield': dividend_yield,
            'Attractiveness': attractiveness
        }
    except Exception as e:
        logger.error(f"Error processing data for {ticker}: {e}")
        return None

# Fetch all data in a single API call
yf_data = fetch_batch_data(top_100_tickers)
logger.info('Data fetched in batch')

def fetch_expiration_dates(ticker):
    try:
        return set(yf_data.tickers[ticker].options)
    except Exception as e:
        logger.error(f"Error fetching expiration dates for {ticker}: {e}")
        return set()

# Find the maximum common expiration date
all_expiration_dates = []
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(fetch_expiration_dates, ticker): ticker for ticker in top_100_tickers}
    for future in as_completed(futures):
        all_expiration_dates.append(future.result())

logger.info('Extracted expiration dates')

common_dates = set.intersection(*all_expiration_dates)
if len(common_dates) > 0:
    max_common_date = max(common_dates)
else:
    date_counts = {}
    for dates in all_expiration_dates:
        for date in dates:
            date_counts[date] = date_counts.get(date, 0) + 1

    threshold = 0.6 * len(top_100_tickers)
    eligible_dates = [date for date, count in date_counts.items() if count >= threshold]

    if eligible_dates:
        max_common_date = max(eligible_dates)
    else:
        logger.warning("No date is available for at least 60% of tickers. Using individual max dates.")
        max_common_date = None

# Process the data
data = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(process_stock_data, ticker, yf_data, max_common_date) for ticker in top_100_tickers]
    
    for future in as_completed(futures):
        result = future.result()
        if result:
            data.append(result)
        
        # Calculate and log progress
        progress = (len(data) / len(top_100_tickers)) * 100
        logger.debug(f"Processing... {progress:.2f}% completed")

# Create a DataFrame from the collected data
df = pd.DataFrame(data)

# Sort the DataFrame by attractiveness and breakeven increase
df = df.sort_values(by=['Expiration Date', 'Breakeven increase'], ascending=[False, True])

# Save the DataFrame to a CSV file in the same directory as the script
script_dir = os.path.dirname(os.path.abspath(__file__))
file_name = "top_100_stock_and_options_data.csv"
file_path = os.path.join(script_dir, file_name)
df.to_csv(file_path, index=False)

logger.info(f"File saved to: {file_path}")

