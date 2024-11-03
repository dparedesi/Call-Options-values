import yfinance as yf
import pandas as pd
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
import requests
from datetime import datetime
from pandas.tseries.offsets import BDay

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
    logger.info('Starting batch fetch...')
    session = create_session()
    try:
        data = yf.Tickers(tickers, session=session)
        if not data:
            logger.error("Failed to fetch data from Yahoo Finance")
            return None
        return data
    except Exception as e:
        logger.error(f"Error in fetch_batch_data: {e}")
        return None

def process_stock_data(ticker, yf_data, max_common_date):
    try:
        stock = yf_data.tickers[ticker]
        
        # Get the last business day
        last_business_day = (datetime.now() - BDay(1)).strftime('%Y-%m-%d')
        
        # Try getting 5 days of history
        history = stock.history(start=last_business_day)
        if history.empty:
            logger.warning(f"No historical data available for {ticker}")
            return None
            
        stock_price = history['Close'].iloc[-1]
        
        # Add a longer delay between API calls
        time.sleep(0.5)
        
        try:
            company_info = stock.info
        except Exception as e:
            logger.error(f"Error fetching company info for {ticker}: {e}")
            return None
        
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

        # Get options data
        try:
            expiration_dates = stock.options
            if not expiration_dates:
                logger.warning(f"No options available for {ticker}")
                return None
                
            if max_common_date and max_common_date in expiration_dates:
                expiration_date = max_common_date
            else:
                expiration_date = max(expiration_dates)
            
            option_chain = stock.option_chain(expiration_date)
            calls = option_chain.calls
            
            if calls.empty:
                logger.warning(f"No call options available for {ticker}")
                return None
                
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
            logger.error(f"Error processing options data for {ticker}: {e}")
            return None
            
    except Exception as e:
        logger.error(f"Error processing data for {ticker}: {e}")
        return None

def process_in_batches(tickers, batch_size=50):
    all_data = []
    
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        logger.info(f"Processing batch {i//batch_size + 1} of {len(tickers)//batch_size + 1}")
        
        yf_data = fetch_batch_data(batch)
        if yf_data is None:
            continue
            
        # Find common expiration dates for this batch
        all_expiration_dates = []
        for ticker in batch:
            try:
                dates = yf_data.tickers[ticker].options
                if dates:
                    all_expiration_dates.append(set(dates))
            except Exception as e:
                logger.error(f"Error getting options dates for {ticker}: {e}")
                
        if all_expiration_dates:
            common_dates = set.intersection(*all_expiration_dates)
            max_common_date = max(common_dates) if common_dates else None
        else:
            max_common_date = None
            
        # Process each ticker in the batch
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_ticker = {
                executor.submit(process_stock_data, ticker, yf_data, max_common_date): ticker 
                for ticker in batch
            }
            
            for future in as_completed(future_to_ticker):
                result = future.result()
                if result:
                    all_data.append(result)
                    
        time.sleep(1)  # Delay between batches
        
    return all_data

def main():
    logger.info("Starting stock data collection...")
    
    # Process all tickers in batches
    data = process_in_batches(top_100_tickers, batch_size=50)
    
    if not data:
        logger.error("No data was collected")
        return
        
    # Create a DataFrame from the collected data
    df = pd.DataFrame(data)
    
    # Sort the DataFrame by attractiveness and breakeven increase
    df = df.sort_values(by=['Expiration Date', 'Breakeven increase'], ascending=[False, True])
    
    # Save the DataFrame to a CSV file
    file_name = "top_100_stock_and_options_data.csv"
    file_path = os.path.join(script_dir, file_name)
    df.to_csv(file_path, index=False)
    
    logger.info(f"File saved to: {file_path}")

if __name__ == "__main__":
    main()