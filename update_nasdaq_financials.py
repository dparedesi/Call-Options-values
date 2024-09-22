import concurrent.futures
import yfinance as yf
import pandas as pd
import time
import random
import logging
from tqdm import tqdm
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read the tickers from the CSV file
tickers_df = pd.read_csv('inputs/biggest_nasdaq_tickers.csv')
TICKERS = tickers_df['tickers'].tolist()

# Read the existing data
try:
    existing_data = pd.read_csv('financials-historical.csv')
    existing_data['fiscalDateEnding'] = pd.to_datetime(existing_data['fiscalDateEnding'])
except FileNotFoundError:
    existing_data = pd.DataFrame(columns=['fiscalDateEnding', 'totalRevenue', 'netIncome', 'ticker'])

# Function to fetch and process data for a single ticker
def fetch_and_process_ticker(ticker):
    try:
        stock = yf.Ticker(ticker)
        financials = stock.quarterly_financials
        
        if financials.empty:
            logging.debug(f"No financial data available for {ticker}")
            return None
        
        df = pd.DataFrame({
            'fiscalDateEnding': financials.columns,
            'totalRevenue': financials.loc['Total Revenue'],
            'netIncome': financials.loc['Net Income'],
            'ticker': ticker
        })
        
        return df.sort_values('fiscalDateEnding', ascending=False)
    except Exception as e:
        logging.error(f"Error processing {ticker}: {str(e)}")
        return None

# Function to fetch and process data for a batch of tickers
def fetch_and_process_batch(tickers):
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_and_process_ticker, tickers))
    return [df for df in results if df is not None]

# Process tickers with rate limiting
batch_size = 500  # Reduced batch size
pause_time = 2  # Reduced pause time
max_retries = 3
all_new_data = []

# Create a progress bar for the entire process
with tqdm(total=len(TICKERS), desc="Processing tickers") as pbar:
    for i in range(0, len(TICKERS), batch_size):
        batch = TICKERS[i:i+batch_size]
        logging.info(f"Processing batch {i//batch_size + 1} of {len(TICKERS)//batch_size + 1}...")
        
        for attempt in range(max_retries):
            try:
                batch_data = fetch_and_process_batch(batch)
                all_new_data.extend(batch_data)
                pbar.update(len(batch))
                break
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1} failed for batch: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 5))
                else:
                    logging.error(f"Max retries reached for batch. Skipping.")
        
        # Save intermediate results
        if len(all_new_data) > 0:
            intermediate_df = pd.concat(all_new_data, ignore_index=True)
            intermediate_df.to_csv(f"intermediate_results_{i}.csv", index=False)
            logging.info(f"Intermediate results saved to intermediate_results_{i}.csv")
        
        time.sleep(pause_time)

# Combine all new data
new_data = pd.concat(all_new_data, ignore_index=True)

# Merge new data with existing data
combined_df = pd.concat([existing_data, new_data], ignore_index=True)

# Remove duplicates, keeping the latest data
combined_df = combined_df.sort_values('fiscalDateEnding', ascending=False)
combined_df = combined_df.drop_duplicates(subset=['ticker', 'fiscalDateEnding'], keep='first')

# Delete rows where either revenue or income is empty
combined_df = combined_df.dropna(subset=['totalRevenue', 'netIncome'], how='any')

# Sort the dataframe
combined_df = combined_df.sort_values(['ticker', 'fiscalDateEnding'], ascending=[True, False])

# Save the combined DataFrame to a CSV file
csv_filename = "financials-historical.csv"
combined_df.to_csv(csv_filename, index=False)
logging.info(f"Data saved to {csv_filename}")

# Delete intermediate files
for i in range(0, len(TICKERS), batch_size):
    intermediate_file = f"intermediate_results_{i}.csv"
    if os.path.exists(intermediate_file):
        os.remove(intermediate_file)
        logging.info(f"Deleted intermediate file: {intermediate_file}")

# Print summary statistics
total_tickers = len(TICKERS)
processed_tickers = combined_df['ticker'].nunique()
logging.info(f"Total tickers: {total_tickers}")
logging.info(f"Processed tickers: {processed_tickers}")
logging.info(f"Tickers with data: {processed_tickers / total_tickers:.2%}")