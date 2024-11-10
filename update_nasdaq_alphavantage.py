import requests
import pandas as pd
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Replace 'YOUR_API_KEY' with your free Alpha Vantage API key
#API_KEY = 'FKS5NZ1S4Z39PKCP' #dparedesi@uni.pe
API_KEY = 'CIUO9YWXH31RZOFM' #daparedes281@gmail.com
#API_KEY = 'XD31H2MN91ZDTP8K' #tinsels-lands05@icloud.com
#API_KEY = 'IALJM6QUYYO7TJX6'
#API_KEY = 'RE84E69TZ8EN8G6M'
#API_KEY = 'G25V5KUS20XB1G0D'
#API_KEY = 'S0DVGAQH1DW139XG'
#API_KEY = 'E435P6HH349L1Q9F'
TICKERS = ['MELI','FI','NOW','AMD','CI','BABA','HCA','SONY','MDLZ','GOOG','MRK','BUD',
           'TMO','SYK','PLD','AMT','AZN','SPOT','ETN','MCD','REGN','GS','NEE']
API_URL = 'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={}&apikey={}'

# Function to fetch and process data for a single ticker
def fetch_and_process_data(ticker):
    try:
        response = requests.get(API_URL.format(ticker, API_KEY))
        data = response.json()
        
        quarterly_reports = data.get('quarterlyReports', [])
        df = pd.DataFrame(quarterly_reports)
        
        desired_columns = ['fiscalDateEnding', 'totalRevenue', 'netIncome']
        available_columns = [col for col in desired_columns if col in df.columns]
        
        if len(available_columns) < len(desired_columns):
            missing_columns = set(desired_columns) - set(available_columns)
            logger.warning(f"Warning for {ticker}: The following columns are missing: {', '.join(missing_columns)}")
            logger.info(f"Available columns for {ticker}: {df.columns.tolist()}")
        
        df = df[available_columns]
        
        numeric_columns = ['totalRevenue', 'netIncome']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if 'fiscalDateEnding' in df.columns:
            df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'])
            five_years_ago = pd.Timestamp.now() - pd.DateOffset(years=5)
            df = df[df['fiscalDateEnding'] > five_years_ago]
        
        df['ticker'] = ticker
        return df
    except Exception as e:
        logger.error(f"Error processing {ticker}: {str(e)}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

# Process all tickers
all_data = []
for ticker in TICKERS:
    logger.info(f"Processing {ticker}...")
    ticker_data = fetch_and_process_data(ticker)
    if not ticker_data.empty:
        all_data.append(ticker_data)
    time.sleep(12)  # To avoid hitting API rate limits (5 calls per minute)

# Combine all data
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)

    # Save the combined DataFrame to a CSV file
    csv_filename = "ad-hoc-report-alphavantage.csv"
    combined_df.to_csv(csv_filename, index=False)
    logger.info(f"Data saved to {csv_filename}")
else:
    logger.warning("No data was successfully processed. No CSV file was created.")
