import requests
import pandas as pd
import time

# Replace 'YOUR_API_KEY' with your free Alpha Vantage API key
#API_KEY = 'FKS5NZ1S4Z39PKCP' #dparedesi@uni.pe
#API_KEY = 'CIUO9YWXH31RZOFM' #daparedes281@gmail.com
API_KEY = 'XD31H2MN91ZDTP8K' #tinsels-lands05@icloud.com
TICKERS = ['CRWD', 'NVDA', 'MSFT', 'GOOGL', 'ENPH', 'JPM', 'IFS', 'AMZN', 'AVGO', 'UEC', 'VZ', 'NU', 'CCJ',
           'T', 'CCL', 'TMUS', 'PM', 'IBM', 'C', 'KMB', 'IBKR', 'NFLX', 'V', 'MA', 'SHOP', 'MSTF','TCOM', 'RKLB', 'ULTA', 'BAP']
TICKERS = ['CRWD', 'NVDA', 'MSFT', 'GOOGL', 'ENPH', 'JPM', 'IFS', 'AMZN', 'IBM', 'C', 'KMB', 
          'IBKR', 'NFLX', 'V', 'AVGO', 'UEC', 'VZ', 'NU', 'CCJ', 'T', 'CCL', 'TMUS', 'PM']
TICKERS = ['MA', 'SHOP', 'TCOM', 'RKLB', 'ULTA', 'BAP', 'AAPL', 'TSLA','META', 'JNJ', 'PG',
           "BAC", 'KO', 'PEP', 'ZK','LI','CELH','BBAR','RCL','DKNG','FCNCA','TOST','BILL','PDD']
TICKERS = ['KKR','JKS','SUPV','KSPI','TEO','E','ET','PFGC','ASAI','NVO','SHEL',
           'DE','ENIC','AXP','ASX','TSM','CAT','TKC','ITUB','PGR','COST','ELV']
TICKERS = ['F','COR','ABEV', 'TM','DIS','BRFS','MUFG','HDB','TLK','KT', 'GGAL', 'BX', 'APO', 'CL']
TICKERS = ['CLF','WBD','CCL','CUK','UBER','NRG','BKNG','LUV','DINO','PAC','PBF','TBBB','TCOM',
           'LAD','IMO','EOG','HEPS','WKC','CNQ','AMD','SQ','MAR','LNG']
TICKERS = ['SUN','HTHT','PAA','PAGP','ASR','CPNG','ASML','CSAN','MUSA','BILI','PCAR','PWR',
           'EPD','WCC','MOH','DHI','SYT','NUE','AZN','CMI','HMY']
TICKERS = ['CNH','MA','GT','NVDA','FCX','HAL','OXY','NGG','KMX','NEE','USFD','AMAT','APTV',
           'SBUX','ZTO','QCOM','LEN','SID','BBVA','AVGO','V','FERG','LYB','CRM']
API_URL = 'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={}&apikey={}'

# Function to fetch and process data for a single ticker
def fetch_and_process_data(ticker):
    response = requests.get(API_URL.format(ticker, API_KEY))
    data = response.json()
    
    quarterly_reports = data.get('quarterlyReports', [])
    df = pd.DataFrame(quarterly_reports)
    
    desired_columns = ['fiscalDateEnding', 'totalRevenue', 'netIncome']
    available_columns = [col for col in desired_columns if col in df.columns]
    
    if len(available_columns) < len(desired_columns):
        missing_columns = set(desired_columns) - set(available_columns)
        print(f"Warning for {ticker}: The following columns are missing: {', '.join(missing_columns)}")
        print(f"Available columns for {ticker}:", df.columns.tolist())
    
    df = df[available_columns]
    
    numeric_columns = ['totalRevenue', 'netIncome']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert fiscalDateEnding to datetime and filter for the last 5 years
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'])
    five_years_ago = pd.Timestamp.now() - pd.DateOffset(years=5)
    df = df[df['fiscalDateEnding'] > five_years_ago]
    
    df['ticker'] = ticker
    return df

# Process all tickers
all_data = []
for ticker in TICKERS:
    print(f"Processing {ticker}...")
    ticker_data = fetch_and_process_data(ticker)
    all_data.append(ticker_data)
    time.sleep(12)  # To avoid hitting API rate limits (5 calls per minute)

# Combine all data
combined_df = pd.concat(all_data, ignore_index=True)


# Save the combined DataFrame to a CSV file
csv_filename = "combined_financial_data.csv"
combined_df.to_csv(csv_filename, index=False)
print(f"\nData saved to {csv_filename}")
