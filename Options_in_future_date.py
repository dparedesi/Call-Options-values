
import yfinance as yf
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# List of top 100 tickers (sample list of tickers, you can adjust)
top_100_tickers = ["GOOGL", "AAPL", "AMZN", "NVDA", "NU", "ASTS", "RKLB", "CRWD", "PLTR", "AVGO",
                   "PDD", "INTC", "MNTS", "MSTR", "TSLA", "AMD", "MARA", "META", "PTON", "V",
                   "MA", "BABA", "SOFI", "SMCI", "RIVN", "MU", "AMC", "MSFT", "LCID", "SNOW",
                   "BAC", "COIN", "CAVA", "F", "HOOD", "FCX", "RDFN", "RIOT", "PFE",
                   "GME", "C", "CLSK", "OXY", "SIRI", "CCL", "PYPL", "SBUX", "ROKU", "CHPT",
                   "UPST", "GM", "LUMN", "ZM", "GLNG", "AFRM", "JD", "TSM", "DKNG",
                   "WBD", "XOM", "DIS", "NKE", "WMT", "UBER", "CCJ", "T", "BA", "ARM", "JPM",
                   "CLOV", "PBR", "AHCO", "SNAP", "CSCO", "RILY", "AAL", "NFLX", "TGT", "CVNA",
                   "MMM", "CHWY", "WDAY", "WFC", "FUBO", "CMG", "GOLD", "X", "DAL", "BILI",
                   "CELH", "WBA", "PARA", "AAP", "SHOP", "RUN", "LUV", "GS",
                   "KO", "SQ", "DELL", "ON", "ENPH", "SEDG", "UEC", "TMUS"]

# List of interest tickers
top_100_tickers = [
    "AAPL", "ABEV", "AMZN", "ASTS", "AVGO", "AXP", "AXON", "BAP", "BBAR",
    "C", "CAT", "CHWY", "CMG", "COST", "CRWD", "E", "ENIC", "ENPH", "ET",
    "F", "GGAL", "GOOGL", "HOOD", "IBM", "JPM", "KMB", "KO", "KSPI", "KT",
    "LLOY", "MA", "META", "MSFT", "NFLX", "NVO", "NU", "NVDA", "PLTR", "PM",
    "RKLB", "SEDG", "SHEL", "SHOP", "SMCI", "T", "TARGET", "TLK", "TM",
    "TMUS", "TSM", "UBER", "V", "VZ", "WMT"
]

def fetch_stock_data(ticker, max_common_date):
    try:
        stock = yf.Ticker(ticker)
        stock_price = stock.history(period='1d')['Close'].iloc[-1]
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
        print(f"\nError fetching data for {ticker}: {e}")
        return None

# Find the maximum common expiration date
all_expiration_dates = []
for ticker in top_100_tickers:
    try:
        stock = yf.Ticker(ticker)
        all_expiration_dates.append(set(stock.options))
    except Exception as e:
        print(f"Error fetching expiration dates for {ticker}: {e}")

common_dates = set.intersection(*all_expiration_dates)
if len(common_dates) > 0:
    max_common_date = max(common_dates)
else:
    date_counts = {}
    for dates in all_expiration_dates:
        for date in dates:
            date_counts[date] = date_counts.get(date, 0) + 1

    threshold = 0.8 * len(top_100_tickers)
    eligible_dates = [date for date, count in date_counts.items() if count >= threshold]

    if eligible_dates:
        max_common_date = max(eligible_dates)
    else:
        print("No date is available for at least 80% of tickers. Using individual max dates.")
        max_common_date = None

# Use ThreadPoolExecutor for parallel processing
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_stock_data, ticker, max_common_date) for ticker in top_100_tickers]
    
    data = []
    for future in as_completed(futures):
        result = future.result()
        if result:
            data.append(result)
        
        # Calculate and print progress
        progress = (len(data) / len(top_100_tickers)) * 100
        print(f"Loading... {progress:.2f}% completed", end='\r')

# Print a newline to move to the next line after the progress indicator
print()

# Convert the list to a DataFrame
df = pd.DataFrame(data)

# Sort the DataFrame by 'Breakeven increase' in ascending order
df = df.sort_values('Breakeven increase', ascending=True)

# Save the sorted data to a CSV file
file_path = os.path.join(os.getcwd(), 'top_100_stock_and_options_data.csv')
df.to_csv(file_path, index=False, encoding='utf-8')
print(f"File saved to: {file_path}")