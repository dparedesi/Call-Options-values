import yfinance as yf
import pandas as pd
import datetime

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

# Target expiration month and year
target_month, target_year = 1, 2026

#List of interest
top_100_tickers = [
    "ASTS", "FUBO", "RKLB", "UPST", "CVNA", "RUN", "SMCI", "CHWY", "NVDA",
    "HOOD", "DKNG", "CRWD", "PLTR", "SHOP", "CCJ", "CCL", "ON", "NU", "UBER",
    "TSM", "AVGO", "META", "NFLX", "CMG", "ZM", "AMZN", "GOOGL", "TGT",
    "MSFT", "AAPL", "V", "MA", "WMT", "JPM", "T", "KO", "ENPH", "SEDG",
    "UEC", "TMUS"
]

# Initialize an empty list to hold the data
data = []

# Total number of tickers
total_tickers = len(top_100_tickers)

# Loop through each ticker
for index, ticker in enumerate(top_100_tickers, 1):
    # Calculate and print progress
    progress = (index / total_tickers) * 100
    print(f"Loading... {progress:.2f}% completed", end='\r')

    try:
        # Fetch stock data
        stock = yf.Ticker(ticker)

        stock_price = stock.history(period='1d')['Close'].iloc[-1]

        # Fetch company description
        company_info = stock.info
        company_description = company_info.get('longBusinessSummary', 'Description not available')

        pe_ratio = company_info.get('trailingPE', 'N/A')
        dividend_yield = company_info.get('dividendYield', 'N/A')
        if dividend_yield != 'N/A':
            dividend_yield = f"{dividend_yield:.1%}"

        # New data points
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

        # Calculate 52-week-upside and 1y-target-upside
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

        # Fetch option expiration dates
        expiration_dates = stock.options

        # Find the last day of the target month
        if target_month == 12:
            next_month = datetime.datetime(target_year + 1, 1, 1)
        else:
            next_month = datetime.datetime(target_year, target_month + 1, 1)
        target_date = next_month - datetime.timedelta(days=1)
        valid_dates = [date for date in expiration_dates if datetime.datetime.strptime(date, '%Y-%m-%d') <= target_date]

        if not valid_dates:
            print(f"\nNo valid expiration date found for {ticker}")
            continue

        expiration_date = max(valid_dates)

        # Fetch the option chain for the expiration date
        option_chain = stock.option_chain(expiration_date)
        calls = option_chain.calls

        # Find the closest strike price to the current stock price
        calls['diff'] = abs(calls['strike'] - stock_price)
        closest_call = calls.loc[calls['diff'].idxmin()]

        # Calculate Breakeven increase
        breakeven_increase = (closest_call['lastPrice'] + closest_call['strike']) / stock_price - 1
        breakeven_increase_formatted = f"{breakeven_increase:.1%}"

        # Calculate Attractiveness
        attractiveness = (fifty_two_week_upside > breakeven_increase and
                          one_year_target_upside > breakeven_increase)

        # Add data to the list
        data.append({
            'Ticker': ticker,
            'Stock Price': stock_price,
            'Call Contract Price': closest_call['lastPrice'],
            'Strike Price': closest_call['strike'],
            'Expiration Date': expiration_date,
            'Breakeven increase': breakeven_increase_formatted,
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
        })

    except Exception as e:
        print(f"\nError fetching data for {ticker}: {e}")

# Print a newline to move to the next line after the progress indicator
print()

# Convert the list to a DataFrame
df = pd.DataFrame(data)

# Sort the DataFrame by 'Breakeven increase' in ascending order
df = df.sort_values('Breakeven increase', ascending=True)

# Save the sorted data to a CSV file (optional)
df.to_csv('top_100_stock_and_options_data.csv', index=False)

# Show the first few rows of the sorted DataFrame
# print(df[['Ticker', 'Stock Price', 'Breakeven increase', 'Company Description']].head())
