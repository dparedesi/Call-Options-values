import yfinance as yf
from datetime import datetime

def get_ex_dividend_dates(stocks):
    ex_dividend_dates = {}
    for stock in stocks:
        try:
            ticker = yf.Ticker(stock)
            info = ticker.info
            ex_dividend_date = info.get('exDividendDate')
            if ex_dividend_date:
                # Convert Unix timestamp to a readable date format
                date = datetime.utcfromtimestamp(ex_dividend_date).strftime('%Y-%m-%d')
                ex_dividend_dates[stock] = date
            else:
                ex_dividend_dates[stock] = "No ex-dividend date available"
        except Exception as e:
            ex_dividend_dates[stock] = f"Error: {str(e)}"
    return ex_dividend_dates

# Example usage
stocks = ['JPM', 'KSPI']  # Add more stocks to this list as needed
ex_dividend_dates = get_ex_dividend_dates(stocks)

for stock, date in ex_dividend_dates.items():
    print(f"{stock}: {date}")
