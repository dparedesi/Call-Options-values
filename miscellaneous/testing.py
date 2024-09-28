import yfinance as yf
import pandas as pd

def get_nasdaq_100():
    nasdaq100 = pd.read_html('https://en.wikipedia.org/wiki/Nasdaq-100')[4]
    tickers = nasdaq100['Ticker'].tolist()
    return tickers

def get_stock_data(tickers):
    data = {}
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            data[ticker] = {
                'market_cap': info.get('marketCap', 0),
                'dividend_yield': info.get('dividendYield', 0)
            }
        except:
            print(f"Could not fetch data for {ticker}")
    return data

def calculate_allocations(stock_data, total_investment=10000):
    total_market_cap = sum(data['market_cap'] for data in stock_data.values())
    for ticker, data in stock_data.items():
        weight = data['market_cap'] / total_market_cap
        allocation = weight * total_investment
        data['allocation'] = allocation
    return stock_data

def main():
    tickers = get_nasdaq_100()
    stock_data = get_stock_data(tickers)
    
    # Sort by market cap
    sorted_stock_data = dict(sorted(stock_data.items(), key=lambda item: item[1]['market_cap'], reverse=True))
    
    allocations = calculate_allocations(sorted_stock_data)
    
    # Prepare data for CSV
    csv_data = []
    for ticker, data in allocations.items():
        csv_data.append({
            'Ticker': ticker,
            'Allocation': f"${data['allocation']:.2f}",
            'Dividend Yield': f"{data['dividend_yield']*100:.2f}%" if data['dividend_yield'] else 'N/A'
        })
    
    # Create DataFrame and export to CSV
    df = pd.DataFrame(csv_data)
    df.to_csv('miscellaneous/nasdaq_100_allocations.csv', index=False)
    print("Data exported to nasdaq_100_allocations.csv")

if __name__ == "__main__":
    main()