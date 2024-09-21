import pandas as pd
import os
import asyncio
import aiohttp
from datetime import datetime

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

async def fetch_stock_data(session, ticker):
    try:
        # Fetch stock data
        async with session.get(f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=summaryDetail,price,defaultKeyStatistics,assetProfile") as response:
            data = await response.json()
            quote = data['quoteSummary']['result'][0]

        stock_price = quote['price']['regularMarketPrice']['raw']
        company_info = quote['assetProfile']
        summary_detail = quote['summaryDetail']
        
        company_description = company_info.get('longBusinessSummary', 'Description not available')
        pe_ratio = summary_detail.get('trailingPE', {}).get('raw', 'N/A')
        dividend_yield = summary_detail.get('dividendYield', {}).get('raw', 'N/A')
        if dividend_yield != 'N/A':
            dividend_yield = f"{dividend_yield:.2%}"
        
        forward_pe = summary_detail.get('forwardPE', {}).get('raw', 'N/A')
        market_cap = summary_detail.get('marketCap', {}).get('raw', 'N/A')
        if market_cap != 'N/A':
            if market_cap >= 1_000_000_000:
                market_cap = f"{market_cap / 1_000_000_000:.1f} B"
            elif market_cap >= 1_000_000:
                market_cap = f"{market_cap / 1_000_000:.1f} M"
            else:
                market_cap = f"{market_cap:,}"
        fifty_two_week_high = summary_detail.get('fiftyTwoWeekHigh', {}).get('raw', 'N/A')
        one_year_target = quote['defaultKeyStatistics'].get('targetMeanPrice', {}).get('raw', 'N/A')

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

        # Fetch options data
        async with session.get(f"https://query2.finance.yahoo.com/v7/finance/options/{ticker}") as response:
            options_data = await response.json()
            expiration_dates = options_data['optionChain']['result'][0]['expirationDates']
            
        expiration_date = max(expiration_dates)
        
        async with session.get(f"https://query2.finance.yahoo.com/v7/finance/options/{ticker}?date={expiration_date}") as response:
            option_chain_data = await response.json()
            calls = pd.DataFrame(option_chain_data['optionChain']['result'][0]['options'][0]['calls'])
        
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
            'Expiration Date': datetime.fromtimestamp(expiration_date).strftime('%Y-%m-%d'),
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

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_stock_data(session, ticker) for ticker in top_100_tickers]
        results = await asyncio.gather(*tasks)
        
    data = [result for result in results if result is not None]
    
    # Convert the list to a DataFrame
    df = pd.DataFrame(data)

    # Sort the DataFrame by 'Breakeven increase' in ascending order
    df = df.sort_values('Breakeven increase', ascending=True)

    # Save the sorted data to a CSV file
    file_path = os.path.join(os.getcwd(), 'top_100_stock_and_options_data.csv')
    df.to_csv(file_path, index=False, encoding='utf-8')
    print(f"File saved to: {file_path}")

if __name__ == "__main__":
    asyncio.run(main())