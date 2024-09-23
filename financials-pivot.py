import pandas as pd
import numpy as np
from scipy import stats
import yfinance as yf
import contextlib
import io

def clean_revenue(revenue):
    if isinstance(revenue, str):
        return float(revenue.replace("$", "").replace(",", ""))
    return float(revenue)

def calculate_metrics(df):
    # Convert fiscalDateEnding to datetime with the correct format
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'], format='%Y-%m-%d')
    
    # Sort by date
    df = df.sort_values('fiscalDateEnding')
    
    # Calculate days since first entry for x-axis
    df['days'] = (df['fiscalDateEnding'] - df['fiscalDateEnding'].min()).dt.days
    
    # Function to safely calculate slope and R-squared
    def safe_linregress(x, y):
        mask = ~(np.isnan(x) | np.isnan(y))
        valid_data = np.sum(mask)
        if valid_data > 2:  # We need at least 3 points for a meaningful regression
            with np.errstate(all='ignore'):
                slope, _, r_value, _, _ = stats.linregress(x[mask], y[mask])
                r_squared = r_value**2 if not np.isnan(r_value) else np.nan
            return slope, r_squared
        return np.nan, np.nan

    # Calculate revenue metrics
    revenue_slope, revenue_r_squared = safe_linregress(df['days'], df['totalRevenue'])
    
    # Calculate net income metrics
    income_slope, income_r_squared = safe_linregress(df['days'], df['netIncome'])
    
    # Calculate combined R-squared
    combined_r_squared = revenue_r_squared * income_r_squared if not np.isnan(revenue_r_squared) and not np.isnan(income_r_squared) else np.nan
    
    # Calculate correlation between revenue and net income
    valid_data = df[['totalRevenue', 'netIncome']].dropna()
    if len(valid_data) > 1:  # We need at least 2 points for correlation
        with np.errstate(all='ignore'):
            revenue_income_correlation = valid_data['totalRevenue'].corr(valid_data['netIncome'])
    else:
        revenue_income_correlation = np.nan
    
    # Calculate percentage change in revenue
    oldest_revenue = df.iloc[0]['totalRevenue']
    newest_revenue = df.iloc[-1]['totalRevenue']
    
    if oldest_revenue != 0 and pd.notna(oldest_revenue) and pd.notna(newest_revenue):
        revenue_change = ((newest_revenue - oldest_revenue) / oldest_revenue)
    else:
        revenue_change = np.nan

    return pd.Series({
        'Ticker': df['ticker'].iloc[0],
        'Oldest Date': df['fiscalDateEnding'].min().strftime('%Y-%m-%d'),
        'Oldest Revenue': oldest_revenue,
        'Newest Date': df['fiscalDateEnding'].max().strftime('%Y-%m-%d'),
        'Newest Revenue': newest_revenue,
        '%Change Revenue': revenue_change,
        'Revenue Slope': revenue_slope,
        'Revenue R²': revenue_r_squared,
        'Net Income Slope': income_slope,
        'Net Income R²': income_r_squared,
        'Combined R²': combined_r_squared,
        'Revenue-Income Correlation': revenue_income_correlation
    })

def get_yfinance_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        return info.get('dividendYield', np.nan), info.get('targetMeanPrice', np.nan)
    except:
        return np.nan, np.nan

def main():
    # Read the CSV file
    df = pd.read_csv('financials-historical.csv')

    # Read the watchlist CSV file
    watchlist_df = pd.read_csv('inputs/watchlist.csv')
    watchlist_tickers = set(watchlist_df['tickers'].str.upper())

    # Clean and convert revenue and net income
    df['totalRevenue'] = df['totalRevenue'].apply(clean_revenue)
    df['netIncome'] = df['netIncome'].apply(clean_revenue)

    # Group by ticker and calculate metrics
    results_df = df.groupby('ticker').apply(calculate_metrics).reset_index(drop=True)

    # Sort by Combined R² in descending order
    results_df = results_df.sort_values('Combined R²', ascending=False)

    # Add columns for dividendYield and targetMeanPrice
    results_df['dividendYield'] = np.nan
    results_df['targetMeanPrice'] = np.nan

    # Fetch yfinance data for stocks in the watchlist
    for ticker in results_df['Ticker']:
        if ticker.upper() in watchlist_tickers:
            dividend_yield, target_mean_price = get_yfinance_data(ticker)
            results_df.loc[results_df['Ticker'] == ticker, 'dividendYield'] = dividend_yield
            results_df.loc[results_df['Ticker'] == ticker, 'targetMeanPrice'] = target_mean_price

    # Export full summary to CSV
    output_file_path = 'financials_summary.csv'
    results_df.to_csv(output_file_path, index=False)

    # Create and export watchlist summary
    watchlist_results_df = results_df[results_df['Ticker'].str.upper().isin(watchlist_tickers)]
    watchlist_output_file_path = 'financials_summary_watchlist.csv'
    watchlist_results_df.to_csv(watchlist_output_file_path, index=False)

    print(f"Analysis complete. Full results exported to '{output_file_path}'.")
    print(f"Watchlist results exported to '{watchlist_output_file_path}'.")

if __name__ == "__main__":
    main()