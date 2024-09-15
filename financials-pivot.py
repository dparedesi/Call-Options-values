import pandas as pd
import numpy as np
from scipy import stats
import yfinance as yf

def clean_revenue(revenue):
    if isinstance(revenue, str):
        return float(revenue.replace("$", "").replace(",", ""))
    return float(revenue)

def calculate_metrics(df, ticker):
    # Convert fiscalDateEnding to datetime with the correct format
    df['fiscalDateEnding'] = pd.to_datetime(df['fiscalDateEnding'], format='%d/%m/%Y')
    
    # Sort by date
    df = df.sort_values('fiscalDateEnding')
    
    # Calculate days since first entry for x-axis
    df['days'] = (df['fiscalDateEnding'] - df['fiscalDateEnding'].min()).dt.days
    
    # Calculate revenue metrics
    revenue_slope, revenue_intercept, revenue_r_value, _, _ = stats.linregress(df['days'], df['totalRevenue'])
    revenue_r_squared = revenue_r_value**2
    
    # Calculate net income metrics
    income_slope, income_intercept, income_r_value, _, _ = stats.linregress(df['days'], df['netIncome'])
    income_r_squared = income_r_value**2
    
    # Calculate combined R-squared
    combined_r_squared = revenue_r_squared * income_r_squared
    
    # Calculate correlation between revenue and net income
    revenue_income_correlation = df['totalRevenue'].corr(df['netIncome'])
    
    # Calculate percentage change in revenue
    oldest_revenue = df.iloc[0]['totalRevenue']
    newest_revenue = df.iloc[-1]['totalRevenue']
    revenue_change_pct = ((newest_revenue - oldest_revenue) / oldest_revenue) * 100
    
    # Pull dividend yield from yfinance
    ticker_info = yf.Ticker(ticker)
    dividend_yield = ticker_info.info.get('dividendYield', None)
    if dividend_yield is not None:
        dividend_yield *= 100  # Convert to percentage
    
    return {
        'Ticker': ticker,
        'Oldest Date': df.iloc[0]['fiscalDateEnding'].strftime('%Y-%m-%d'),
        'Oldest Revenue': oldest_revenue,
        'Newest Date': df.iloc[-1]['fiscalDateEnding'].strftime('%Y-%m-%d'),
        'Newest Revenue': newest_revenue,
        '%Change Revenue': revenue_change_pct,
        '%Dividend Yield': dividend_yield,
        'Revenue Slope': revenue_slope,
        'Revenue R²': revenue_r_squared,
        'Net Income Slope': income_slope,
        'Net Income R²': income_r_squared,
        'Combined R²': combined_r_squared,
        'Revenue-Income Correlation': revenue_income_correlation
    }

# Read the CSV file
df = pd.read_csv('financials-historical.csv')

# Clean and convert revenue and net income
df['totalRevenue'] = df['totalRevenue'].apply(clean_revenue)
df['netIncome'] = df['netIncome'].apply(clean_revenue)

# Group by ticker and calculate metrics
results = []
for ticker, group in df.groupby('ticker'):
    results.append(calculate_metrics(group, ticker))

# Create DataFrame from results
results_df = pd.DataFrame(results)

# Sort by Combined R² in descending order
results_df = results_df.sort_values('Combined R²', ascending=False)

# Export to CSV
output_file_path = 'financials_summary.csv'
results_df.to_csv(output_file_path, index=False)

print(f"Analysis complete. Results exported to '{output_file_path}'.")