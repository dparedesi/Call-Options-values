import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# Base URL of the page to scrape (without the page number)
base_url = "https://www.capitoltrades.com/trades?pageSize=500"
base_url = "https://www.capitoltrades.com/trades?pageSize=1000&page="

# Initialize lists to store the extracted data
politicians = []
issuers = []
tickers = []
publish_dates = []
trade_dates = []
filed_after_days = []
owners = []
trade_types = []
trade_sizes = []
prices = []

# Start tracking time
start_time = time.time()

# Pagination loop
page_number = 1
while True:
    # Display loading message with elapsed time
    elapsed_time = time.time() - start_time
    print(f"Loading for {int(elapsed_time)} seconds...", end='\r')

    # Send a GET request to fetch the page content
    response = requests.get(base_url + str(page_number))
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract rows from the table body
    rows = soup.select('table.q-table tbody tr')

    # If no rows are found, break the loop (end of pagination)
    if not rows or (page_number-1)*100 == 500:
        break

    # Extract data from each row
    for row in rows:
        # Extract data with checks for None
        politician = row.select_one('.q-column--politician h2 a')
        politicians.append(politician.text.strip() if politician else None)

        issuer = row.select_one('.q-column--issuer h3 a')
        issuers.append(issuer.text.strip() if issuer else None)

        ticker = row.select_one('.q-column--issuer span')
        tickers.append(ticker.text.strip() if ticker else None)

        pub_date_3 = row.select_one('.q-column--pubDate .text-size-3')
        pub_date_2 = row.select_one('.q-column--pubDate .text-size-2')
        publish_dates.append(
            (pub_date_3.text.strip() if pub_date_3 else '') + " " +
            (pub_date_2.text.strip() if pub_date_2 else '')
        )

        trade_date_3 = row.select_one('.q-column--txDate .text-size-3')
        trade_date_2 = row.select_one('.q-column--txDate .text-size-2')
        trade_dates.append(
            (trade_date_3.text.strip() if trade_date_3 else '') + " " +
            (trade_date_2.text.strip() if trade_date_2 else '')
        )

        filed_after = row.select_one('.q-column--reportingGap .q-value span')
        filed_after_days.append(filed_after.text.strip() if filed_after else None)

        owner = row.select_one('.q-column--owner .q-label')
        owners.append(owner.text.strip() if owner else None)

        trade_type = row.select_one('.q-column--txType .tx-type')
        trade_types.append(trade_type.text.strip() if trade_type else None)

        # Extract trade size and replace en dash with ' to '
        trade_size = row.select_one('.q-column--value .text-size-2')
        trade_sizes.append(trade_size.text.strip().replace('–', '-') if trade_size else None)

        price = row.select_one('.q-column--price .q-field')
        prices.append(price.text.strip() if price else None)

    # Move to the next page
    page_number += 1

# Clear the loading message after the loop ends
print(" " * 40, end='\r')

# Create a DataFrame with the extracted data
df = pd.DataFrame({
    'Politician': politicians,
    'Issuer': issuers,
    'Ticker': tickers,
    'Trade Type': trade_types,
    'Published Date': publish_dates,
    'Trade Date': trade_dates,
    'Filed After (Days)': filed_after_days,
    'Owner': owners,
    'Trade Size': trade_sizes,
    'Price': prices
})

# Print total time
print(f"Completed in: {int(elapsed_time)} seconds...", end='\r')

# After scraping, save the DataFrame to a CSV file in the root directory of the repo
file_path = os.path.join(os.getcwd(), 'politicians_trades.csv')  # Ensure it saves in the root of the repo
print(df)
df.to_csv(file_path, index=False, encoding='utf-8')
print(f"File saved to: {file_path}")
