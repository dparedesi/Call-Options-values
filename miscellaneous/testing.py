import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv  # Fixed import statement
import time
import random

# Load environment variables
load_dotenv()

def get_13f_filings():
    # Set up the API request
    base_url = "https://www.sec.gov/cgi-bin/browse-edgar"
    
    # Get yesterday's date in the correct format (YYYYMMDD)
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    
    params = {
        "action": "getcompany",
        "type": "13F-HR",
        "dateb": yesterday,
        "owner": "include",
        "start": "0",
        "count": "100",
        "output": "atom"
    }
    
    headers = {
        "User-Agent": "YourCompanyName YourAppName YourEmail@example.com"
    }
    
    # Implement a delay before making the request
    time.sleep(random.uniform(1, 3))
    
    # Make the request
    response = requests.get(base_url, params=params, headers=headers)
    
    if response.status_code == 200:
        # Parse the XML response
        from xml.etree import ElementTree
        root = ElementTree.fromstring(response.content)
        
        filings = []
        for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
            title = entry.find('{http://www.w3.org/2005/Atom}title').text
            link = entry.find('{http://www.w3.org/2005/Atom}link').attrib['href']
            filings.append(f"{title}\n{link}\n")
        
        return filings
    else:
        print(f"Error: {response.status_code}")
        print(f"Response content: {response.text}")
        return None

def send_email(filings):
    print("13F Filing Details:")
    for filing in filings:
        print(filing)
        print("-" * 50)

def main():
    filings = get_13f_filings()
    if filings:
        send_email(filings)
    else:
        print("No new 13F filings found or there was an error fetching the data.")

if __name__ == "__main__":
    main()
