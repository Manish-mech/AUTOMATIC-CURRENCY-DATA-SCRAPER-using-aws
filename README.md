# AUTOMATIC-CURRENCY-DATA-SCRAPER-using-aws
![Architecture](https://s3.ap-northeast-1.amazonaws.com/motulaal.io/Crypto+share+monitor/architecture.png)

## Table of Contents

1. Problem Statement
2. Tools Used
3. Requirements
4. AWS Architecture
5. Code Snippet
6. Result
7. Conclusion

---

## Problem Statement
![Problem Statement](https://s3.ap-northeast-1.amazonaws.com/motulaal.io/Crypto+share+monitor/requirement.png)

A Google sheet containing last week’s opening, closing, highest, and lowest prices is required for specific currency tickers, fetched through APIs.

---

## Tools Utilized 🛠️

- Python
- Yahoo Finance API
- CloudWatch Event (Scheduled data extraction trigger)
- AWS SQS (Sequencing trigger query)
- AWS Lambda (Serverless service)
- IAM (Permissions setup for AWS services)

---

## Requirements 📋

To provide accurate and up-to-date currency market data, an automated algorithm refreshes a CSV file every Monday. This ensures users have access to the latest opening, closing, highest, and lowest prices for each currency, facilitating informed decision-making.

---

## AWS Architecture
![Architecture](https://s3.ap-northeast-1.amazonaws.com/motulaal.io/Crypto+share+monitor/architecture.png)
---
**Automatic Data Scraper Action 🎯:**

- CloudWatch Event triggers SQS for scheduled Lambda function execution.
- SQS triggers Lambda weekly for data extraction.
- Lambda function runs Python program using Yahoo Finance API.
- Extracted data is stored in an S3 bucket for access.

---

## Code Snippet
<pre><code>
# Python code for fetching and processing currency data from Yahoo Finance

# ...(
# Import necessary libraries
from yahoo_fin.stock_info import *  # Import Yahoo Finance API functions
import pandas as pd  # Import pandas for data manipulation
from datetime import datetime, timedelta  # Import datetime for date calculations

# The input currency pairs for which we have to find the data
forex_pairs = [
    "NZDUSD=X", "GBPNZD=X", "NZDCAD=X", "EURNZD=X", "NZDCHF=X", "NZDJPY=X",
    "AUDUSD=X", "GBPAUD=X", "AUDCAD=X", "AUDCHF=X", "EURAUD=X", "AUDJPY=X",
    "EURGBP=X", "EURJPY=X", "EURCHF=X", "EURCAD=X", "GBPUSD=X", "GBPCAD=X",
    "GBPJPY=X", "USDJPY=X", "USDCHF=X", "CADJPY=X", "AUDNZD=X", "CADCHF=X",
    "GBPCHF=X", "CHFJPY=X", "EURUSD=X", "USDCAD=X"
]

# Function which will extract the required price

def get_forex_data(start_date, end_date, forex_pairs):
    forex_data = {}

    for pair in forex_pairs:
        data = get_data(pair, start_date, end_date, interval="1wk")
        if not data.empty:
            # Get the data for the last complete market week closing
            last_complete_week = data.index[-2]
            weekly_data = data.loc[last_complete_week]

            forex_data[pair] = {
                "Week_Open": round(weekly_data["open"], 4),
                "Week_Close": round(weekly_data["close"], 5),
                "Week_High": round(weekly_data["high"], 5),
                "Week_Low": round(weekly_data["low"], 5),
                "pips": round((weekly_data["close"] - weekly_data["open"]) * 10000, 2),
                "change": round(((weekly_data["close"] - weekly_data["open"]) / weekly_data["open"]) * 100, 3)
            }

    # Create a DataFrame from the forex_data dictionary
    return pd.DataFrame(forex_data).T

import io
import boto3
from datetime import datetime, timedelta

''' Lambda function which will help Lambda function to understand which code
it have to execute'''

def lambda_handler(event, context):
    # Calculate start_date and end_date, specify the range of duration
    today = datetime.today()
    last_week_start = today - timedelta(days=today.weekday() + 7)
    start_date = last_week_start.strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    
    df = get_forex_data(start_date, end_date, forex_pairs)
    
    # Save the DataFrame to an Excel file in memory
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index_label="Forex Pair", engine="xlsxwriter")
    excel_buffer.seek(0)
    
    # Upload the Excel file to an S3 bucket
    s3_bucket_name = 'your-s3-bucket-name'
    s3_object_key = 'forex_data.xlsx'
    s3_client = boto3.client('s3')
    s3_client.upload_fileobj(excel_buffer, s3_bucket_name, s3_object_key)
    
    return {
        'statusCode': 200,
        'body': f"Data has been saved to S3 bucket: s3://{s3_bucket_name}/{s3_object_key}"
    }
</code></pre>
---
## Result🏆

The required data for specified currency tickers has been successfully extracted and updated for the current week. Excel files are versioned in the S3 bucket to maintain historical data.
![Result](https://s3.ap-northeast-1.amazonaws.com/motulaal.io/Crypto+share+monitor/result.png)


---
## Conclusion ✨

This automated system streamlines financial data collection for specific currency tickers, ensuring reliability and eliminating the need for manual intervention. Users consistently have access to accurate market data for informed decision-making. The solution is scalable and efficient, serving as a valuable asset for businesses and individuals seeking timely financial insights.
