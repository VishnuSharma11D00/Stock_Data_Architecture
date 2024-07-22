import os
import requests
import boto3
from datetime import datetime

# Alpha Vantage API settings
API_URL = "https://www.alphavantage.co/query"
FUNCTION = os.getenv('TIME_SERIES_FUCTION')
API_KEY = os.getenv('API_KEY')
SYMBOL = os.getenv('SYMBOL')
OUTPUT_SIZE = os.getenv('OUTPUT_SIZE', "compact")

# AWS S3 settings
BUCKET_NAME = os.getenv('BUCKET_NAME')
S3_BASE_PATH = os.getenv('S3_BASE_PATH')
S3_FILE_NAME = f"{S3_BASE_PATH}stock_data_{SYMBOL}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

def lambda_handler(event, context):
    try:
        # Fetch stock data from Alpha Vantage API in CSV format
        params = {
            "function": FUNCTION,
            "symbol": SYMBOL,
            "apikey": API_KEY,
            "outputsize": OUTPUT_SIZE,
            "datatype": "csv"  # Request CSV format
        }
        response = requests.get(API_URL, params=params)
        
        if response.status_code == 200:
            # Save the CSV data to S3
            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=S3_FILE_NAME,
                Body=response.content,  # Use response.content for binary data
                ContentType="text/csv"
            )
            
            return {
                "statusCode": 200,
                "body": "Data saved to S3 successfully"
            }
        else:
            return {
                "statusCode": response.status_code,
                "body": "Failed to fetch data from Alpha Vantage"
            }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"An error occurred: {str(e)}"
        }

if __name__ == "__main__":
    lambda_handler(None, None)
