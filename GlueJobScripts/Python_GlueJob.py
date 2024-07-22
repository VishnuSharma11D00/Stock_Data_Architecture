import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import psycopg2

# Getting job arguments
args = getResolvedOptions(sys.argv, ['CustomerDataETLJob', 'AuroraConnection', 'database2'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Extracting RDS connection details from Glue connection
connection = glueContext.extract_jdbc_conf(args['RDS_CONNECTION_NAME'])
rds_endpoint = connection['url'].split('/')[2].split(':')[0]
rds_port = connection['url'].split('/')[2].split(':')[1]
rds_username = connection['user']
rds_password = connection['password']
rds_db_name = args['RDS_DB_NAME']

# Glue database and tables
database = "stockdata-gluedb"
customers_table = "secret_candles_customer_data__test___customers_csv"
portfolios_table = "secret_candles_customer_data__test___portfolios_csv"
transactions_table = "secret_candles_customer_data__test___transactions_csv"

# Read data from Glue Catalog
customers = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=customers_table)
portfolios = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=portfolios_table)
transactions = glueContext.create_dynamic_frame.from_catalog(database=database, table_name=transactions_table)

# Convert to Spark DataFrames
customers_df = customers.toDF()
portfolios_df = portfolios.toDF()
transactions_df = transactions.toDF()

# Create RDS connection
conn = psycopg2.connect(
    host=rds_endpoint,
    port=rds_port,
    database=rds_db_name,
    user=rds_username,
    password=rds_password
)
cur = conn.cursor()

# SQL commands to create tables if they don't exist
create_customers_table = """
CREATE TABLE IF NOT EXISTS Customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone_number VARCHAR(20),
    address VARCHAR(255),
    join_date DATE
);
"""

create_portfolios_table = """
CREATE TABLE IF NOT EXISTS Portfolios (
    portfolio_id INT PRIMARY KEY,
    customer_id INT,
    portfolio_name VARCHAR(100),
    creation_date DATE,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);
"""

create_transactions_table = """
CREATE TABLE IF NOT EXISTS Transactions (
    transaction_id INT PRIMARY KEY,
    portfolio_id INT,
    transaction_date DATE,
    type VARCHAR(10),
    amount DECIMAL(10, 2),
    stock_symbol VARCHAR(10),
    shares INT,
    price_per_share DECIMAL(10, 2),
    FOREIGN KEY (portfolio_id) REFERENCES Portfolios(portfolio_id)
);
"""

# Execute table creation
cur.execute(create_customers_table)
cur.execute(create_portfolios_table)
cur.execute(create_transactions_table)
conn.commit()

# Function to write data to RDS
def write_to_rds(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{rds_endpoint}:{rds_port}/{rds_db_name}") \
        .option("dbtable", table_name) \
        .option("user", rds_username) \
        .option("password", rds_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Write data to RDS
write_to_rds(customers_df, "Customers")
write_to_rds(portfolios_df, "Portfolios")
write_to_rds(transactions_df, "Transactions")

# Close the cursor and connection
cur.close()
conn.close()

job.commit()
