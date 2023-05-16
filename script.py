import cx_Oracle
import pandas as pd
import numpy as np
import json
from datetime import datetime
import warnings
warnings.filterwarnings('ignore', 'pandas only supports SQLAlchemy connectable')

# Oracle database connection
dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='xe')
conn = cx_Oracle.connect(user='store', password='pass', dsn=dsn_tns)

# Fetching data from database
def fetch_data(query):
    return pd.read_sql(query, con=conn)

# Function to convert Timestamp to string
def convert_timestamp_to_string(timestamp):
    if pd.isnull(timestamp) or pd.isna(timestamp) or isinstance(timestamp, pd._libs.tslibs.nattype.NaTType):
        return None
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")

# Function to save data as pretty-printed JSON
def save_as_json(data, filename):
    def handle_nan(x):
        if pd.isna(x):
            return None
        return x

    data = [{k: handle_nan(v) for k, v in item.items()} for item in data]
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4, default=str)

# Query to fetch product data with discount, category, and stock info
product_query = """
SELECT p.*, d.*, pc.*, s.*
FROM product p 
LEFT JOIN discount d ON p.discount_id = d.discount_id 
LEFT JOIN product_categories pc ON p.category_id = pc.category_id
LEFT JOIN stock s ON p.product_id = s.product_id
"""
product_df = fetch_data(product_query)
product_df.fillna(value=np.nan, inplace=True)
product_df['CREATED_AT'] = product_df['CREATED_AT'].apply(convert_timestamp_to_string)
product_df['LAST_MODIFIED'] = product_df['LAST_MODIFIED'].apply(convert_timestamp_to_string)
product_json = product_df.apply(lambda row: {
    "product": {
        **row[['PRODUCT_ID', 'PRODUCT_NAME', 'SKU', 'PRICE', 'CREATED_AT', 'LAST_MODIFIED']].to_dict(),
        **({"discount": row[['DISCOUNT_ID', 'DISCOUNT_NAME', 'DISCOUNT_DESC', 'DISCOUNT_PERCENT', 'IS_ACTIVE_STATUS', 'CREATED_AT', 'MODIFIED_AT']].to_dict()} if not pd.isnull(row['DISCOUNT_ID']) else {}),
        **({"category": row[['CATEGORY_ID', 'CATEGORY_NAME']].to_dict()} if not pd.isnull(row['CATEGORY_ID']) else {}),
        **({"stock": row[['PRODUCT_ID', 'QUANTITY', 'MAX_STOCK_QUANTITY', 'UNIT']].to_dict()} if not pd.isnull(row['PRODUCT_ID']) else {})
    }
}, axis=1).tolist()
save_as_json(product_json, 'json_files/product.json')

# Query to fetch employee data with department info
employee_query = """
SELECT e.*, d.*
FROM employees e 
LEFT JOIN departments d ON e.department_id = d.department_id
"""
employee_df = fetch_data(employee_query)
employee_df.fillna(value=np.nan, inplace=True)
employee_df['DATE_OF_BIRTH'] = employee_df['DATE_OF_BIRTH'].apply(convert_timestamp_to_string)
employee_df['HIRE_DATE'] = employee_df['HIRE_DATE'].apply(convert_timestamp_to_string)
employee_json = employee_df.apply(lambda row: {
    "employee": {
        **row[['EMPLOYEE_ID', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'DATE_OF_BIRTH', 'HIRE_DATE', 'SALARY', 'PHONE_NUMBER', 'EMAIL', 'SSN_NUMBER', 'MANAGER_ID']].to_dict(),
        **({"department": row[['DEPARTMENT_ID', 'DEPARTMENT_NAME', 'MANAGER_ID', 'DEPARTMENT_DESC']].to_dict()} if not pd.isnull(row['DEPARTMENT_ID']) else {})
    }
}, axis=1).tolist()
save_as_json(employee_json, 'json_files/employees.json')

# Query to fetch order details with addresses and payment details
order_query = """
SELECT od.*, a.*, pd.*
FROM order_details od 
LEFT JOIN addresses a ON od.delivery_adress_id = a.adress_id
LEFT JOIN payment_details pd ON od.payment_id = pd.payment_details_id
"""
order_df = fetch_data(order_query)
order_df.fillna(value=np.nan, inplace=True)
order_df['CREATED_AT'] = order_df['CREATED_AT'].apply(convert_timestamp_to_string)
order_df['MODIFIED_AT'] = order_df['MODIFIED_AT'].apply(convert_timestamp_to_string)
order_json = order_df.apply(lambda row: {
    "order_details": {
        **row[['ORDER_DETAILS_ID', 'USER_ID', 'TOTAL', 'PAYMENT_ID', 'SHIPPING_METHOD', 'DELIVERY_ADRESS_ID', 'CREATED_AT', 'MODIFIED_AT']].to_dict(),
        **({"address": row[['ADRESS_ID', 'LINE_1', 'LINE_2', 'CITY', 'ZIP_CODE', 'PROVINCE', 'COUNTRY']].to_dict()} if not pd.isnull(row['ADRESS_ID']) else {}),
        **({"payment_details": row[['PAYMENT_DETAILS_ID', 'ORDER_ID', 'AMOUNT', 'PROVIDER', 'PAYMENT_STATUS', 'CREATED_AT', 'MODIFIED_AT']].to_dict()} if not pd.isnull(row['PAYMENT_DETAILS_ID']) else {})
    }
}, axis=1).tolist()
save_as_json(order_json, 'json_files/order_details.json')

tables = ['employees_archive', 'order_items', 'cart_item', 'shopping_session', 'store_users']
for table in tables:
    df = fetch_data(f"SELECT * FROM {table}")
    df_json = df.to_dict('records')
    save_as_json(df_json, f'json_files/{table}.json')

conn.close()