import cx_Oracle
import pandas as pd
import numpy as np
import warnings
from py2neo import Graph, Node, Relationship
warnings.filterwarnings('ignore', 'pandas only supports SQLAlchemy connectable')

#Oracle database connection
dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='xe')
conn = cx_Oracle.connect(user='store', password='pass', dsn=dsn_tns)

#Fetching data from database
def fetch_data(query):
    return pd.read_sql(query, con=conn)

#Function to convert Timestamp to string
def convert_timestamp_to_string(timestamp):
    if pd.isnull(timestamp) or pd.isna(timestamp) or isinstance(timestamp, pd._libs.tslibs.nattype.NaTType):
        return None
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")

#Function to handle NaN and NaT
def handle_nan_and_nat(x):
    if pd.isna(x) or (isinstance(x, pd._libs.tslibs.nattype.NaTType) or isinstance(x, np.datetime64)):
        return None
    elif isinstance(x, pd._libs.tslibs.timestamps.Timestamp):
        return x.to_pydatetime()
    return x

#Neo4j database connection
graph = Graph("neo4j://localhost:7999", auth=("neo4j", "password"))

#Example: Fetch and process product data and insert into Neo4j
product_query = """
SELECT p.*, d.*, pc.*, s.*
FROM product p 
LEFT JOIN discount d ON p.discount_id = d.discount_id 
LEFT JOIN product_categories pc ON p.category_id = pc.category_id
LEFT JOIN stock s ON p.product_id = s.product_id
"""
product_df = fetch_data(product_query)
for index, row in product_df.iterrows():
    row_dict = row.to_dict()
    for key in row_dict:
        row_dict[key] = handle_nan_and_nat(row_dict[key])
    node = Node("Product", **row_dict)
    graph.create(node)


# Fetch and process employee data and insert into Neo4j
employee_query = """
SELECT e.*, d.*
FROM employees e 
LEFT JOIN departments d ON e.department_id = d.department_id
"""
employee_df = fetch_data(employee_query)
for index, row in employee_df.iterrows():
    row_dict = row.to_dict()
    for key in row_dict:
        row_dict[key] = handle_nan_and_nat(row_dict[key])
    node = Node("Employee", **row_dict)
    graph.create(node)

# Fetch and process order_items data and insert into Neo4j
order_items_query = """
SELECT od.*, a.*, pd.*
FROM order_details od 
LEFT JOIN addresses a ON od.delivery_adress_id = a.adress_id
LEFT JOIN payment_details pd ON od.payment_id = pd.payment_details_id
"""
order_items_df = fetch_data(order_items_query)
for index, row in order_items_df.iterrows():
    row_dict = row.to_dict()
    for key in row_dict:
        row_dict[key] = handle_nan_and_nat(row_dict[key])
    node = Node("OrderItems", **row_dict)
    graph.create(node)

# Fetch and process employees_archive data and insert into Neo4j
employees_archive_query = "SELECT * FROM employees_archive"
employees_archive_df = fetch_data(employees_archive_query)
for index, row in employees_archive_df.iterrows():
    row_dict = row.to_dict()
    for key in row_dict:
        row_dict[key] = handle_nan_and_nat(row_dict[key])
    node = Node("EmployeesArchive", **row_dict)
    graph.create(node)

# Fetch and process cart_item data and insert into Neo4j
cart_item_query = "SELECT * FROM cart_item"
cart_item_df = fetch_data(cart_item_query)
for index, row in cart_item_df.iterrows():
    row_dict = row.to_dict()
    for key in row_dict:
        row_dict[key] = handle_nan_and_nat(row_dict[key])
    node = Node("CartItem", **row_dict)
    graph.create(node)

# Fetch and process shopping_session data and insert into Neo4j
shopping_session_query = "SELECT * FROM shopping_session"
shopping_session_df = fetch_data(shopping_session_query)
for index, row in shopping_session_df.iterrows():
    row_dict = row.to_dict()
    for key in row_dict:
        row_dict[key] = handle_nan_and_nat(row_dict[key])
    node = Node("ShoppingSession", **row_dict)
    graph.create(node)

# Fetch and process store_users data and insert into Neo4j
store_users_query = "SELECT * FROM store_users"
store_users_df = fetch_data(store_users_query)
for index, row in store_users_df.iterrows():
    row_dict = row.to_dict()
    for key in row_dict:
        row_dict[key] = handle_nan_and_nat(row_dict[key])
    node = Node("StoreUsers", **row_dict)
    graph.create(node)


# ... Existing code for creating nodes ...

# Fetch and process product data and insert into Neo4j
product_query = "SELECT * FROM product"
product_df = fetch_data(product_query)

for index, row in product_df.iterrows():
    row_dict = row.to_dict()
    for key in row_dict:
        row_dict[key] = handle_nan_and_nat(row_dict[key])
    product_node = Node("Product", **row_dict)
    graph.create(product_node)

    # If the product has a discount
    if not pd.isnull(row['DISCOUNT_ID']):
        discount_node = graph.nodes.match("Discount", DISCOUNT_ID=row_dict['DISCOUNT_ID']).first()
        if discount_node:
            relationship = Relationship(product_node, "HAS_DISCOUNT", discount_node)
            graph.create(relationship)

    # If the product has a category
    if not pd.isnull(row['CATEGORY_ID']):
        category_node = graph.nodes.match("ProductCategory", CATEGORY_ID=row_dict['CATEGORY_ID']).first()
        if category_node:
            relationship = Relationship(product_node, "BELONGS_TO", category_node)
            graph.create(relationship)

    # If the product has stock information
    if not pd.isnull(row['PRODUCT_ID']):
        stock_node = graph.nodes.match("Stock", PRODUCT_ID=row_dict['PRODUCT_ID']).first()
        if stock_node:
            relationship = Relationship(product_node, "HAS_STOCK", stock_node)
            graph.create(relationship)

# ... Code for creating relationships for other tables ...

conn.close()