import cx_Oracle
import pandas as pd
import numpy as np
from py2neo import Graph, Node, Relationship

# Oracle DB connection details
oracle_host = 'localhost'
oracle_port = '1521'
oracle_sid = 'xe'
oracle_user = 'store'
oracle_password = 'pass'

# Establish connections to Oracle DB and MongoDB
oracle_conn = cx_Oracle.connect(f"{oracle_user}/{oracle_password}@{oracle_host}:{oracle_port}/{oracle_sid}")

print('oracle')


url = "bolt://localhost:7687"
username = "neo4j"
password = "password"

# Configuração da conexão ao Neo4j
graph = Graph(url, auth=(username, password))

# Consulta para obter os usuários da loja
query_users = "SELECT * FROM store_users"

# Consulta para obter os produtos
query_products = "SELECT * FROM product"

# Consulta para obter os detalhes do pedido
query_order_details = "SELECT * FROM order_details"

# Executar consulta de usuários
df_users = pd.read_sql(query_users, oracle_conn)
print(df_users)

# Executar consulta de produtos
df_products = pd.read_sql(query_products, oracle_conn)

# Executar consulta de detalhes do pedido
df_order_details = pd.read_sql(query_order_details, oracle_conn)






