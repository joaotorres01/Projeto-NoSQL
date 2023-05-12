import cx_Oracle
import json

# Credenciais para se conectar ao banco de dados
user = 'store'
password = 'pass'
hostname = 'localhost'
port = '1521'
service_name = 'xe'

# Criar a string de conexão
dsn = cx_Oracle.makedsn(hostname, port, service_name=service_name)

# Conectar ao banco de dados
conn = cx_Oracle.connect(user, password, dsn)

# Criar um cursor
cur = conn.cursor()

# Lista de tabelas para extrair dados
tabelas = [
    'STORE_USERS',
    'PRODUCT_CATEGORIES',
    'PRODUCT',
    'DISCOUNT',
    'CART_ITEM',
    'SHOPPING_SESSION',
    'ORDER_DETAILS',
    'ORDER_ITEMS',
    'PAYMENT_DETAILS',
    'EMPLOYEES',
    'DEPARTMENTS',
    'ADDRESSES',
    'EMPLOYEES_ARCHIVE',
    'STOCK',
]

# Para cada tabela na lista, executar uma consulta SQL
for tabela in tabelas:
    print(f"Extraindo dados da tabela {tabela}")
    cur.execute(f"SELECT * FROM {tabela}")

    # Recuperar todos os resultados
    rows = cur.fetchall()

    # Imprimir os resultados
    for row in rows:
        print(row)

# Fechar o cursor e a conexão
cur.close()
conn.close()