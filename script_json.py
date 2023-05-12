import cx_Oracle
import json
import datetime

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

# Primeiro, vamos definir algumas consultas SQL para obter as informações necessárias
queries = {
    "store_users": """
        SELECT *
        FROM STORE_USERS
    """,
    "product": """
        SELECT p.*, pc.category_name, d.*
        FROM PRODUCT p
        JOIN PRODUCT_CATEGORIES pc ON p.category_id = pc.category_id
        LEFT JOIN DISCOUNT d ON p.discount_id = d.discount_id
    """,
    "sessions": """
        SELECT ss.session_id, ss.user_id, ss.created_at, ss.modified_at,
        ci.cart_item_id, ci.product_id, ci.quantity,
        p.product_name, p.category_id, p.sku, p.price, p.discount_id
        FROM SHOPPING_SESSION ss
        JOIN CART_ITEM ci ON ss.session_id = ci.session_id
        JOIN PRODUCT p ON ci.product_id = p.product_id
    """,
    "order_details": """
        SELECT od.*, oi.*, pd.*, p.*, a.*
        FROM ORDER_DETAILS od
        JOIN ORDER_ITEMS oi ON od.order_details_id = oi.order_details_id
        JOIN PAYMENT_DETAILS pd ON od.payment_id = pd.payment_details_id
        JOIN PRODUCT p ON oi.product_id = p.product_id
        JOIN ADDRESSES a ON od.delivery_adress_id = a.adress_id
    """,
    "employees": """
        SELECT e.*, d.*, ea.*
        FROM EMPLOYEES e
        JOIN DEPARTMENTS d ON e.department_id = d.department_id
        LEFT JOIN EMPLOYEES_ARCHIVE ea ON e.employee_id = ea.old_employee_id
    """,
    "stock": """
        SELECT *
        FROM STOCK
    """
}

# Agora, para cada consulta, vamos executá-la e gerar o JSON correspondente
for collection, query in queries.items():
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()

    # Obter os nomes das colunas
    column_names = [desc[0] for desc in cur.description]

    # Para simplificar, vamos assumir que todos os IDs estão na primeira coluna
    documents = []
    for row in data:
        document = {}

        # Para cada campo na linha, adicione-o ao documento com o nome da coluna correspondente
        for i, field in enumerate(row):
            if isinstance(field, datetime.datetime):
                field = field.strftime("%Y-%m-%d %H:%M:%S")
            document[column_names[i].lower()] = field

        # Adicione o documento à lista de documentos
        documents.append(document)

    # Finalmente, escreva os documentos em um arquivo JSON
    with open(f"{collection}.json", "w") as f:
        json.dump(documents, f)