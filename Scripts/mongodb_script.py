import cx_Oracle
from pymongo import MongoClient

# Oracle DB connection details
oracle_host = 'localhost'
oracle_port = '1521'
oracle_sid = 'xe'
oracle_user = 'store'
oracle_password = 'pass'

# MongoDB connection details
mongo_db = 'Store'

# Establish connections to Oracle DB and MongoDB
oracle_conn = cx_Oracle.connect(f"{oracle_user}/{oracle_password}@{oracle_host}:{oracle_port}/{oracle_sid}")
mongo_conn = MongoClient("mongodb+srv://mario:mario@cluster0.f3uul7o.mongodb.net/?retryWrites=true&w=majority")
mongo_db = mongo_conn[mongo_db]

# Function to fetch data from Oracle DB
def fetch_data_from_oracle(query):
    cursor = oracle_conn.cursor()
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

# Function to insert data into MongoDB
def insert_data_into_mongo(collection_name, data):
    collection = mongo_db[collection_name]
    collection.insert_many(data)

# Function to migrate Product, Discount, Category, and Stock tables
def migrate_product_data():
    product_query = """
        SELECT p.product_id, p.product_name, p.sku, p.price, p.created_at, p.last_modified,
               c.category_id, c.category_name,
               d.discount_id, d.discount_name, d.discount_desc, d.discount_percent, d.is_active_status,
               s.quantity, s.max_stock_quantity, s.unit
        FROM product p
        JOIN product_categories c ON p.category_id = c.category_id
        LEFT JOIN discount d ON p.discount_id = d.discount_id
        JOIN stock s ON p.product_id = s.product_id
    """
    product_data = fetch_data_from_oracle(product_query)

    products = []
    for row in product_data:
        product = {
            'product_id': row['PRODUCT_ID'],
            'product_name': row['PRODUCT_NAME'],
            'sku': row['SKU'],
            'price': row['PRICE'],
            'created_at': row['CREATED_AT'],
            'last_modified': row['LAST_MODIFIED'],
            'category': {
                'category_id': row['CATEGORY_ID'],
                'category_name': row['CATEGORY_NAME']
            },
            'stock': {
                'quantity': row['QUANTITY'],
                'max_stock_quantity': row['MAX_STOCK_QUANTITY'],
                'unit': row['UNIT']
            }
        }

        if row['DISCOUNT_ID'] is not None:  # Check if a discount exists
            product['discount'] = {
                'discount_id': row['DISCOUNT_ID'],
                'discount_name': row['DISCOUNT_NAME'],
                'discount_desc': row['DISCOUNT_DESC'],
                'discount_percent': row['DISCOUNT_PERCENT'],
                'is_active_status': row['IS_ACTIVE_STATUS']
            }
        
        products.append(product)

    insert_data_into_mongo('Product', products)

# Function to migrate Employees and Department tables
def migrate_employees_data():
    employees_query = """
        SELECT e.employee_id, e.first_name, e.middle_name, e.last_name, e.date_of_birth, e.department_id,
               e.hire_date, e.salary, e.phone_number, e.email, e.ssn_number, e.manager_id,
               d.department_name, d.department_desc
        FROM employees e, departments d
        WHERE e.department_id = d.department_id
    """
    employees_data = fetch_data_from_oracle(employees_query)

    employees = []
    for row in employees_data:
        
        employee = {
            'employee_id': row['EMPLOYEE_ID'],
            'first_name': row['FIRST_NAME'],
            'middle_name': row['MIDDLE_NAME'],
            'last_name': row['LAST_NAME'],
            'date_of_birth': row['DATE_OF_BIRTH'],
            'department': {
                'department_id': row['DEPARTMENT_ID'],
                'manager_id': row['MANAGER_ID'],
                'department_name': row['DEPARTMENT_NAME'],
                'department_desc': row['DEPARTMENT_DESC']
            },
            'hire_date': row['HIRE_DATE'],
            'salary': row['SALARY'],
            'phone_number': row['PHONE_NUMBER'],
            'email': row['EMAIL'],
            'ssn_number': row['SSN_NUMBER'],
            'manager_id': row['MANAGER_ID']
        }
        employees.append(employee)

    insert_data_into_mongo('Employees', employees)

# Function to migrate Employees_Archive table
def migrate_employees_archive_data():
    employees_archive_query = """
        SELECT *
        FROM employees_archive
    """
    employees_archive_data = fetch_data_from_oracle(employees_archive_query)

    insert_data_into_mongo('Employees_Archive', employees_archive_data)

# Function to migrate Order_Details, Addresses, Payment_Details, and Order_Items tables
def migrate_order_data():
    order_query = """
        SELECT order_details_id, user_id, total, payment_id, shipping_method,
               delivery_adress_id, created_at, modified_at
        FROM order_details
    """
    order_data = fetch_data_from_oracle(order_query)

    orders = []
    for row in order_data:
        order_id = row['ORDER_DETAILS_ID']
        order = {
            'order_id': order_id,
            'user_id': row['USER_ID'],
            'total': row['TOTAL'],
            'payment_id': row['PAYMENT_ID'],
            'shipping_method': row['SHIPPING_METHOD'],
            'delivery_address_id': row['DELIVERY_ADRESS_ID'],
            'created_at': row['CREATED_AT'],
            'modified_at': row['MODIFIED_AT'],
            'address': None,
            'payment': None,
            'order_items': []  # Initialize an empty list for order items
        }
        orders.append(order)

    address_query = """
        SELECT adress_id, line_1, line_2, city, zip_code, province, country
        FROM addresses
        WHERE adress_id = {}
    """

    for order in orders:
        if order['delivery_address_id'] is not None:
            address_data = fetch_data_from_oracle(address_query.format(order['delivery_address_id']))
            if address_data:
                address_row = address_data[0]
                order['address'] = {
                    'address_id': address_row['ADRESS_ID'],
                    'line_1': address_row['LINE_1'],
                    'line_2': address_row['LINE_2'],
                    'city': address_row['CITY'],
                    'zip_code': address_row['ZIP_CODE'],
                    'province': address_row['PROVINCE'],
                    'country': address_row['COUNTRY']
                }

    payment_query = """
        SELECT payment_details_id, order_id, amount, provider, payment_status
        FROM payment_details
        WHERE order_id = {}
    """

    for order in orders:
        if order['payment_id'] is not None:
            payment_data = fetch_data_from_oracle(payment_query.format(order['payment_id']))
            if payment_data:
                payment_row = payment_data[0]
                order['payment'] = {
                    'payment_details_id': payment_row['PAYMENT_DETAILS_ID'],
                    'order_id': payment_row['ORDER_ID'],
                    'amount': payment_row['AMOUNT'],
                    'provider': payment_row['PROVIDER'],
                    'payment_status': payment_row['PAYMENT_STATUS']
                }

    order_items_query = """
        SELECT order_items_id, order_details_id, product_id, created_at, modified_at
        FROM order_items
        WHERE order_details_id = {}
    """

    for order in orders:
        order_items_data = fetch_data_from_oracle(order_items_query.format(order['order_id']))
        for row in order_items_data:
            order_item = {
                'order_items_id': row['ORDER_ITEMS_ID'],
                'order_details_id': row['ORDER_DETAILS_ID'],
                'product_id': row['PRODUCT_ID'],
                'created_at': row['CREATED_AT'],
                'modified_at': row['MODIFIED_AT']
            }
            order['order_items'].append(order_item)

    insert_data_into_mongo('Order', orders)





# Function to migrate Shopping_Session and Cart_Item tables
def migrate_shopping_data():
    session_query = """
        SELECT session_id, user_id, created_at, modified_at
        FROM shopping_session
    """
    session_data = fetch_data_from_oracle(session_query)

    sessions = []
    for row in session_data:
        session_id = row['SESSION_ID']
        session = {
            'session_id': session_id,
            'user_id': row['USER_ID'],
            'created_at': row['CREATED_AT'],
            'modified_at': row['MODIFIED_AT'],
            'cart_items': []
        }
        sessions.append(session)

    cart_item_query = """
        SELECT cart_item_id, session_id, product_id, quantity, created_at, modified_at
        FROM cart_item
    """
    cart_item_data = fetch_data_from_oracle(cart_item_query)

    for session in sessions:
        for row in cart_item_data:
            if session['session_id'] == row['SESSION_ID']:
                cart_item = {
                    'cart_item_id': row['CART_ITEM_ID'],
                    'product_id': row['PRODUCT_ID'],
                    'quantity': row['QUANTITY'],
                    'created_at': row['CREATED_AT'],
                    'modified_at': row['MODIFIED_AT'],
                }
                session['cart_items'].append(cart_item)

    insert_data_into_mongo('Shopping_Session', sessions)

# Function to migrate Store_Users table
def migrate_store_users_data():
    store_users_query = """
        SELECT su.user_id, su.first_name, su.middle_name, su.last_name, su.phone_number, su.email, su.username,
               su.user_password, su.registered_at
        FROM store_users su
    """
    store_users_data = fetch_data_from_oracle(store_users_query)

    insert_data_into_mongo('Store_Users', store_users_data)

# Migrate data from Oracle DB to MongoDB
migrate_product_data()
migrate_employees_data()
migrate_employees_archive_data()
migrate_order_data()
migrate_shopping_data()
migrate_store_users_data()

# Close the connections
oracle_conn.close()
mongo_conn.close()
