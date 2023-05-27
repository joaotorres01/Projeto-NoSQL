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

# Function to migrate store_users table
def migrate_store_users():
    query = """
        SELECT su.user_id, su.first_name, su.middle_name, su.last_name, su.phone_number, su.email, su.username,
               su.user_password, su.registered_at, ss.session_id, ss.created_at AS session_created_at,
               ss.modified_at AS session_modified_at, ci.cart_item_id, ci.product_id, ci.quantity,
               ci.created_at AS cart_item_created_at, ci.modified_at AS cart_item_modified_at
        FROM store_users su
        LEFT JOIN shopping_session ss ON su.user_id = ss.user_id
        LEFT JOIN cart_item ci ON ss.session_id = ci.session_id
    """
    store_users_data = fetch_data_from_oracle(query)
    
    store_users = {}
    for data in store_users_data:
        user_id = data['USER_ID']
        
        if user_id not in store_users:
            store_users[user_id] = {
                'user_id': user_id,
                'first_name': data['FIRST_NAME'],
                'middle_name': data['MIDDLE_NAME'],
                'last_name': data['LAST_NAME'],
                'phone_number': data['PHONE_NUMBER'],
                'email': data['EMAIL'],
                'username': data['USERNAME'],
                'user_password': data['USER_PASSWORD'],
                'registered_at': data['REGISTERED_AT'],
                'shopping_session': {
                    'session_id': data['SESSION_ID'],
                    'created_at': data['SESSION_CREATED_AT'],
                    'modified_at': data['SESSION_MODIFIED_AT'],
                    'cart_items': []
                }
            }
        
        if data['CART_ITEM_ID']:
            store_users[user_id]['shopping_session']['cart_items'].append({
                'cart_item_id': data['CART_ITEM_ID'],
                'product_id': data['PRODUCT_ID'],
                'quantity': data['QUANTITY'],
                'created_at': data['CART_ITEM_CREATED_AT'],
                'modified_at': data['CART_ITEM_MODIFIED_AT']
            })
    
    insert_data_into_mongo('Users', list(store_users.values()))

# Function to migrate product_categories table
def migrate_product_categories():
    query = "SELECT * FROM product_categories"
    product_categories_data = fetch_data_from_oracle(query)
    insert_data_into_mongo('Categories', product_categories_data)

# Function to migrate product table
def migrate_product():
    query = """
        SELECT p.product_id, p.product_name, c.category_name, p.sku, p.price, d.discount_name, d.discount_desc,
               d.discount_percent, d.is_active_status, p.created_at, p.last_modified,
               s.quantity, s.max_stock_quantity, s.unit
        FROM product p
        INNER JOIN product_categories c ON p.category_id = c.category_id
        LEFT JOIN discount d ON p.discount_id = d.discount_id
        INNER JOIN stock s ON p.product_id = s.product_id
    """
    product_data = fetch_data_from_oracle(query)
    insert_data_into_mongo('Product', product_data)



def migrate_order_details():
    query = """
        SELECT od.order_details_id, od.user_id, od.total, od.payment_id, od.shipping_method,
               a.line_1, a.line_2, a.city, a.zip_code, a.province, a.country,
               pd.amount, pd.provider, pd.payment_status, od.created_at, od.modified_at
        FROM order_details od
        INNER JOIN addresses a ON od.delivery_adress_id = a.adress_id
        INNER JOIN payment_details pd ON od.order_details_id = pd.order_id
    """
    order_details_data = fetch_data_from_oracle(query)
    
    for order_details in order_details_data:
        order_details_id = order_details['ORDER_DETAILS_ID']
        order_items_query = f"""
            SELECT *
            FROM order_items
            WHERE order_details_id = {order_details_id}
        """
        order_items_data = fetch_data_from_oracle(order_items_query)
        order_details['order_items'] = order_items_data
    
    insert_data_into_mongo('Order_Details', order_details_data)

# Function to migrate employees table
def migrate_employees():
    employees_query = """
        SELECT e.employee_id, e.first_name, e.middle_name, e.last_name, e.date_of_birth, d.department_name,
               e.hire_date, e.salary, e.phone_number, e.email, e.ssn_number, e.manager_id
        FROM employees e
        INNER JOIN departments d ON e.department_id = d.department_id
    """
    employees_data = fetch_data_from_oracle(employees_query)
    
    for employee_data in employees_data:
        employee_id = employee_data['EMPLOYEE_ID']
        archive_query = f"""
            SELECT *
            FROM employees_archive
            WHERE new_employee_id = {employee_id}
        """
        archive_data = fetch_data_from_oracle(archive_query)
        if archive_data:
            employee_data['employees_archive'] = archive_data[0]
    
    insert_data_into_mongo('Employees', employees_data)

#remove the id for archive (in the department)


# Migrate data from Oracle DB to MongoDB
migrate_store_users()
#migrate_product()
#migrate_shopping_session()
#migrate_order_details()
#migrate_employees()

# Close the connections
oracle_conn.close()
mongo_conn.close()