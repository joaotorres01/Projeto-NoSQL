import cx_Oracle
import pandas as pd
import numpy as np
from neo4j import GraphDatabase

# Oracle DB connection details
oracle_host = 'localhost'
oracle_port = '1521'
oracle_sid = 'xe'
oracle_user = 'store'
oracle_password = 'pass'

# Establish connections to Oracle DB and MongoDB
oracle_conn = cx_Oracle.connect(f"{oracle_user}/{oracle_password}@{oracle_host}:{oracle_port}/{oracle_sid}")

def run_cypher_query(query):
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "mario12345"))
    with driver.session() as session:
        result = session.run(query)
        return result

def fetch_data_from_oracle(query):
    cursor = oracle_conn.cursor()
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


from py2neo import Graph

# Criação de nós User
def migrate_user_data():
    user_query = """
        SELECT user_id, first_name, middle_name, last_name, phone_number, email, username, user_password, registered_at
        FROM store_users
    """
    user_data = fetch_data_from_oracle(user_query)

    for row in user_data:
        user_id = row['USER_ID']
        first_name = row['FIRST_NAME']
        middle_name = row['MIDDLE_NAME']
        last_name = row['LAST_NAME']
        phone_number = row['PHONE_NUMBER']
        email = row['EMAIL']
        username = row['USERNAME']
        user_password = row['USER_PASSWORD']
        registered_at = row['REGISTERED_AT']

        cypher_query = f"CREATE (u:User {{user_id: {user_id}, first_name: '{first_name}', middle_name: '{middle_name}', last_name: '{last_name}', phone_number: '{phone_number}', email: '{email}', username: '{username}', user_password: '{user_password}', registered_at: '{registered_at}'}})"
        run_cypher_query(cypher_query)

# Criação de nós Session e relacionamento HAS_SESSION
def migrate_session_data():
    session_query = """
        SELECT session_id, user_id, created_at, modified_at
        FROM shopping_session
    """
    session_data = fetch_data_from_oracle(session_query)

    for row in session_data:
        session_id = row['SESSION_ID']
        user_id = row['USER_ID']
        created_at = row['CREATED_AT']
        modified_at = row['MODIFIED_AT']

        cypher_query = f"MATCH (u:User {{user_id: {user_id}}}) " \
                       f"CREATE (s:Session {{session_id: {session_id}, created_at: '{created_at}', modified_at: '{modified_at}'}}), " \
                       f"(u)-[:HAS_SESSION]->(s)"
        
        run_cypher_query(cypher_query)





# Criação de nós Cart e relacionamento HAS_CART
def migrate_cart_data():
    cart_query = """
        SELECT cart_item_id, session_id, product_id, quantity, created_at, modified_at
        FROM cart_item
    """
    cart_data = fetch_data_from_oracle(cart_query)

    for row in cart_data:
        cart_item_id = row['CART_ITEM_ID']
        session_id = row['SESSION_ID']
        product_id = row['PRODUCT_ID']
        quantity = row['QUANTITY']
        created_at = row['CREATED_AT']
        modified_at = row['MODIFIED_AT']
        
        cypher_query = f"MATCH (s:Session {{session_id: {session_id}}}) " \
                       f"CREATE (c:Cart {{cart_item_id: {cart_item_id}, " \
                       f"product_id: {product_id}, " \
                       f"quantity: {quantity}, " \
                       f"created_at: '{created_at}', " \
                       f"modified_at: '{modified_at}'}}), " \
                       f"(s)-[:HAS_CART]->(c)"
        run_cypher_query(cypher_query)



# Criação de nós Address
def migrate_address_data():
    address_query = """
        SELECT adress_id, line_1, line_2, city, zip_code, province, country
        FROM addresses
    """
    address_data = fetch_data_from_oracle(address_query)

    for row in address_data:
        address_id = row['ADRESS_ID']
        line_1 = row['LINE_1']
        line_2 = row['LINE_2']
        city = row['CITY']
        zip_code = row['ZIP_CODE']
        province = row['PROVINCE']
        country = row['COUNTRY']

        cypher_query = f"CREATE (a:Address {{address_id: '{address_id}', line_1: '{line_1}', line_2: '{line_2}', city: '{city}', zip_code: '{zip_code}', province: '{province}', country: '{country}'}})"
        run_cypher_query(cypher_query)


# Criação de nós Product e relacionamento BELONGS_TO
def migrate_product_data():
    product_query = """
        SELECT product_id, product_name, category_id, sku, price, discount_id, created_at, last_modified
        FROM product
    """
    product_data = fetch_data_from_oracle(product_query)

    for row in product_data:
        product_id = row['PRODUCT_ID']
        name = row['PRODUCT_NAME']
        category_id = row['CATEGORY_ID']
        sku = row['SKU']
        price = row['PRICE']
        discount_id = row['DISCOUNT_ID']
        created_at = row['CREATED_AT']
        last_modified = row['LAST_MODIFIED']

        cypher_query = f"MATCH (c:Category {{category_id: {category_id}}}) " \
                       f"CREATE (p:Product {{product_id: {product_id}, product_name: '{name}', " \
                       f"sku: '{sku}', price: {price}, " \
                       f"discount_id: {discount_id if discount_id is not None else 'NULL'}, " \
                       f"created_at: '{created_at}', last_modified: '{last_modified}'}}), " \
                       f"(p)-[:BELONGS_TO]->(c)"
        run_cypher_query(cypher_query)


# Criação de nós Category
def migrate_category_data():
    category_query = """
        SELECT category_id, category_name
        FROM product_categories
    """
    category_data = fetch_data_from_oracle(category_query)

    for row in category_data:
        category_id = row['CATEGORY_ID']
        category_name = row['CATEGORY_NAME']

        cypher_query = f"CREATE (c:Category {{category_id: {category_id}, category_name: '{category_name}'}})"
        run_cypher_query(cypher_query)


# Criação de nós Discount e relacionamento HAS_DISCOUNT
def migrate_discount_data():
    discount_query = """
        SELECT discount_id, discount_name, discount_desc, discount_percent, is_active_status, created_at, modified_at
        FROM discount
    """
    discount_data = fetch_data_from_oracle(discount_query)

    for row in discount_data:
        discount_id = row['DISCOUNT_ID']
        discount_name = row['DISCOUNT_NAME']
        discount_desc = row['DISCOUNT_DESC']
        discount_percent = row['DISCOUNT_PERCENT']
        is_active_status = row['IS_ACTIVE_STATUS']
        created_at = row['CREATED_AT']
        modified_at = row['MODIFIED_AT']

        cypher_query = f"MATCH (p:Product {{discount_id: {discount_id}}}) " \
                       f"CREATE (d:Discount {{discount_id: {discount_id}, " \
                       f"discount_name: '{discount_name}', discount_desc: '{discount_desc}', " \
                       f"discount_percent: {discount_percent}, " \
                       f"is_active_status: '{is_active_status}', " \
                       f"created_at: '{created_at}', modified_at: '{modified_at}'}}), " \
                       f"(p)-[:HAS_DISCOUNT]->(d)"
        run_cypher_query(cypher_query)

# Criação de nós Payment
def migrate_payment_data():
    payment_query = """
        SELECT payment_details_id, order_id, amount, provider, payment_status, created_at, modified_at
        FROM payment_details
    """
    payment_data = fetch_data_from_oracle(payment_query)

    for row in payment_data:
        payment_details_id = row['PAYMENT_DETAILS_ID']
        order_id = row['ORDER_ID']
        amount = row['AMOUNT']
        provider = row['PROVIDER']
        payment_status = row['PAYMENT_STATUS']
        created_at = row['CREATED_AT']
        modified_at = row['MODIFIED_AT']

        cypher_query = f"CREATE (p:Payment {{payment_details_id: {payment_details_id}, " \
                       f"order_id: {order_id}, amount: {amount}, provider: '{provider}', " \
                       f"payment_status: '{payment_status}', created_at: '{created_at}', " \
                       f"modified_at: '{modified_at}'}})"
        run_cypher_query(cypher_query)

# Criação de nós Employee e relacionamentos BELONGS_TO e MANAGES
def migrate_employee_data():
    employee_query = """
        SELECT employee_id, first_name, middle_name, last_name, date_of_birth, department_id,
               hire_date, salary, phone_number, email, ssn_number, manager_id
        FROM employees
    """
    employee_data = fetch_data_from_oracle(employee_query)

    for row in employee_data:
        employee_id = row['EMPLOYEE_ID']
        first_name = row['FIRST_NAME']
        middle_name = row['MIDDLE_NAME']
        last_name = row['LAST_NAME']
        date_of_birth = row['DATE_OF_BIRTH']
        department_id = row['DEPARTMENT_ID']
        hire_date = row['HIRE_DATE']
        salary = row['SALARY']
        phone_number = row['PHONE_NUMBER']
        email = row['EMAIL']
        ssn_number = row['SSN_NUMBER']
        manager_id = row['MANAGER_ID']

        cypher_query = (
            f"CREATE (e:Employee {{"
            f"employee_id: {employee_id}, "
            f"first_name: '{first_name}', "
            f"middle_name: '{middle_name if middle_name else ''}', "
            f"last_name: '{last_name}', "
            f"date_of_birth: '{date_of_birth}', "
            f"department_id: {department_id}, "
            f"hire_date: '{hire_date}', "
            f"salary: {salary}, "
            f"phone_number: '{phone_number if phone_number else ''}', "
            f"email: '{email if email else ''}', "
            f"ssn_number: '{ssn_number}'"
            "})"
        )
        run_cypher_query(cypher_query)

        if manager_id:
            cypher_query = (
                f"MATCH (manager:Employee {{employee_id: {manager_id}}}), "
                f"(e:Employee {{employee_id: {employee_id}}}) "
                "CREATE (manager)-[:MANAGES]->(e)"
            )
            run_cypher_query(cypher_query)

# Criação de nós Department
def migrate_department_data():
    department_query = """
        SELECT department_id, department_name, manager_id, department_desc
        FROM departments
    """
    department_data = fetch_data_from_oracle(department_query)

    for row in department_data:
        department_id = row['DEPARTMENT_ID']
        name = row['DEPARTMENT_NAME']
        manager_id = row['MANAGER_ID']
        department_desc = row['DEPARTMENT_DESC'].replace("'", "")  # Remove single quotes

        cypher_query = (
            "CREATE (d:Department {"
            f"department_id: {department_id}, "
            f"department_name: '{name}', "
            f"manager_id: {manager_id}, "
            f"department_desc: '{department_desc}'"
            "})"
        )
        run_cypher_query(cypher_query)


# Criação de nós Stock e relacionamento HAS_STOCK
def migrate_stock_data():
    stock_query = """
        SELECT product_id, quantity, max_stock_quantity, unit
        FROM stock
    """
    stock_data = fetch_data_from_oracle(stock_query)

    for row in stock_data:
        product_id = row['PRODUCT_ID']
        quantity = row['QUANTITY']
        max_stock_quantity = row['MAX_STOCK_QUANTITY']
        unit = row['UNIT']
        
        cypher_query = (
            f"MATCH (p:Product {{product_id: {product_id}}}) "
            f"CREATE (s:Stock {{"
            f"product_id: {product_id}, "
            f"quantity: {quantity}, "
            f"max_stock_quantity: {max_stock_quantity}, "
            f"unit: '{unit}'"
            f"}}), "
            "(p)-[:HAS_STOCK]->(s)"
        )
        run_cypher_query(cypher_query)


def create_deliver_at_relationship():
    deliver_at_query = """
        SELECT delivery_adress_id, order_details_id
        FROM order_details
    """
    deliver_at_data = fetch_data_from_oracle(deliver_at_query)

    for row in deliver_at_data:
        delivery_address_id = row['DELIVERY_ADRESS_ID']
        order_details_id = row['ORDER_DETAILS_ID']
        cypher_query = (
            f"MATCH (a:Address {{address_id: '{delivery_address_id}'}}), "
            f"(o:OrderDetails {{order_details_id: {order_details_id}}}) "
            "CREATE (a)-[:DELIVER_AT]->(o)"
        )
        run_cypher_query(cypher_query)


def create_order_details_and_relationships():
    order_details_query = """
        SELECT order_details_id, user_id, total, payment_id, delivery_adress_id
        FROM order_details
    """
    order_details_data = fetch_data_from_oracle(order_details_query)

    for row in order_details_data:
        order_details_id = row['ORDER_DETAILS_ID']
        user_id = row['USER_ID']
        total = row['TOTAL']
        payment_id = row['PAYMENT_ID']
        delivery_address_id = row['DELIVERY_ADRESS_ID']

        # Create order_details node
        cypher_query = (
            f"CREATE (od:OrderDetails {{order_details_id: {order_details_id}, "
            f"user_id: {user_id}, total: {total}, payment_id: {payment_id}, "
            f"delivery_address_id: {delivery_address_id}}})"
        )
        run_cypher_query(cypher_query)

        # Create relationship between order_items and order_details
        order_items_query = f"SELECT order_items_id, product_id FROM order_items WHERE order_details_id = {order_details_id}"
        order_items_data = fetch_data_from_oracle(order_items_query)

        for item_row in order_items_data:
            order_items_id = item_row['ORDER_ITEMS_ID']
            product_id = item_row['PRODUCT_ID']

            # Create order_items node
            cypher_query = (
                f"CREATE (oi:OrderItems {{order_items_id: {order_items_id}}})"
            )
            run_cypher_query(cypher_query)

            # Create relationship between order_items and order_details
            cypher_query = (
                f"MATCH (od:OrderDetails {{order_details_id: {order_details_id}}}), "
                f"(oi:OrderItems {{order_items_id: {order_items_id}}}) "
                "CREATE (oi)-[:ORDERED_IN]->(od)"
            )
            run_cypher_query(cypher_query)

            # Create relationship between product and order_items
            cypher_query = (
                f"MATCH (p:Product {{product_id: {product_id}}}), "
                f"(oi:OrderItems {{order_items_id: {order_items_id}}}) "
                "CREATE (p)-[:IS_IN_ORDER_ITEM]->(oi)"
            )
            run_cypher_query(cypher_query)

def create_ordered_relationship():
    order_details_query = """
        SELECT order_details_id, user_id
        FROM order_details
    """
    order_details_data = fetch_data_from_oracle(order_details_query)

    for row in order_details_data:
        order_details_id = row['ORDER_DETAILS_ID']
        user_id = row['USER_ID']

        # Create relationship between user and order_details
        cypher_query = (
            f"MATCH (u:User {{user_id: {user_id}}}), "
            f"(od:OrderDetails {{order_details_id: {order_details_id}}}) "
            "CREATE (u)-[:ORDERED]->(od)"
        )
        run_cypher_query(cypher_query)

def create_employees_archive_node():
    employees_archive_query = """
        SELECT *
        FROM employees_archive
    """
    employees_archive_data = fetch_data_from_oracle(employees_archive_query)

    for row in employees_archive_data:
        event_date = row['EVENT_DATE']
        event_type = row['EVENT_TYPE']
        user_name = row['USER_NAME']
        old_employee_id = row['OLD_EMPLOYEE_ID']
        old_first_name = row['OLD_FIRST_NAME']
        old_middle_name = row['OLD_MIDDLE_NAME']
        old_last_name = row['OLD_LAST_NAME']
        old_date_of_birth = row['OLD_DATE_OF_BIRTH']
        old_department_id = row['OLD_DEPARTMENT_ID']
        old_hire_date = row['OLD_HIRE_DATE']
        old_salary = row['OLD_SALARY']
        old_phone_number = row['OLD_PHONE_NUMBER']
        old_email = row['OLD_EMAIL']
        old_ssn_number = row['OLD_SSN_NUMBER']
        old_manager_id = row['OLD_MANAGER_ID']
        new_employee_id = row['NEW_EMPLOYEE_ID']
        new_first_name = row['NEW_FIRST_NAME']
        new_middle_name = row['NEW_MIDDLE_NAME']
        new_last_name = row['NEW_LAST_NAME']
        new_date_of_birth = row['NEW_DATE_OF_BIRTH']
        new_department_id = row['NEW_DEPARTMENT_ID']
        new_hire_date = row['NEW_HIRE_DATE']
        new_salary = row['NEW_SALARY']
        new_phone_number = row['NEW_PHONE_NUMBER']
        new_email = row['NEW_EMAIL']
        new_ssn_number = row['NEW_SSN_NUMBER']
        new_manager_id = row['NEW_MANAGER_ID']

         # Create EmployeesArchive node
        cypher_query = (
                f"CREATE (ea:EmployeesArchive {{"
                f"event_date: '{event_date}', "
                f"event_type: '{event_type}', "
                f"user_name: '{user_name}', "
                f"old_employee_id: {old_employee_id if old_employee_id is not None else 'NULL'}, "
                f"old_first_name: '{old_first_name if old_first_name is not None else 'NULL'}', "
                f"old_middle_name: '{old_middle_name if old_middle_name is not None else 'NULL'}', "
                f"old_last_name: '{old_last_name if old_last_name is not None else 'NULL'}', "
                f"old_date_of_birth: '{old_date_of_birth if old_date_of_birth is not None else 'NULL'}', "
                f"old_department_id: {old_department_id if old_department_id is not None else 'NULL'}, "
                f"old_hire_date: '{old_hire_date if old_hire_date is not None else 'NULL'}', "
                f"old_salary: {old_salary if old_salary is not None else 'NULL'}, "
                f"old_phone_number: '{old_phone_number if old_phone_number is not None else 'NULL'}', "
                f"old_email: '{old_email if old_email is not None else 'NULL'}', "
                f"old_ssn_number: '{old_ssn_number if old_ssn_number is not None else 'NULL'}', "
                f"old_manager_id: {old_manager_id if old_manager_id is not None else 'NULL'}, "
                f"new_employee_id: {new_employee_id if new_employee_id is not None else 'NULL'}, "
                f"new_first_name: '{new_first_name if new_first_name is not None else 'NULL'}', "
                f"new_middle_name: '{new_middle_name if new_middle_name is not None else 'NULL'}', "
                f"new_last_name: '{new_last_name if new_last_name is not None else 'NULL'}', "
                f"new_date_of_birth: '{new_date_of_birth if new_date_of_birth is not None else 'NULL'}', "
                f"new_department_id: {new_department_id if new_department_id is not None else 'NULL'}, "
                f"new_hire_date: '{new_hire_date if new_hire_date is not None else 'NULL'}', "
                f"new_salary: {new_salary if new_salary is not None else 'NULL'}, "
                f"new_phone_number: '{new_phone_number if new_phone_number is not None else 'NULL'}', "
                f"new_email: '{new_email if new_email is not None else 'NULL'}', "
                f"new_ssn_number: '{new_ssn_number if new_ssn_number is not None else 'NULL'}', "
                f"new_manager_id: {new_manager_id if new_manager_id is not None else 'NULL'}"
                f"}}) "
            )

        run_cypher_query(cypher_query)


def create_paid_relationship():
    order_details_query = """
        SELECT order_details_id, user_id, total, payment_id, delivery_adress_id
        FROM order_details
    """
    order_details_data = fetch_data_from_oracle(order_details_query)

    for row in order_details_data:
        order_details_id = row['ORDER_DETAILS_ID']
        payment_id = row['PAYMENT_ID']

        # Create relationship between order_details and payment_details
        cypher_query = (
            f"MATCH (od:OrderDetails {{order_details_id: {order_details_id}}}), "
            f"(pd:Payment {{order_id: {order_details_id}, payment_details_id: {payment_id}}}) "
            "CREATE (od)-[:IS_PAID]->(pd)"
        )
        run_cypher_query(cypher_query)



# Executar as migrações de dados
#migrate_user_data()
#migrate_session_data()
#migrate_cart_data()
#migrate_address_data()
#migrate_category_data()
#migrate_product_data()
#migrate_discount_data()
#migrate_payment_data()
#migrate_department_data()
#migrate_employee_data()
#migrate_stock_data()
#create_order_details_and_relationships()
#create_deliver_at_relationship()
#create_ordered_relationship()
#create_employees_archive_node()
create_paid_relationship()




