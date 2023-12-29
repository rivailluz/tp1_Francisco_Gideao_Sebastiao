def insert_customer(cursor, customer_id):
    try:
        cursor.execute("INSERT INTO customer (id) VALUES (%s) ON CONFLICT (id) DO NOTHING;", (customer_id,))
    except Exception as e:
        print(f"Erro ao inserir cliente: {e}")

def insert_list_customer(conn, list_customers):
    try:
        sql_insert = "INSERT INTO customer (id) VALUES (%s) ON CONFLICT (id) DO NOTHING;"
        with conn.cursor() as cursor:
            for customer in list_customers:
                cursor.execute(sql_insert, (customer,))
        print('Commiting customers...')
        conn.commit()
    except Exception as e:
        print(f"Erro ao inserir cliente: {e}")

def insert_group(cursor, group_name):
    try:
        cursor.execute("INSERT INTO \"group\" (name) VALUES (%s);", (group_name,))
    except Exception as e:
        print(f"Erro ao inserir grupo: {e}")

def insert_list_group(conn, list_groups):
    try:
        sql_insert = "INSERT INTO \"group\" (name) VALUES (%s);"
        with conn.cursor() as cursor:
            for group in list_groups:
                cursor.execute(sql_insert, (group,))
        print('Commiting groups...')
        conn.commit()
    except Exception as e:
        print(f"Erro ao inserir grupo: {e}")


def insert_list_category(connection, list_categories):
    try:
        cursor = connection.cursor()
        sql_insert = "INSERT INTO category (id, name, parent_id) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING;"
        for category in list_categories.values():
            cursor.execute(sql_insert, (category.id_categoria, category.nome, category.ancester_id))
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir categoria: {e}")


def insert_product(cursor, asin, salesrank, title, group_id):
    try:
        cursor.execute("INSERT INTO product (ASIN, salesrank, title, id_group) VALUES (%s, %s, %s, %s);",
                       (asin, salesrank, title, group_id))
    except Exception as e:
        print(f"Erro ao inserir produto: {e}")

def insert_list_product(conn, list_products):
    try:
        sql_insert = "INSERT INTO product (id, ASIN, salesrank, title, id_group) VALUES (%s, %s, %s, %s, %s);"
        with conn.cursor() as cursor:
            for product in list_products:
                cursor.execute(sql_insert, (product.id, product.ASIN, product.salesrank, product.title, product.group))
        conn.commit()
    except Exception as e:
        print(f"Erro ao inserir produto: {e}")


def insert_category(cursor, category):
    try:
        sql_insert = "INSERT INTO category (id, name, parent_id) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING;"
        cursor.execute(sql_insert, (category.id_categoria, category.nome, category.ancester_id))
    except Exception as e:
        print(f"Erro ao inserir categoria: {e}")


def insert_review(cursor, product_id, customer_id, date_created, rating, votes, helpful):
    try:
        cursor.execute("INSERT INTO review (id_product, id_customer, date_created, rating, votes, helpful) "
                       "VALUES (%s, %s, %s, %s, %s, %s);",
                       (product_id, customer_id, date_created, rating, votes, helpful))
    except Exception as e:
        print(f"Erro ao inserir revisão: {e}")


def get_groups_dictionay(cursor):
    try:
        cursor.execute("SELECT id, name FROM \"group\";")
        groups = cursor.fetchall()
        dict_groups = {}
        for group in groups:
            dict_groups[group[1]] = group[0]
        return dict_groups
    except Exception as e:
        print(f"Erro ao buscar grupos: {e}")


def insert_category_product_list(connection, list_category_product):
    try:
        cursor = connection.cursor()
        sql_insert = "INSERT INTO category_product (id_product, id_category) VALUES (%s, %s);"
        for category_product in list_category_product:
            cursor.execute(sql_insert, (category_product.id_produto, category_product.id_categoria))
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir categoria do produto: {e}")


def insert_list_product(connection, list_products):
    try:
        sql_insert_all = "INSERT INTO product (id, asin, salesrank, title, id_group) VALUES (%s, %s, %s, %s, %s);"
        sql_insert_ids = "INSERT INTO product (id, asin) VALUES (%s, %s);"
        cursor = connection.cursor()
        dict_group = get_groups_dictionay(cursor)
        for product in list_products:
            if product.title is None:
                cursor.execute(sql_insert_ids, (product.id, product.ASIN))
            else:
                cursor.execute(sql_insert_all, (product.id, product.ASIN, product.salesrank,
                                                product.title, dict_group[product.group]))
        print('Commiting products...')
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir produto: {e}")


def insert_list_similarity_products(connection, list_similarity_products):
    sql_insert = "INSERT INTO similarity_products (asin_product, asin_product_similar) VALUES (%s, %s);"
    try:
        cursor = connection.cursor()
        for similarity_product in list_similarity_products:
            for product_similar in similarity_product.id_products_similares:
                cursor.execute(sql_insert, (similarity_product.id_product, product_similar))
        print('Commiting similarity products...')
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir produto similar: {e}")


def insert_list_review(connection, list_reviews):
    sql_insert = ("INSERT INTO review (id_product, id_customer, date_created, rating, votes, helpful) VALUES (%s, %s, "
                  "%s, %s, %s, %s);")
    try:
        cursor = connection.cursor()
        for review in list_reviews:
            cursor.execute(sql_insert, (
                review.id_product, review.customer_id, review.date, review.rating, review.votes, review.helpful))
        print("Inserindo reviews no banco de dados...")
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir revisão: {e}")

