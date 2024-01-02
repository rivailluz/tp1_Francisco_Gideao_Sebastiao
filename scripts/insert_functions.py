from psycopg2.extras import execute_batch


def insert_customer(cursor, customer_id):
    try:
        cursor.execute("INSERT INTO customer (id) VALUES (%s) ON CONFLICT (id) DO NOTHING;", (customer_id,))
    except Exception as e:
        print(f"Erro ao inserir cliente: {e}")


def insert_group(cursor, group_name):
    try:
        cursor.execute("INSERT INTO \"group\" (name) VALUES (%s);", (group_name,))
    except Exception as e:
        print(f"Erro ao inserir grupo: {e}")


def insert_product(cursor, asin, salesrank, title, group_id):
    try:
        cursor.execute("INSERT INTO product (ASIN, salesrank, title, id_group) VALUES (%s, %s, %s, %s);",
                       (asin, salesrank, title, group_id))
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


def insert_list_customer(conn, list_customers):
    try:
        with conn.cursor() as cursor:
            list_values = ','.join(cursor.mogrify("(%s)", (customer,)).decode("utf-8") for customer in list_customers)
            cursor.execute(f"INSERT INTO customer (cod) VALUES {list_values} ON CONFLICT (id) DO NOTHING;")
        print('Commiting customers...')
        conn.commit()
    except Exception as e:
        print(f"Erro ao inserir cliente: {e}")


def insert_list_group(conn, list_groups):
    try:
        with conn.cursor() as cursor:
            list_values = ','.join(cursor.mogrify("(%s)", (group,)).decode("utf-8") for group in list_groups)
            cursor.execute(f"INSERT INTO \"group\" (name) VALUES {list_values};")
        print('Commiting groups...')
        conn.commit()
    except Exception as e:
        print(f"Erro ao inserir grupo: {e}")


def insert_list_category(connection, list_categories):
    try:
        with connection.cursor() as cursor:
            list_values = ','.join(
                cursor.mogrify("(%s, %s, %s)", (category.id_categoria, category.nome, category.ancester_id)).decode(
                    "utf-8") for category in list_categories)
            cursor.execute(
                f"INSERT INTO category (id, name, parent_id) VALUES {list_values} ON CONFLICT (id) DO NOTHING;")
        print('Commiting categories...')
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir categoria: {e}")


# def insert_list_product(conn, list_products):
#     try:
#         with conn.cursor() as cursor:
#             list_values = [(product.id, product.ASIN, product.salesrank, product.title, product.group_id) for product in
#                            list_products]
#             cursor.execute(
#                 f'INSERT INTO product (id, ASIN, salesrank, title, id_group) VALUES %s ON CONFLICT (id) DO NOTHING;',
#                 (list_values,))
#         conn.commit()
#     except Exception as e:
#         print(f"Erro ao inserir produto: {e}")


def insert_category_product_list(connection, list_category_product):
    try:
        with connection.cursor() as cursor:
            list_values = ','.join(
                cursor.mogrify("(%s, %s)", (category_product.id_produto, category_product.id_categoria)).decode(
                    "utf-8") for category_product in list_category_product)
            cursor.execute(f"INSERT INTO category_product (id_product, id_category) VALUES {list_values};")
        print('Commiting category products...')
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir categoria do produto: {e}")


def insert_list_product(connection, list_products):
    try:
        sql_insert_all = "INSERT INTO product (id, asin, salesrank, title, id_group) VALUES "
        sql_insert_ids = "INSERT INTO product (id, asin) VALUES "
        with connection.cursor() as cursor:
            dict_group = get_groups_dictionay(cursor)
            list_values_id = ''
            list_values_all = ''
            for product in list_products:
                if product.title is None:
                    list_values_id += f"({product.id}, '{product.ASIN}'),"
                else:
                    list_values_all += f"({product.id}, '{product.ASIN}', {product.salesrank}, '{product.title}', {dict_group[product.group]}),"
            if list_values_id != '':
                cursor.execute(sql_insert_ids + list_values_id[:-1] + ";")
            if list_values_all != '':
                cursor.execute(sql_insert_all + list_values_all[:-1] + ";")

        print('Commiting products...')
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir produto: {e}")


def insert_list_similarity_products(connection, list_similarity_products):
    sql_insert = "INSERT INTO similarity_products (asin_product, asin_product_similar) VALUES "
    try:
        with connection.cursor() as cursor:
            list_values = ''
            for similarity_product in list_similarity_products:
                for product_similar in similarity_product.id_products_similares:
                    list_values += f"({similarity_product.id_product}, {product_similar}),"
            cursor.execute(sql_insert + list_values[:-1] + ";")
        print('Commiting similarity products...')
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir produto similar: {e}")


def insert_list_review(connection, list_reviews):
    sql_insert = ("INSERT INTO review (id_product, cod_customer, date_created, rating, votes, helpful) VALUES ;")
    try:
        with connection.cursor() as cursor:
            list_values = ','.join(
                cursor.mogrify("(%s, %s, %s, %s, %s, %s)", (
                    review.id_product, review.customer_id, review.date, review.rating, review.votes, review.helpful))
                    .decode("utf-8") for review in list_reviews)
            cursor.execute(sql_insert + list_values + ";")

        print("Inserindo reviews no banco de dados...")
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir revisão: {e}")
