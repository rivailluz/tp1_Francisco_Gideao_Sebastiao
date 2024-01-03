from psycopg2.extras import execute_batch

"""
Módulo com funções para inserção de dados no banco de dados.
Autor: Francisco Rivail Santos da Luz Junior
Descrição: Módulo com funções para inserção de dados no banco de dados.
- Para as funções de inserção de um único dado, é necessário passar como parâmetro o cursor e o dado a ser inserido.
- Para as funções de inserção de uma lista de dados, é necessário passar como parâmetro a conexão e a lista de dados
 a serem inseridos.
- O método utilizado para inserção de uma lista de dados é o execute_batch, que é mais rápido que o executemany.
"""


def insert_customer(cursor, customer_id):
    """
        Função para inserir um cliente no banco de dados.

        Args:
            cursor: cursor para executar as operações no banco de dados.
            customer_id (int): ‘Id’ do cliente a ser inserido.

        """
    try:
        cursor.execute("INSERT INTO customer (id) VALUES (%s) ON CONFLICT (id) DO NOTHING;", (customer_id,))
    except Exception as e:
        print(f"Erro ao inserir cliente: {e}")


def insert_group(cursor, group_name):
    """
    Função para inserir um grupo no banco de dados.
    Args:
        cursor: cursor para executar as operações no banco de dados.
        group_name (str): Nome do grupo a ser inserido.
    """
    try:
        cursor.execute("INSERT INTO \"group\" (name) VALUES (%s);", (group_name,))
    except Exception as e:
        print(f"Erro ao inserir grupo: {e}")


def insert_product(cursor, asin, salesrank, title, group_id):
    """
    Função para inserir um produto no banco de dados.
    Args:
        cursor: cursor para executar as operações no banco de dados.
        asin (str): ‘ASIN’ do produto a ser inserido.
        salesrank (int): ‘Salesrank’ do produto a ser inserido.
        title (str): ‘Title’ do produto a ser inserido.
        group_id (int): ‘Id’ do grupo do produto a ser inserido.
    """
    try:
        cursor.execute("INSERT INTO product (ASIN, salesrank, title, id_group) VALUES (%s, %s, %s, %s);",
                       (asin, salesrank, title, group_id))
    except Exception as e:
        print(f"Erro ao inserir produto: {e}")


def insert_category(cursor, category):
    """
    Função para inserir uma categoria no banco de dados.
    Args:
        cursor: cursor para executar as operações no banco de dados.
        category (Category): Categoria a ser inserida.
    """
    try:
        sql_insert = "INSERT INTO category (id, name, parent_id) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING;"
        cursor.execute(sql_insert, (category.id_categoria, category.nome, category.ancester_id))
    except Exception as e:
        print(f"Erro ao inserir categoria: {e}")


def insert_review(cursor, product_id, customer_id, date_created, rating, votes, helpful):
    """
    Função para inserir uma revisão no banco de dados.
    Args:
        cursor: cursor para executar as operações no banco de dados.
        product_id (int): ‘Id’ do produto da revisão a ser inserida.
        customer_id (int): ‘Id’ do cliente da revisão a ser inserida.
        date_created (date): Data da revisão a ser inserida.
        rating (int): ‘Rating’ da revisão a ser inserida.
        votes (int): ‘Votes’ da revisão a ser inserida.
        helpful (int): ‘Helpful’ da revisão a ser inserida.
    """
    try:
        cursor.execute("INSERT INTO review (id_product, id_customer, date_created, rating, votes, helpful) "
                       "VALUES (%s, %s, %s, %s, %s, %s);",
                       (product_id, customer_id, date_created, rating, votes, helpful))
    except Exception as e:
        print(f"Erro ao inserir revisão: {e}")


def get_groups_dictionay(cursor):
    """
    Função para buscar os grupos no banco de dados e retornar um dicionário com os grupos.
    Args:
        cursor: cursor para executar as operações no banco de dados.
    Returns:
        dict_groups (dict): Dicionário com os grupos.
    """
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
    """
        Função para inserir uma lista de clientes no banco de dados.
        :param conn: Conexão com o banco de dados.
        :type conn: Connection
        :param list_customers: Lista de clientes a serem inseridos.
        :type list_customers: List
    """
    try:
        with conn.cursor() as cursor:
            sql = "INSERT INTO customer (cod) VALUES (%s);"
            tuplas_values = [(customer,) for customer in list_customers]
            execute_batch(cursor, sql, tuplas_values)
        print('Commiting customers...')
        conn.commit()
    except Exception as e:
        print(f"Erro ao inserir cliente: {e}")


def insert_list_group(conn, list_groups):
    """
    Função para inserir uma lista de grupos no banco de dados.

    :param conn: Conexão com o banco de dados.
    :type conn: Connection

    :param list_groups: Lista de grupos a serem inseridos.
    :type list_groups: List
    """

    try:
        with conn.cursor() as cursor:
            sql = "INSERT INTO \"group\" (name) VALUES (%s);"
            tuplas_values = [(group,) for group in list_groups]
            execute_batch(cursor, sql, tuplas_values)
        print('Commiting groups...')
        conn.commit()
    except Exception as e:
        print(f"Erro ao inserir grupo: {e}")


def insert_list_category(connection, list_categories):
    """
    Função para inserir uma lista de categorias no banco de dados.
    :param connection: Conexão com o banco de dados.
    :type connection: Connection
    :param list_categories: Lista de categorias a serem inseridas.
    :type list_categories: List
    """

    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO category (id, name, parent_id) VALUES (%s, %s, %s);"
            tuplas_values = [(category.id_categoria, category.nome, category.ancester_id) for category in
                             list_categories.values()]
            execute_batch(cursor, sql, tuplas_values)
        print('Commiting categories...')
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir categoria: {e}")


def insert_category_product_list(connection, list_category_product):
    """
    Função para inserir uma lista de categorias de produtos no banco de dados.
    :param connection: Conexão com o banco de dados.
    :param list_category_product: Lista de categorias de produtos a serem inseridas.
    """
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO category_product (id_product, id_category) VALUES (%s, %s);"
            tuplas_values = [(category_product.id_produto, category_product.id_categoria) for category_product in
                             list_category_product]
            execute_batch(cursor, sql, tuplas_values)
        print('Commiting category products...')
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir categoria do produto: {e}")


def insert_list_product(connection, list_products):
    """
    Função para inserir uma lista de produtos no banco de dados.
    :param connection: Conexão com o banco de dados.
    :type connection: Connection
    :param list_products: Lista de produtos a serem inseridos.
    :type list_products: List
    """
    try:
        with connection.cursor() as cursor:
            dict_group = get_groups_dictionay(cursor)
            sqlAll = "INSERT INTO product (id, asin, salesrank, title, id_group) VALUES (%s, %s, %s, %s, %s);"
            sqlTwo = "INSERT INTO product (id, asin) VALUES (%s, %s);"
            tuplas_values_all = [(product.id, product.ASIN, product.salesrank, product.title, dict_group[product.group])
                                 for product in list_products if product.title is not None]
            tuplas_values_id = [(product.id, product.ASIN) for product in list_products if product.title is None]

            execute_batch(cursor, sqlAll, tuplas_values_all)
            execute_batch(cursor, sqlTwo, tuplas_values_id)

        print('Commiting products...')
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir produto: {e}")


def insert_list_similarity_products(connection, list_similarity_products):
    """
    Função para inserir uma lista de produtos similares no banco de dados.
    :param connection: Conexão com o banco de dados.
    :type connection: Connection
    :param list_similarity_products: Lista de produtos similares a serem inseridos.
    :type list_similarity_products: List
    """
    sql_insert = "INSERT INTO similarity_products (asin_product, asin_product_similar) VALUES "
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO similarity_products (asin_product, asin_product_similar) VALUES (%s, %s);"
            tuplas_values = [(similarity_product.id_product, product_similar) for similarity_product in
                             list_similarity_products for product_similar in
                             similarity_product.id_products_similares]
            execute_batch(cursor, sql, tuplas_values)
        print('Commiting similarity products...')
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir produto similar: {e}")


def insert_list_review(connection, list_reviews):
    """
    Função para inserir uma lista de revisões no banco de dados.
    :param connection: Conexão com o banco de dados.
    :type connection: Connection
    :param list_reviews: Lista de revisões a serem inseridas.
    :type list_reviews: List
    """
    try:
        sql = ("INSERT INTO review (id_product, cod_customer, date_created, rating, votes, helpful) "
               "VALUES (%s, %s, %s, %s, %s, %s);")
        with connection.cursor() as cursor:
            tuplas_values = [(review.id_product, review.customer_id, review.date, review.rating, review.votes,
                              review.helpful) for review in list_reviews]
            execute_batch(cursor, sql, tuplas_values)
        print("Inserindo reviews no banco de dados...")
        connection.commit()
    except Exception as e:
        print(f"Erro ao inserir revisão: {e}")
