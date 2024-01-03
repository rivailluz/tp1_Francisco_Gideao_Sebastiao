"""
Módulo com funções para criação de tabelas no banco de dados.
Autor: Francisco Rivail Santos da Luz Junior
Descrição: Módulo com funções para criação de tabelas no banco de dados."""
from config import config
import psycopg2

def create_table_customer(conn):
    """
    Função para criar a tabela de clientes
    :param conn: Conexão com o banco de dados
    :type conn: psycopg2.extensions.connection
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customer (
                    id VARCHAR(24) PRIMARY KEY
                );
            """)
        conn.commit()
        print("Tabela 'customer' criada com sucesso.")
    except Exception as e:
        conn.rollback()
        print(f"Erro ao criar a tabela 'customer': {e}")

def create_table_group(conn):
    """
    Função para criar a tabela de grupos
    :param conn: Conexão com o banco de dados
    :type Conn: psycopg2.extensions.connection
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS "group" (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(32) UNIQUE NOT NULL
                );
            """)
        conn.commit()
        print("Tabela 'group' criada com sucesso.")
    except Exception as e:
        print(f"Erro ao criar a tabela 'group': {e}")

def create_table_category(conn):
    """
    Função para criar a tabela de categorias
    :param conn: Conexão com o banco de dados
    :type conn: psycopg2.extensions.connection
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS category (
                    id INT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    parent_id INT,
                    CONSTRAINT fk_category FOREIGN KEY (parent_id) REFERENCES category (id) 
                    on delete SET NULL on update CASCADE
                    );
            """)
        conn.commit()
        print("Tabela 'category' criada com sucesso.")
    except Exception as e:
        print(f"Erro ao criar a tabela 'category': {e}")

def create_table_product(conn):
    """
    Função para criar a tabela de produtos
    :param conn: Conexão com o banco de dados
    :type conn: psycopg2.extensions.connection
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS product (
                    id INT PRIMARY KEY,
                    ASIN VARCHAR(255) NOT NULL,
                    salesrank INT check ( salesrank >= 0 ) NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    id_group INT NOT NULL,
                    CONSTRAINT fk_group FOREIGN KEY (id_group) REFERENCES "group" (id) on delete SET NULL on update CASCADE
                );
            """)
        conn.commit()
        print("Tabela 'produtct' criada com sucesso.")
    except Exception as e:
        print(f"Erro ao criar a tabela 'produtct': {e}")

def create_table_review(conn):
    """
    Função para criar a tabela de ‘reviews’
    :param conn: Conexão com o banco de dados
    :type conn: psycopg2.extensions.connection
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS review (
                    id serial PRIMARY KEY,
                    id_product INT NOT NULL,
                    id_customer VARCHAR(24) NOT NULL,
                    date_created DATE NOT NULL,
                    rating INT NOT NULL,
                    votes INT NOT NULL,
                    helpful INT NOT NULL,
                    CONSTRAINT fk_customer FOREIGN KEY (id_customer) REFERENCES customer (cod) ON DELETE CASCADE ON UPDATE CASCADE,
                    CONSTRAINT fk_product FOREIGN KEY (id_product) REFERENCES product (id) ON DELETE CASCADE ON UPDATE CASCADE
                );
            """)
        conn.commit()
        print("Tabela 'review' criada com sucesso.")
    except Exception as e:
        print(f"Erro ao criar a tabela 'review': {e}")

def create_table_category_product(conn):
    """
    Função para criar a tabela de categorias de produtos
    :param conn: Conexão com o banco de dados
    :type conn: psycopg2.extensions.connection
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS category_product (
                    id_product INT NOT NULL,
                    id_category INT NOT NULL,
                    primary key (id_product, id_category),
                    CONSTRAINT fk_category_product FOREIGN KEY (id_category) REFERENCES category (id) on delete CASCADE on update CASCADE,
                    CONSTRAINT fk_product_category FOREIGN KEY (id_product) REFERENCES product (id) on delete CASCADE on update CASCADE
                );
            """)
        conn.commit()
        print("Tabela 'category_product' criada com sucesso.")
    except Exception as e:
        print(f"Erro ao criar a tabela 'category_product': {e}")

def create_table_similarity_products(conn):
    """
    Função para criar a tabela de produtos similares
    :param conn: Conexão com o banco de dados
    :type conn: psycopg2.extensions.connection
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS similarity_products (
                    id_product INT NOT NULL,
                    id_product_similar INT NOT NULL,
                    primary key (id_product, id_product_similar),
                    CONSTRAINT fk_product_similar FOREIGN KEY (id_product_similar) REFERENCES product (id) on delete CASCADE on update CASCADE,
                    CONSTRAINT fk_product_similar2 FOREIGN KEY (id_product) REFERENCES product (id) on delete CASCADE on update CASCADE
                );
            """)
        conn.commit()
        print("Tabela 'similarity_products' criada com sucesso.")
    except Exception as e:
        print(f"Erro ao criar a tabela 'similarity_products': {e}")

def createAllTables():
    """
    Função para criar todas as tabelas do banco de dados
    """
    params = config(default=False)
    connection = psycopg2.connect(**params)
    try:
        sql_create_customer_table = """CREATE TABLE IF NOT EXISTS customer (
                                        cod VARCHAR(16) PRIMARY KEY
                                    );"""

        sql_create_group_table = """CREATE TABLE IF NOT EXISTS "group" (
                                        id SERIAL PRIMARY KEY,
                                        name VARCHAR(32) UNIQUE NOT NULL
                                    );"""

        sql_create_category_table = """CREATE TABLE IF NOT EXISTS category (
                                        id INT PRIMARY KEY,
                                        name VARCHAR(255) NOT NULL,
                                        parent_id INT,
                                        CONSTRAINT fk_category FOREIGN KEY (parent_id) REFERENCES category (id) 
                                        on delete SET NULL on update CASCADE
                                        );"""

        sql_create_product_table = """CREATE TABLE IF NOT EXISTS product (
                                        id INT PRIMARY KEY,
                                        ASIN VARCHAR(255) NOT NULL UNIQUE,
                                        salesrank INT NULL,
                                        title VARCHAR(512) NULL,
                                        id_group INT NULL,
                                        CONSTRAINT fk_group FOREIGN KEY (id_group) REFERENCES "group" (id) 
                                        on delete SET NULL on update CASCADE
                                    );"""

        sql_create_review_table = """CREATE TABLE IF NOT EXISTS review (
                                        id serial PRIMARY KEY,
                                        id_product INT NOT NULL,
                                        cod_customer VARCHAR(16) NOT NULL,
                                        date_created DATE NOT NULL,
                                        rating INT NOT NULL,
                                        votes INT NOT NULL,
                                        helpful INT NOT NULL,
                                        CONSTRAINT fk_customer FOREIGN KEY (cod_customer) REFERENCES customer (cod) 
                                        ON DELETE CASCADE ON UPDATE CASCADE,
                                        CONSTRAINT fk_product FOREIGN KEY (id_product) REFERENCES product (id) 
                                        ON DELETE CASCADE ON UPDATE CASCADE
                                    );"""
        sql_create_category_product_table = """CREATE TABLE IF NOT EXISTS category_product (
                                        id_product INT,
                                        id_category INT, 
                                        primary key (id_product, id_category),
                                        CONSTRAINT fk_category_product 
                                        FOREIGN KEY (id_category) REFERENCES category (id) 
                                        on delete CASCADE on update CASCADE,
                                        CONSTRAINT fk_product_category FOREIGN KEY (id_product) REFERENCES product (id) 
                                        on delete CASCADE on update CASCADE
                                    );"""
        sql_create_similarity_products_table = """CREATE TABLE IF NOT EXISTS similarity_products (
            asin_product VARCHAR(255) NOT NULL,
            asin_product_similar VARCHAR(255) NOT NULL,
            primary key (asin_product, asin_product_similar),
            CONSTRAINT fk_product_similar FOREIGN KEY (asin_product) REFERENCES product (ASIN) 
            on delete CASCADE on update CASCADE,
            CONSTRAINT fk_product_similar2 FOREIGN KEY (asin_product_similar) REFERENCES product (ASIN) 
            on delete CASCADE on update CASCADE
        );"""
        # create tables
        cursor = connection.cursor()
        cursor.execute(sql_create_customer_table)
        cursor.execute(sql_create_group_table)
        cursor.execute(sql_create_category_table)
        cursor.execute(sql_create_product_table)
        cursor.execute(sql_create_review_table)
        cursor.execute(sql_create_category_product_table)
        cursor.execute(sql_create_similarity_products_table)

        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        connection.rollback()
    finally:
        if connection is not None:
            connection.close()