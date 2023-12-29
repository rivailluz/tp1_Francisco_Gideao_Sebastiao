#!/usr/bin/python
from collections import OrderedDict

import psycopg2
import re
from config import config
from create_fuctions import *
from entities.product import Produto
from entities.review import Review
from entities.products_similaties import ProdutosSimilares
from datetime import datetime
from entities.category import Categoria
from entities.categoria_produto import CategoriaProduto


# F
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
        for category in list_categories:
            print(category)
            cursor.execute(sql_insert, (category.id_categoria, category.nome, category.ancester_id))
            print("Inserindo categoria no banco de dados...")
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
            print("Inserindo categoria do produto no banco de dados...")
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


# def exexute_sqls():
#     # conn = None
#     # try:
#     #     params = config()
#     #
#     #     # connect to the PostgreSQL server
#     #     print('Connecting to the PostgreSQL database...')
#     #     conn = psycopg2.connect(**params)
#     #
#     #     # create tables
#     #     # create_table_customer(conn)
#     #     # create_table_group(conn)
#     #     # create_table_category(conn)
#     #     # create_table_product(conn)
#     #     # create_table_review(conn)
#     #     # create_table_category_product(conn)
#     #     # create_table_similarity_products(conn)
#     #
#     #     # populate tables
#     #     # populate_group_table(conn)
#     # except (Exception, psycopg2.DatabaseError) as error:
#     #     print(error)
#     # finally:
#     #     if conn is not None:
#     #         conn.close()
#     #         print('Database connection closed.')
#
#     with open("../amazon-meta.txt", "r") as file:
#         try:
#             params = config()
#             connection = psycopg2.connect(**params)
#             cursor = connection.cursor()
#             list_customers = set()
#
#             list_products = []
#             product = None
#
#             list_reviews = []
#             review = None
#
#             list_similar_products = []
#             similar_product = None
#
#             list_categories = {}
#             category = None
#             padrao = re.compile(r"^(.+)\[(\d+)]$")
#
#             list_categories_produto = []
#
#             list_groups = set()
#             for number, line in enumerate(file, start=1):
#                 info = line.split()
#                 if len(info) < 1:
#                     continue
#
#                 ###################
#                 # Product
#                 ###################
#                 if info[0].startswith('Id:'):
#                     if product is not None:
#                         list_products.append(product)
#                     product = Produto(int(info[1]))
#                 if info[0].startswith('ASIN:'):
#                     product.ASIN = info[1]
#                 if info[0].startswith('title:'):
#                     product.title = " ".join(info[1:])
#                 if info[0].startswith('group:'):
#                     product.group = " ".join(info[1:])
#                 if info[0].startswith('salesrank:'):
#                     product.salesrank = int(info[1])
#
#                 ###################
#                 # GROUP
#                 ###################
#                 # if info[0].startswith('group:'):
#                 #     name_group = " ".join(info[1:])
#                 #     list_groups.add(name_group)
#
#                 ###################
#                 # CATEGORY
#                 ###################
#                 # if info[0].startswith('categories:'):
#                 #     total_categories = int(info[1])
#                 #     if total_categories > 0:
#                 #         for _ in range(total_categories):
#                 #             line = next(file)
#                 #             info_category = line.split('|')
#                 #             if len(info_category) < 1:
#                 #                 break
#                 #             id_category = None
#                 #             category_fields = padrao.match(info_category[0])
#                 #             if category_fields is not None:
#                 #                 category_name = category_fields.group(1).strip()
#                 #                 category_id = int(category_fields.group(2).strip())
#                 #                 list_categories[category_id] = (Categoria(category_id, category_name))
#                 #                 id_category = category_id
#                 #             for category in info_category[1:]:
#                 #                 category_fields = padrao.match(category)
#                 #                 if category_fields is not None:
#                 #                     category_name = category_fields.group(1).strip()
#                 #                     category_id = int(category_fields.group(2).strip())
#                 #                     list_categories[category_id] = (Categoria(category_id, category_name, id_category))
#                 #                     id_category = category_id
#                 #
#                 #             if product is not None:
#                 #                 list_categories_produto.append(CategoriaProduto(product.id, id_category))
#
#                 ###################
#                 # Product Similarities
#                 ###################
#                 # if len(info) >= 2 and info[0].startswith("similar:"):
#                 #     quantity_similar = int(info[1])
#                 #     if quantity_similar > 0:
#                 #         if similar_product is not None:
#                 #             list_similar_products.append(similar_product)
#                 #         similar_product = ProdutosSimilares(product.ASIN)
#                 #         for _ in range(quantity_similar):
#                 #             similar_product.adicionar_produto_similar(info[2 + _])
#
#                 ################
#                 # REVIEW
#                 ################
#                 if len(info) >= 1 and line.split()[0].startswith("reviews:"):
#                     total_reviews = int(line.split()[4])
#                     if total_reviews > 0:
#                         count = 0
#                         while count < total_reviews:
#                             count += 1
#                             line = next(file)
#                             info_review = line.split()
#                             if len(info_review) <= 2:
#                                 # print(info_review)
#                                 break
#
#                             if review is not None:
#                                 list_reviews.append(review)
#                             review = Review(product.id)
#                             review.date = datetime.strptime(info_review[0], "%Y-%m-%d").date()
#                             review.customer_id = info_review[2]
#                             review.rating = int(info_review[4])
#                             review.votes = int(info_review[6])
#                             review.helpful = int(info_review[8])
#
#                             customer_id = info_review[2]
#                             # date_created = datetime.strptime(line.split()[0], "%Y-%m-%d").date()
#                             # rating = int(line.split()[3])
#                             # votes = int(line.split()[5])
#                             # helpful = int(line.split()[7])
#                             # print(f"cutomer: {customer_id}")
#                             list_customers.add(customer_id)
#                             # insert_customer(cursor, customer_id)
#                             # print(f"rating: {rating}")
#                             # print(f"votes: {votes}")
#                             # print(f"helpful: {helpful}"
#
#             if product is not None:
#                 list_products.append(product)
#
#             if review is not None:
#                 list_reviews.append(review)
#             # print(list_customers)
#             # print(len(list_customers))
#
#             # for product in list_products:
#             #     print(product)
#             # print(len(list_products))
#
#             # PARTE PARA INSERIR OS DADOS NO BANCO DE CUSTOMERS
#             # print(len(list_customers))
#             # for customer in list_customers:
#             #     insert_customer(cursor, customer)
#
#             print('connecting to the PostgreSQL database...')
#
#             # PARTE PARA INSERIR OS DADOS NO BANCO DE REVIEWS
#             # for review in list_reviews:
#             #     print(review)
#             # print(len(list_reviews))
#             insert_list_review(cursor, list_reviews)
#
#             # print(len(list_similar_products))
#             # for similar_product in list_similar_products:
#             #     print(similar_product)
#
#             # while len(list_categories) > 0:
#             #     list_ancesters = []
#             #     cat = list_categories.popitem()[1]
#             #     cat_aux = cat
#             #
#             #     while cat_aux.ancester_id is not None:
#             #         try:
#             #             cat_aux = list_categories.pop(cat_aux.ancester_id)
#             #             list_ancesters.insert(0, cat_aux)
#             #         except KeyError:
#             #             print(f"Chave {cat_aux.ancester_id} não encontrada. Saindo do loop interno.")
#             #             break
#             #     for ancester in list_ancesters:
#             #         insert_category(cursor, ancester)
#             #     insert_category(cursor, cat)
#
#             # for category in list_categories.values():
#             #     list_ancesters = []
#             #     cat_aux = category
#             #     teste += 1
#             #
#             #     while cat_aux.ancester_id is not None:
#             #         list_ancesters.append(cat_aux.ancester_id)
#             #         cat_aux = list_categories[cat_aux.ancester_id]
#             #     for ancester in list_ancesters:
#             #         cat_ancester = list_categories[ancester]
#             #         if cat_ancester is not None:
#             #             teste += 1
#             #             list_categories.pop(cat_ancester.id_categoria)
#
#             # PARTE PARA INSERIR OS DADOS DE CATEGORIAS NO BANCO
#             # insert_list_category(cursor, list_categories)
#
#             # PARTE PARA INSERIR OS DADOS NO BANCO DE CATEGORIAS DE PRODUTOS
#             # for category_produto in list_categories_produto:
#             #     print(category_produto)
#             # print(len(list_categories_produto))
#             # insert_category_product_list(cursor, list_categories_produto)
#
#             # PARTE PARA INSERIR OS DADOS NO BANCO DE PRODUTOS
#             # insert_list_product(cursor, list_products)
#
#             # PARTE PARA INSERIR OS PRODUTOS SIMILARES
#             # list_asin_existentes = {}
#             # for product in list_products:
#             #     list_asin_existentes[product.ASIN] = True
#             #
#             # for similar_product in list_similar_products:
#             #     count = 0
#             #     tamanho = len(similar_product.id_products_similares)
#             #     list_remove = []
#             #     while count < tamanho:
#             #         asin_similar = similar_product.id_products_similares[count]
#             #         verification = list_asin_existentes.get(asin_similar, False)
#             #         if verification is False:
#             #             similar_product.remover_produto_similar(asin_similar)
#             #             tamanho -= 1
#             #         else:
#             #             count += 1
#             # list_asin_existentes.clear()
#             # insert_list_similarity_products(cursor, list_similar_products)
#
#             # PARTE PARA INSERIR OS DADOS NO BANCO DE GRUPO
#             # for group in list_groups:
#             #     print(group)
#             #     insert_group(cursor, group)
#             # print(list_groups)
#
#             connection.commit()
#             cursor.close()
#             connection.close()
#         except (Exception, psycopg2.DatabaseError) as error:
#             connection.rollback()
#             print(error)
#         finally:
#             if connection is not None:
#                 connection.close()
#                 print('Database connection closed.')
#
#
# if __name__ == '__main__':
#     exexute_sqls()
