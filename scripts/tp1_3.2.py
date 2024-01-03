import psycopg2
import sys
import re
import time

from create_fuctions import createAllTables
from config import config

from entities.product import Produto
from entities.review import Review
from entities.products_similaties import ProdutosSimilares
from datetime import datetime
from entities.category import Categoria
from entities.categoria_produto import CategoriaProduto

from insert_functions import *


def create_database():
    params = config(default=True)
    print(params)
    connection = psycopg2.connect(**params)
    try:
        connection.autocommit = True

        exists_query = "SELECT * FROM pg_database where datname = 'amazon';"
        with connection.cursor() as cursor:
            cursor.execute(exists_query)
            database_exists = cursor.fetchone()

        if not database_exists:
            cursor = connection.cursor()
            cursor.execute("CREATE DATABASE amazon;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()

if __name__ == '__main__':
    argumentos = sys.argv[1:]
    path = "../amazon-meta.txt"
    if len(argumentos) >= 1:
        for argumento in argumentos:
            if argumento == 'create':
                create_database()
            else:
                path = argumento


    print('Trabalho Prático 1 - 2023.2')
    print('Criando tabelas...')
    createAllTables()

    print('Separando os dados do arquivo...')
    start_time = time.time()
    with open(path, "r", encoding='utf-8') as file:
        try:
            params = config()
            connection = psycopg2.connect(**params)
            cursor = connection.cursor()

            list_customers = set()

            list_products = []
            product = None

            list_reviews = []
            review = None

            list_similar_products = []
            similar_product = None

            list_categories = {}
            category = None
            padrao = re.compile(r"^(.+)\[(\d+)]$")

            list_categories_produto = set()

            list_groups = set()
            start_time = time.time()

            for line in file:
                info = line.split()
                if len(info) < 1:
                    continue

                ###################
                # Product
                ###################
                if info[0].startswith('Id:'):
                    if product is not None:
                        list_products.append(product)
                    product = Produto(int(info[1]))
                elif info[0].startswith('ASIN:'):
                    product.ASIN = info[1]
                elif info[0].startswith('title:'):
                    product.title = " ".join(info[1:])
                elif info[0].startswith('group:'):
                    ###################
                    # GROUP
                    ###################
                    group_name = " ".join(info[1:])
                    product.group = group_name
                    list_groups.add(group_name)
                elif info[0].startswith('salesrank:'):
                    product.salesrank = int(info[1])


                ###################
                # CATEGORY
                ###################
                elif info[0].startswith('categories:'):
                    total_categories = int(info[1])
                    count = 0
                    while count < total_categories:
                        line = next(file)
                        info_category = line.split('|')
                        if len(info_category) < 1:
                            break
                        id_category = None
                        category_fields = padrao.match(info_category[0])
                        if category_fields is not None:
                            category_name = category_fields.group(1).strip()
                            category_id = int(category_fields.group(2).strip())
                            list_categories[category_id] = (Categoria(category_id, category_name))
                            id_category = category_id
                        for category in info_category[1:]:
                            category_fields = padrao.match(category)
                            if category_fields is not None:
                                category_name = category_fields.group(1).strip()
                                category_id = int(category_fields.group(2).strip())
                                list_categories[category_id] = (Categoria(category_id, category_name, id_category))
                                id_category = category_id
                        list_categories_produto.add(CategoriaProduto(product.id, id_category))
                        count += 1

                ###################
                # Product Similarities
                ###################
                elif len(info) >= 2 and info[0].startswith("similar:"):
                    quantity_similar = int(info[1])
                    if quantity_similar > 0:
                        if similar_product is not None:
                            list_similar_products.append(similar_product)
                        similar_product = ProdutosSimilares(product.ASIN)
                        count = 0
                        while count < quantity_similar:
                            similar_product.adicionar_produto_similar(info[2 + count])
                            count += 1

                ################
                # REVIEW
                ################
                elif line.split()[0].startswith("reviews:"):
                    total_reviews = int(line.split()[4])
                    if total_reviews > 0:
                        for i in range(total_reviews):
                            line = next(file)
                            info_review = line.split()

                            review = Review(product.id)
                            review.date = datetime.strptime(info_review[0], "%Y-%m-%d").date()
                            review.customer_id = info_review[2]
                            review.rating = int(info_review[4])
                            review.votes = int(info_review[6])
                            review.helpful = int(info_review[8])

                            customer_id = info_review[2]
                            list_customers.add(customer_id)
                            list_reviews.append(review)

            if product is not None:
                list_products.append(product)

            if review is not None:
                list_reviews.append(review)

            end_time = time.time()

            print('Tempo de execução com array: ', end_time - start_time)
            print('Tempo de inicio: ', start_time)
            print('Tempo de fim: ', end_time)

            print('Inserindo dados no banco de dados...')

            # PARTE PARA INSERIR OS DADOS NO BANCO DE CUSTOMERS
            print('Inserindo customers no banco de dados...')
            insert_list_customer(connection, list_customers)
            list_customers.clear()
            # PARTE PARA INSERIR OS DADOS NO BANCO DE GRUPO
            print('Inserindo grupos no banco de dados...')
            insert_list_group(connection, list_groups)
            list_groups.clear()

            # PARTE PARA INSERIR OS DADOS NO BANCO DE PRODUTOS
            print('Inserindo produtos no banco de dados...')
            insert_list_product(connection, list_products)

            # PARTE PARA INSERIR OS PRODUTOS SIMILARES
            # Nesta parte, é necessário verificar se os produtos similares já existem no banco de dados, pois um produto
            # similar pode existir no arquivo de entrada como produto similar porem não existir como produo no arquivo
            list_asin_existentes = {}
            print('Inserindo produtos similares no banco de dados...')
            for product in list_products:
                list_asin_existentes[product.ASIN] = True

            for similar_product in list_similar_products:
                count = 0
                tamanho = len(similar_product.id_products_similares)
                list_remove = []
                while count < tamanho:
                    asin_similar = similar_product.id_products_similares[count]
                    verification = list_asin_existentes.get(asin_similar, False)
                    if verification is False:
                        similar_product.remover_produto_similar(asin_similar)
                        tamanho -= 1
                    else:
                        count += 1
            list_asin_existentes.clear()
            insert_list_similarity_products(connection, list_similar_products)
            list_similar_products.clear()
            list_products.clear()

            # PARTE PARA INSERIR OS DADOS DE CATEGORIAS NO BANCO
            print('Inserindo categorias no banco de dados...')
            insert_list_category(connection, list_categories)
            list_categories.clear()

            # PARTE PARA INSERIR OS DADOS NO BANCO DE CATEGORIAS DE PRODUTOS
            print('Inserindo categorias de produtos no banco de dados...')
            insert_category_product_list(connection, list_categories_produto)
            list_categories_produto.clear()

            # PARTE PARA INSERIR OS DADOS NO BANCO DE REVIEWS
            print('Inserindo reviews no banco de dados...')
            insert_list_review(connection, list_reviews)
            list_reviews.clear()

            connection.commit()
            cursor.close()
            connection.close()
            print('Dados inseridos com sucesso.')
            end_time = time.time()
            print('Tempo de execução com banco: ', end_time - start_time)
            print('Tempo de inicio: ', start_time)
            print('Tempo de fim: ', end_time)
        except (Exception, psycopg2.DatabaseError) as error:
            connection.rollback()
            print(error)
        except Exception as e:
            print(e)
            print('Fim do arquivo.')
        finally:
            if connection is not None:
                connection.close()
                print('Database connection closed.')
