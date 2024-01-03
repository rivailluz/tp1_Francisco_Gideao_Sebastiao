"""
Módulo com funções para pesquisa no banco de dados.
Autor:
Francisco
Gideão
Sebastião
Descrição: Módulo com funções para pesquisa no banco de dados.
"""

from builtins import int
import psycopg2

from config import config


def verifyIdProduct(id_product):
    """
    Verifica se o ‘id’ do produto existe no banco de dados.
    :param id_product: ‘Id’ do produto
    :type id_product: int
    :return: bool - True se o ‘id’ existe, False se não existe
    """
    params = config()
    connection = psycopg2.connect(**params)
    try:
        cursor = connection.cursor()

        sql = f"SELECT * FROM product WHERE id = {id_product};"
        cursor.execute(sql)
        result = cursor.fetchone()

        return result if result else False
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            cursor.close()
            connection.close()


def printTable(result, header):
    if result:
        print()
        for col_name in header:
            print(f"{col_name[0]:{col_name[1]}}", end=" | ")
        print()
        for col_name in header:
            print(f'{"-" * col_name[1]}', end="---")
        print()
        for row in result:
            for value in enumerate(row):
                print(f"{str(value[1]):{header[value[0]][1]}}", end=" | ")
            print()
    else:
        print("Nenhum resultado encontrado.")

def selectOptionA():
    """
    Listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação;
    A função recebe o ‘id’ do produto e realiza a consulta no banco de dados.
    Na consulta são realizados dois selects, um para os 5 comentários mais úteis e com maior avaliação e outro para os
    5 comentários mais úteis e com menor avaliação.
    """
    print(
        'Listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação;')
    params = config()
    connection = psycopg2.connect(**params)
    id_product = input('Digite o id do produto: ')
    ready_search = False
    backScreen = False
    while not ready_search:
        if id_product == 'v' or id_product == 'V':
            backScreen = True
            ready_search = True
        else:
            try:
                id_product = int(id_product)
            except ValueError:
                id_product = input('Id deve ser um número inteiro, digite novamente(ou V para voltar): ')
                continue
            asin_product = verifyIdProduct(id_product)
            if not asin_product:
                id_product = input('Id não encontrado, digite novamente(ou V para voltar): ')
                continue
            else:
                ready_search = True
    if backScreen:
        return
    try:
        cursor = connection.cursor()

        sql = f'''(select 'Maiores avaliçoes' as tipo, *
        from review
        where id_product = {id_product}
        order by rating desc, helpful desc
        limit 5)
        union
        (select 'Menores avaliaçoes' as tipo, *
        from review
        where id_product = {id_product}
        order by rating asc ,helpful desc
        limit 5)
         order by tipo, helpful desc;'''
        cursor.execute(sql)
        result = cursor.fetchall()
        # Imprimir cabeçalho
        header = (('tipo', 18), ('id_review', 15), ('id_product', 10), ('id_customer', 15), ('date-created', 12), ('rating', 6), ('votes', 5), ('helpful', 7))
        printTable(result, header)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            cursor.close()
            connection.close()
            print('Conexão com o banco de dados encerrada.')


def selectOptionB():
    """
    Listar os produtos similares com maiores vendas do que ele;
    A função recebe o ‘id’ do produto e realiza a consulta no banco de dados.
    Na consulta é realizado um select para os produtos similares a ele e com os salesrank menores.
    """
    print('Listar os produtos similares com maiores vendas do que ele;')
    params = config()
    connection = psycopg2.connect(**params)
    id_product = input('Digite o id do produto: ')
    ready_search = False
    backScreen = False
    asin_product = None
    while not ready_search:
        if id_product == 'v' or id_product == 'V':
            backScreen = True
            ready_search = True
        else:
            try:
                id_product = int(id_product)
            except ValueError:
                id_product = input('Id deve ser um número inteiro, digite novamente(ou V para voltar): ')
                continue
            asin_product = verifyIdProduct(id_product)
            if not asin_product:
                id_product = input('Id não encontrado, digite novamente(ou V para voltar): ')
                continue
            else:
                asin_product = asin_product[1]
                ready_search = True

    try:
        if not backScreen:
            print('Realizando a consulta B: ')
            cursor = connection.cursor()

            sql = f'''select product.*
            from product join similarity_products on product.ASIN = similarity_products.asin_product_similar 
            and similarity_products.asin_product = '{asin_product}'
            and product.salesrank < (select salesrank from product where id = {id_product});'''
            cursor.execute(sql)
            result = cursor.fetchall()
            # Imprimir cabeçalho
            header = (('id_product', 10), ('ASIN', 10), ('salesrank', 10), ('title', 50), ('id_group', 10))
            printTable(result, header)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            cursor.close()
            connection.close()


def selectOptionC():
    """
    Calcular a média de avaliação do produto ao longo do tempo (em dias);
    A função recebe o ‘id’ do produto e realiza a consulta no banco de dados.
    A função realiza a pesquisa das reviews do produto e calcula a média de avaliação do produto ao longo do tempo.
    """
    print('Evolução diária das médias de avaliação ao longo do intervalo de tempo coberto pelos dados.')
    params = config()
    connection = psycopg2.connect(**params)
    id_product = input('Digite o id do produto: ')
    ready_search = False
    backScreen = False

    while not ready_search:
        if id_product == 'v' or id_product == 'V':
            backScreen = True
            ready_search = True
        else:
            try:
                id_product = int(id_product)
            except ValueError:
                id_product = input('Id deve ser um número inteiro, digite novamente(ou V para voltar): ')
                continue
            asin_product = verifyIdProduct(id_product)
            if not asin_product:
                id_product = input('Id não encontrado, digite novamente(ou V para voltar): ')
                continue
            else:
                ready_search = True

    try:
        if not backScreen:
            print('Realizando a consulta C: ')
            cursor = connection.cursor()

            sql = f'''select date_created, count(*) as quantidade_de_reviews, round(avg(rating), 4) as media_avaliacao_dia,
            round(AVG(AVG(rating)) OVER (ORDER BY date_created), 4) AS media_acumulada
            from review
            where id_product = {id_product}
            group by date_created
            order by date_created;'''

            cursor.execute(sql)
            result = cursor.fetchall()
            # Imprimir cabeçalho
            header = (('date_created', 12), ('quantidade_de_reviews', 24), ('media_avaliacao_dia', 20), ('media_acumulada', 16))
            printTable(result, header)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            cursor.close()
            connection.close()


def selectOptionD():
    """
    A função realiza um select em produtos, criando uma coluna extra para coloção do ranking de vendas do produto,
    ordenando os produtos pelo ranking de vendas e limitando a 10 o número de produtos retornados.
    :return:
    """
    print('Listar os 10 produtos líderes de venda em cada grupo de produtos.')
    params = config()
    connection = psycopg2.connect(**params)

    try:
        print('Realizando a consulta D: ')
        cursor = connection.cursor()

        sql = f'''WITH RankedProducts AS (
                  SELECT "group".name as group_name, ROW_NUMBER() OVER (PARTITION BY id_group ORDER BY salesrank) AS ranking, product.*
                    FROM product inner join "group" on product.id_group = "group".id
                    where salesrank > 0)
                    SELECT *
                    FROM RankedProducts
                    WHERE ranking <= 10
                    order by group_name, ranking;'''
        cursor.execute(sql)
        result = cursor.fetchall()
        # Imprimir cabeçalho
        header = (
        ('group_name', 16), ('ranking', 8), ('id_product', 10), ('ASIN', 10), ('salesrank', 10), ('title', 50),
        ('id_group', 10))
        printTable(result, header)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            cursor.close()
            connection.close()

def selectOptionE():
    print('Listar os 10 produtos com a maior média de avaliações úteis positivas por produto')
    params = config()
    connection = psycopg2.connect(**params)

    try:
        print('Realizando a consulta E: ')
        cursor = connection.cursor()

        sql = f'''with RankingGroup as
         (select  "group".name as group_name, review.id_customer, count(*), 
         row_number() over (partition by "group".name order by count(*) desc) as ranking
                from review
                join product on review.id_product = product.id
                join "group" on product.id_group = "group".id
                group by review.id_customer, "group".name)
                select * from RankingGroup where ranking <= 10
                order by group_name, ranking;'''
        cursor.execute(sql)
        result = cursor.fetchall()
        # Imprimir cabeçalho
        header = (('group_name', 14), ('id_customer', 14), ('qdte_comment', 10), ('ranking', 8))
        printTable(result, header)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            cursor.close()
            connection.close()


def selectOptionG():
    print('Listar os 10 clientes que mais fizeram comentários por grupo de produto.')
    params = config()
    connection = psycopg2.connect(**params)

    try:
        print('Realizando a consulta G: ')
        cursor = connection.cursor()

        sql = f'''with RankingGroup as
         (select  "group".name as group_name, review.id_customer, count(*), 
         row_number() over (partition by "group".name order by count(*) desc) as ranking
                from review
                join product on review.id_product = product.id
                join "group" on product.id_group = "group".id
                group by review.id_customer, "group".name)
                select * from RankingGroup where ranking <= 10
                order by group_name, ranking;'''
        cursor.execute(sql)
        result = cursor.fetchall()
        # Imprimir cabeçalho
        header = (('group_name', 14), ('id_customer', 14), ('qdte_comment', 10), ('ranking', 8))
        printTable(result, header)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            cursor.close()
            connection.close()


def selectOptionG():
    print('Listar os 10 clientes que mais fizeram comentários por grupo de produto.')
    params = config()
    connection = psycopg2.connect(**params)

    try:
        print('Realizando a consulta G: ')
        cursor = connection.cursor()

        sql = f'''with RankingGroup as
         (select  "group".name as group_name, review.cod_customer, count(*), 
         row_number() over (partition by "group".name order by count(*) desc) as ranking
                from review
                join product on review.id_product = product.id
                join "group" on product.id_group = "group".id
                group by review.cod_customer, "group".name)
                select * from RankingGroup where ranking <= 10
                order by group_name, ranking;'''
        cursor.execute(sql)
        result = cursor.fetchall()
        # Imprimir cabeçalho
        header = (('group_name', 14), ('id_customer', 14), ('qdte_comment', 10), ('ranking', 8))
        printTable(result, header)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            cursor.close()
            connection.close()
