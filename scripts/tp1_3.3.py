import os
from scripts.search_functions import *

messageBack = 'Digite (v) para voltar e (x) para sair: '


def clearTerminal():
    print(os.name)
    os.system('clear')
    os.system('cls' if os.name == 'nt' else 'clear')


def run_option(option):
    optionFunction = None
    optionLoop = True

    if option == 'a':
        optionFunction = selectOptionA
    elif option == 'b':
        optionFunction = selectOptionB
    elif option == 'c':
        optionFunction = selectOptionC
    elif option == 'd':
        optionFunction = selectOptionD
    elif option == 'e':
        optionFunction = selectOptionB
    elif option == 'f':
        optionFunction = selectOptionB
    elif option == 'g':
        optionFunction = selectOptionG
    elif option == 'h':
        exit()

    clearTerminal()
    optionFunction()

    optionLoop = input(messageBack)
    while optionLoop != 'v':
        if optionLoop == 'v':
            optionLoop = False
        elif optionLoop == 'x':
            exit()
        else:
            optionLoop = input('Opção inválida, digite novamente: ')


def optionValid(option):
    return (option == 'a' or option == 'b' or option == 'c' or option == 'd' or option == 'e'
            or option == 'f' or option == 'g' or option == 'x')


def dashboard():
    option = 0
    while option != -1:
        clearTerminal()
        print('Trabalho Prático 1 - 2023.2')
        print('Escolha uma das opções abaixo:')
        print(
            '''a) Listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor 
avaliação;''')
        print('''b) Listar os produtos similares com maiores vendas do que ele;''')
        print(
            '''c) Mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo 
de entrada;''')
        print('''d) Listar os 10 produtos líderes de venda em cada grupo de produtos;''')
        print('''e) Listar os 10 produtos com a maior média de avaliações úteis positivas por produto;''')
        print('''f) Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto;''')
        print('''g) Listar os 10 clientes que mais fizeram comentários por grupo de produto.''')
        print('''x) Sair do programa.''')
        option = input('Digite a opção desejada: ')

        while not optionValid(option):
            option = input('Opção inválida, digite novamente: ')
        if option == 'x':
            exit()
        run_option(option)


dashboard()
