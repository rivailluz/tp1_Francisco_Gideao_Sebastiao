# Repositorio Forkado do Trabalho Prático 1 de Bancos de Dados I

# Trabalho Prático 1 de Bancos de Dados I
Os detalhes sobre o trabalho prático estão disponíveis [aqui](https://docs.google.com/document/d/1CXf_y392fJ_KNTZbdr5TWSRgEuYXFPyGTJOh4DcqOdA/edit):
### Requisitos
Para utilizar este projeto, certifique-se de ter instalado em sua máquina:
- Postgres
- Python 3.8

### Como utilizar este repositório

#### Executando o Postgres via Docker
Para rodar o Postgres através do Docker, é necessário ter o Docker instalado em sua máquina. Siga as instruções [aqui](https://docs.docker.com/get-docker/) para a instalação. Após a instalação, execute o seguinte comando na raiz do projeto:

```bash
docker-compose up -d
```

Este comando irá baixar a imagem do Postgres e iniciar o container. Para verificar se o container está em execução, utilize o comando:

```bash
docker ps
```

Se o container estiver em execução, a saída será semelhante a esta:

```bash
CONTAINER ID   IMAGE          COMMAND                  CREATED         STATUS         PORTS      NAMES
ididididiidid   postgres:13.3  "docker-entrypoint.s…"   2 minutes ago   Up 2 minutes   5432/tcp   postgres
```

#### Executando o Postgres na sua máquina
Para rodar o Postgres em sua máquina, é necessário ter o Postgres instalado. Siga as instruções [aqui](https://www.postgresql.org/download/) para a instalação.

#### Executando o projeto

Com o Postgres em execução, configure o arquivo 'database.ini' com as informações do seu banco de dados. O arquivo deve ter o seguinte formato:

```ini
[postgresql]
host=localhost
database=postgres
user='coloque seu usuário aqui'
password='coloque sua senha aqui'
```

Após configurar o arquivo, crie um ambiente virtual e instale as dependências do projeto executando os seguintes comandos:

```bash
pip install -r requirements.txt
```
#### Crianção e população do banco de dados
Para essa etapa, é necessário ter o para a população do banco o arquivo amazon-meta.txt na raiz do projeto (ou passa o caminho em que esta o arquivo em relação ao projeto, ao rodar o script). Este arquivo pode ser baixado [aqui](http://snap.stanford.edu/data/amazon-meta.html).

Também é necessário ter o arquivo 'database.ini' configurado corretamente e as dependências do projeto instaladas.
Para criar o banco de dados e popular as tabelas, execute o seguinte comando:
```bash
python scripts/tp1_3.2.py
```

Existe a opção de criar um banco de dados padrão chamado "amazon". Para isso, execute o seguinte comando:
```bash
python scripts/tp1_3.2.py create
```

Se o arquivo da população não estiver na raiz do projeto, é possível passar o caminho do arquivo como argumento. Para isso, execute o seguinte comando:
```bash
python scripts/tp1_3.2.py <caminho do arquivo>
```

Agora, para iniciar o dashboard, execute o seguinte comando:

```bash
python scripts/tp1_3.3.py
```

## Copiando esse repositorio
Você deve ter uma conta no github. A criação de contas é gratis e o GitHub é importante para sua visa profissional e carreira

Para fazer isso siga esses passos:

https://user-images.githubusercontent.com/118348/229365938-48d261c8-b569-463c-bc00-462eb218b423.mp4

Para entender melhor [git e github](https://www.alura.com.br/artigos/o-que-e-git-github).

## Configurando

### Docker e Docker Compose

Instalando o [docker desktop e docker compose (Windows, Linux e Mac)](https://www.docker.com/products/docker-desktop/)

Instalando na linha de comando

[Docker](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04-pt) e [Docker Compose Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-20-04-pt)

#### Como funciona o docker compose

[Docker Compose - Explicado](https://blog.4linux.com.br/docker-compose-explicado/)

### Postgres

Criar pasta `postgres-data` na raiz do projeto. Essa pasta **não deve ser enviada** para o github.

Depois você deve subir o docker-compose com o postgres. Da primeira vez vai demorar um pouco, e fique de olho nos logs para qualquer erro.

```bash
docker-compose up -d
```

### Python

Criar o ambiente virtual

```bash
python3 -m venv .tp1
```

Ativar o ambiente virtual

```bash
source .tp1/bin/activate
```

## Usando o postgres na sua maquina

Após subir, você conseguirá conectar no banco. Ele vem vazio e você terá que preencher ele com o que o trabalho pede.

```bash
psql -h localhost -U postgres
```

As credenciais são:

```yaml
username: postgres
password: postgres
```

## Usando Python

Para instalar bibliotecas necessarias para o trabalho, use o pip [DEPOIS de ativar o ambiente](#python) virtual.

```bash
pip install <biblioteca>
```
