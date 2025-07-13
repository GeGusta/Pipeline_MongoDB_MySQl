import mysql.connector
import pandas as pd
import os
from dotenv import load_dotenv

def connect_mysql(host_name, user_name, pw):
    cnx = mysql.connector.connect(
        host = host_name,
        user = user_name,
        password = pw
    )
    print(cnx)
    return cnx

def create_cursor(cnx):
    cursor = cnx.cursor()
    return cursor

def create_database(cursor, db_name):
    cursor.execute(f"CREATE DATABASE {db_name}")
    print(f"\nBase de dados {db_name} criada")

def show_databases(cursor):
    cursor.execute("SHOW DATABASES")
    for x in cursor:
        print(x)

def create_product_table(cursor, db_name, tb_name):    
    cursor.execute(f"""
        CREATE TABLE {db_name}.{tb_name}(
                id VARCHAR(100),
                Produto VARCHAR(100),
                Categoria_Produto VARCHAR(100),
                Preco FLOAT(10,2),
                Frete FLOAT(10,2),
                Data_Compra DATE,
                Vendedor VARCHAR(100),
                Local_Compra VARCHAR(100),
                Avaliacao_Compra INT,
                Tipo_Pagamento VARCHAR(100),
                Qntd_Parcelas INT,
                Latitude FLOAT(10,2),
                Longitude FLOAT(10,2),
                
                PRIMARY KEY (id));
    """)
                   
    print(f"\nTabela {tb_name} criada")

def show_tables(cursor, db_name):
    cursor.execute(f"USE {db_name}")
    cursor.execute("SHOW TABLES")
    for x in cursor:
        print(x)

def read_csv(path):
    df = pd.read_csv(path)
    return df

def add_product_data(cnx, cursor, df, db_name, tb_name):
    lista = [tuple(row) for _, row in df.iterrows()]
    sql = f"INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    cursor.executemany(sql, lista)
    print(f"\n {cursor.rowcount} dados foram inseridos na tabela {tb_name}.")
    cnx.commit()

# Carrega as variáveis do arquivo .env no ambiente de trabalho
load_dotenv()

#Obtendo o valor das variáveis de ambiente
host_name = os.getenv("DB_HOST")
user_name = os.getenv("DB_USERNAME")
pw = os.getenv("DB_PASSWORD")
print("|"*80)
print("Conectando ao banco MySQL")
# realizando a conexão com mysql
cnx = connect_mysql(host_name, user_name, pw)
cursor = create_cursor(cnx)

print("|"*80)
print("Criando a base de dados 'dbprodutos'")
# criando a base de dados
create_database(cursor, "dbprodutos")
show_databases(cursor)

print("|"*80)
print("Criando a tabela de dados 'tb_livos'")
# criando tabela
create_product_table(cursor, "dbprodutos", "tb_livros")
show_tables(cursor, "dbprodutos")

print("|"*80)
print("Lendo o CSV com as informações e adicionando no banco de dados.")
# lendo e adicionando os dados
df = read_csv("/root/Documentos/pipeline-py-mongo-mysql/Data/tb_livros.csv")
add_product_data(cnx, cursor, df, "dbprodutos", "tb_livros")

print("|"*80)
print("Criando a tabela de dados 'tb_compras_mais_2021'")
# criando tabela
create_product_table(cursor, "dbprodutos", 'tb_compras_mais_2021')
show_tables(cursor, "dbprodutos")

print("|"*80)
print("Lendo o CSV com as informações e adicionando no banco de dados.")
# lendo e adicionando os dados
df = read_csv("/root/Documentos/pipeline-py-mongo-mysql/Data/compras_2021_mais.csv")
add_product_data(cnx, cursor, df, "dbprodutos", 'tb_compras_mais_2021')