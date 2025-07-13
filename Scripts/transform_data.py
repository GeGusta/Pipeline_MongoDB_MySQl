import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv

def connect_mongo(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client

def visualize_collection(col):
    for doc in col.find():
        print(doc)

def rename_column(col, col_name, new_name):
    col.update_many({}, {"$rename": {f"{col_name}": f"{new_name}"}})

def select_category(col, category):
    query = { "Categoria do Produto": f"{category}"}

    lista_categoria = []
    for doc in col.find(query):
        lista_categoria.append(doc)

    return lista_categoria

def make_regex(col, regex):
    query = {"Data da Compra": {"$regex": f"{regex}"}}

    lista_regex = []
    for doc in col.find(query):
        lista_regex.append(doc)
    
    return lista_regex

def create_dataframe(lista):
    df =  pd.DataFrame(lista)
    return df

def format_date(df):
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y")
    df['Data da Compra'] = df['Data da Compra'].dt.strftime('%Y-%m-%d')

def save_csv(df, path):
    df.to_csv(path, index=False)
    print(f"\nO arquivo {path} foi salvo")

# Carrega as variáveis do arquivo .env no ambiente de trabalho
load_dotenv()

#Obtendo o valor das variáveis de ambiente
uri = os.getenv("MONGODB_URI")

# estabelecendo a conexão e recuperando os dados do MongoDB
print("|"*80)
print("Conectando ao banco.")
client = connect_mongo(uri)
db = client["db_produtos"]
col = db["produtos"]

# renomeando as colunas de latitude e longitude

print("|"*80)
print("Renomeando as colunas lat e lon.")
rename_column(col, "lat", "Latitude")
rename_column(col, "lon", "Longitude")


# salvando os dados da categoria livros
print("|"*80)
print("Selecionando os dados da categoria Livros e salvando")
tb_livros = select_category(col, "livros")
df_livros = create_dataframe(tb_livros)
format_date(df_livros)
save_csv(df_livros, "/root/Documentos/pipeline-py-mongo-mysql/Data/tb_livros.csv")

# salvando os dados dos produtos vendidos a partir de 2021
print("|"*80)
print("Selecionando os dados de compra com ano maior que 2020 e salvando")
tb_produtos = make_regex(col, "/202[1-9]")
df_produtos = create_dataframe(tb_produtos)
format_date(df_produtos)
save_csv(df_produtos, "/root/Documentos/pipeline-py-mongo-mysql/Data/compras_2021_mais.csv")

client.close()