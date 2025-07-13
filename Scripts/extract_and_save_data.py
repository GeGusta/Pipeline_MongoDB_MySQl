from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
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

def create_connect_db(client, db_name):
    db = client[db_name]
    return db

def create_connect_collection(db, col_name):
    collection = db[col_name]
    return collection

def extract_api_data(url):
    return requests.get(url).json()

def insert_data(col, data):
    docs = col.insert_many(data)
    n_docs_inseridos = len(docs.inserted_ids)
    return n_docs_inseridos

# Carrega as variáveis do arquivo .env no ambiente de trabalho
load_dotenv()

#Obtendo o valor das variáveis de ambiente
uri = os.getenv("MONGODB_URI")

print("|"*80)
print("Conectando ao banco.")
#Conectando ao mongo, criando o banco e a colecao.
client = connect_mongo(uri)
db = create_connect_db(client, "db_produtos")
col = create_connect_collection(db, "produtos")

print("|"*80)
print("Extraindo dados API.")
#Extraindo dados da API
data = extract_api_data("https://labdados.com/produtos")
print(f"\nQuantidade de dados extraidos: {len(data)}")
print("|"*80)
print("Inserindo dados na colecao")
#Inserindo dados na colecao
docs = insert_data(col, data)
print(f"\nQuantidade de documentos inseidos: {docs}")

client.close()