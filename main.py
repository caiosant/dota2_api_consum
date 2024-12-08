import requests
import pandas as pd
import psycopg2
import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Carregar variáveis de ambiente
load_dotenv(override=True)

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)

# URL da API
url = "https://api.opendota.com/api/players/21273055"

# Fazer a requisição para a API
resp = requests.get(url)
data = resp.json()

# Extrair informações relevantes
user_profile_data = data.get("profile", {})
user_nickname = user_profile_data.get("personaname")
user_avatar = user_profile_data.get("avatarmedium")
user_country = user_profile_data.get("loccountrycode")
user_steamid = user_profile_data.get("steamid")
user_profile = user_profile_data.get("profileurl")
created_at = time.strftime('%Y-%m-%d %H:%M:%S')
updated_at = time.strftime('%Y-%m-%d %H:%M:%S')

# Organizar os dados em um dicionário
user_data2 = {
    'nickname': user_nickname,
    'avatar': user_avatar,
    'country': user_country,
    'steam_id': user_steamid,
    'profile': user_profile,
    'created_at': created_at,
    'updated_at': updated_at
}

def create_connection():
    """Cria uma conexão com o banco de dados PostgreSQL."""
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    return conn

def setup_database(conn):
    """Cria a tabela de usuários se ela não existir."""
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            nickname TEXT,
            avatar TEXT,
            country TEXT,
            steam_id TEXT,
            profile TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    ''')
    conn.commit()
    cursor.close()
    
def save_to_database(conn, data, table_name='users'):
    conn = create_connection()
    setup_database(conn)
    cursor = conn.cursor()
      
    
    if not data['steam_id']:
        print('Dados Nulos')
    else:
        sql_exist = """
                SELECT EXISTS (
                                SELECT 1 FROM users WHERE steam_id = %s
                            )
            """
            
        sql_update = """
                UPDATE users
                SET nickname = %s, 
                    avatar = %s,
                    country = %s,
                    profile = %s,
                    updated_at = %s
                WHERE steam_id = %s AND (nickname != %s OR avatar != %s OR country != %s OR profile != %s)
            """
        
        cursor.execute(sql_exist, (data['steam_id'],))
        exists = cursor.fetchone()[0]
        
        if exists:
            print(f"Steam_id '{data['steam_id']}' já existe no banco de dados.")
            cursor.execute(sql_update, (data['nickname'], data['avatar'], data['country'], data['profile'], data['updated_at'], data['steam_id'],data['nickname'],data['avatar'], data['country'], data['profile'],))
            print(f"Steam_id '{data['steam_id']}' Atualizado com sucesso.")
        else:
            df = pd.DataFrame([data])  # Converte o dicionário em um DataFrame
            df.to_sql(table_name, engine, if_exists='append', index=False)  
            print(f"Steam_id '{data['steam_id']}' não foi encontrado no banco de dados.")
        
        
conn = create_connection() 
save_to_database(conn, user_data2)
