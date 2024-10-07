import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os


# Carregar variáveis de ambiente do .env
load_dotenv()

def get_mysql_connection():
    try:
        # Configurar a conexão com o banco de dados
        connection = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST'),
            database=os.getenv('MYSQL_DATABASE'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD')
        )

        if connection.is_connected():
            print("Conexão com MySQL estabelecida com sucesso.")
            return connection
    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None