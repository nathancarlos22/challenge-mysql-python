from db_connection import get_mysql_connection
import os
import pandas as pd
from mysql.connector import Error

def retrieve_data(product_code=None, store_code=None, date=None):
    # Conectar ao banco de dados
    connection = get_mysql_connection()
    if connection is None:
        return None

    try:
        cursor = connection.cursor(dictionary=True)

        # consulta SQL dinâmica
        query = "SELECT * FROM data_product_sales WHERE 1=1"
        params = []

        if product_code:
            query += " AND PRODUCT_CODE = %s"
            params.append(product_code)

        if store_code:
            query += " AND STORE_CODE = %s"
            params.append(store_code)

        if date and len(date) == 2:
            query += " AND DATE BETWEEN %s AND %s"
            params.append(date[0])
            params.append(date[1])

        # consulta com parâmetros
        cursor.execute(query, params)
        result = cursor.fetchall()

        # resultados em um DataFrame
        df = pd.DataFrame(result)
        return df

    except Error as e:
        print(f"Erro ao consultar MySQL: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexão com MySQL fechada")

# Chamada da função
# EXEMPLO DE PRODUCT_CODE 10, STORE_CODE 10 E DATA ENTRE 2019-01-01 E 2019-01-31
my_data = retrieve_data(product_code=10, store_code=10, date=['2019-01-01', '2019-01-31'])
print(my_data)
 