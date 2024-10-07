from db_connection import get_mysql_connection
from mysql.connector import Error
import pandas as pd

def execute_query(query):
    connection = get_mysql_connection()
    if connection is None:
        return None

    try:
        cursor = connection.cursor(dictionary=True)

        cursor.execute(query)
        result = cursor.fetchall()

        df = pd.DataFrame(result)
        return df
    
    except Error as e:
        print(f"Erro ao consultar MySQL: {e}")
        return None
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexão com MySQL fechada")


# CONSULTAS
query1 = """
SELECT 
    STORE_CODE,
    STORE_NAME,
    START_DATE,
    END_DATE,
    BUSINESS_NAME,
    BUSINESS_NAME,
    BUSINESS_CODE
FROM data_store_cad;
"""


query2 = """
SELECT 
    STORE_CODE,
    DATE,
    SALES_VALUE,
    SALES_QTY
FROM data_store_sales
WHERE DATE BETWEEN '2019-01-01' AND '2019-12-31';
"""

# Executando
data_store_cad = execute_query(query1)
data_store_sales = execute_query(query2)

# convertendo para datetime
data_store_sales['DATE'] = pd.to_datetime(data_store_sales['DATE'])

# Filtrando o dataframe da segunda consuta pelo periodo 
filtered_sales = data_store_sales[
    (data_store_sales['DATE'] >= '2019-10-01') &
    (data_store_sales['DATE'] <= '2019-12-31')
]

# Exibindo
print("Consulta 1: data_store_cad")
print(data_store_cad)

print("\nConsulta 2: filtered_sales (após aplicar o filtro de data)")
print(filtered_sales)

# Unir as tabelas com base na coluna STORE_CODE
merged_data = pd.merge(data_store_cad, filtered_sales, on='STORE_CODE')

# pegando a media das taxas de movimentações
merged_data['Taxa de Movimentação'] = merged_data['SALES_VALUE'] / merged_data['SALES_QTY']
taxa_movimentacao = merged_data.groupby(['STORE_NAME', 'BUSINESS_NAME']).agg({'Taxa de Movimentação': 'mean'}).reset_index()

# arredondando ate duas casas
data_visualization = taxa_movimentacao.round(2)

print("Visualização da tabela pedida (Loja, Categoria e Taxa de Movimentação)")

data_visualization.columns = ['Loja', 'Categoria', 'TM']
print(taxa_movimentacao.round(2))

