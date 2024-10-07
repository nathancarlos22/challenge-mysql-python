from db_connection import get_mysql_connection
from mysql.connector import Error

def connect_to_mysql():
    # Conectar ao banco de dados
    connection = get_mysql_connection()
    if connection is None:
        return None

    try:
        cursor = connection.cursor()

        # Consultas SQL
        # 1. Quais são os 10 produtos mais caros da empresa?
        print("\n1. Os 10 produtos mais caros:")
        cursor.execute("""
            SELECT PRODUCT_NAME, PRODUCT_VAL
            FROM data_product
            ORDER BY PRODUCT_VAL DESC
            LIMIT 10
        """)
        products = cursor.fetchall()
        for product in products:
            print(f"Produto: {product[0]}, Preço: {product[1]}")

        # 2. Quais seções os departamentos de 'BEBIDAS' e 'PADARIA' possuem?
        print("\n2. Seções dos departamentos 'BEBIDAS' e 'PADARIA':")
        cursor.execute("""
            SELECT DISTINCT SECTION_NAME
            FROM data_product
            WHERE DEP_NAME IN ('BEBIDAS', 'PADARIA');
        """)

        tables = cursor.fetchall()

        sections = []
        for table in tables:
            print(table[0])

        # 3. Total de vendas de produtos (em $) de cada Área de Negócio no primeiro trimestre de 2019
        print("\n3. Total de vendas de produtos (em $) de cada Área de Negócio no 1º trimestre de 2019:")
        cursor.execute("""
            SELECT BUSINESS_NAME, SUM(dss.SALES_VALUE) AS TOTAL_SALES
            FROM data_store_cad s
            JOIN data_store_sales dss ON s.STORE_CODE = dss.STORE_CODE
            WHERE dss.DATE BETWEEN '2019-01-01' AND '2019-03-31'
            GROUP BY s.BUSINESS_NAME

        """)
        sales = cursor.fetchall()
        for sale in sales:
            print(f"Área de Negócio: {sale[0]}, Total de Vendas: {sale[1]}")

    except Error as e:
        print(f"Erro ao consultar ao MySQL: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexão com MySQL fechada")

# Chamando a função de conexão
connect_to_mysql()

