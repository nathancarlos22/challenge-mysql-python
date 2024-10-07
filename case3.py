from db_connection import get_mysql_connection
from case2 import execute_query
from mysql.connector import Error
import pandas as pd
import matplotlib.pyplot as plt

query = """
SELECT
    Id,
    Title,
    Genre,
    Director,
    Actors,
    Year,
    Runtime,
    Rating,
    Votes,
    RevenueMillions,
    Metascore

FROM
    IMDB_movies
"""

df = execute_query(query)

# gráfico de dispersão comparando receita e avaliação
plt.figure(figsize=(10, 6))
plt.scatter(df['Rating'], df['RevenueMillions'], alpha=0.7, c=df['Votes'], cmap='viridis')
plt.title('Receita vs Avaliação dos Filmes')
plt.xlabel('Avaliação (Rating)')
plt.ylabel('Receita (Milhões)')
plt.colorbar(label='Número de Votos')
plt.tight_layout()
plt.show()

'''Esse gráfico ajuda a entender a correlação entre a receita e a avaliação dos filmes
    Filmes mais bem avaliados geram mais receita ?
    O gráfico mostra que sim, os filmes com maiores avaliações como 7, 8, 9 tem mais receita

'''