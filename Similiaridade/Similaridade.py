import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.metrics.pairwise import cosine_similarity

# Carregar os dados
df = pd.read_excel('Dados.xlsx')  # Adicionar a extensão .xlsx

# Mostrar as primeiras linhas do dataframe
print(df.head())

# Criar a matriz de utilidade
matriz_utilizade = df.pivot_table(index='técnico', columns='time', values='rating', fill_value=0)

# Mostrar as primeiras linhas da matriz de utilidade
print(matriz_utilizade.head())

# Calcular a similaridade usando o cosseno
matriz_similiaridade = cosine_similarity(matriz_utilizade)

# Converter a matriz de similaridade em um DataFrame para fácil manipulação
df_similiaridade = pd.DataFrame(matriz_similiaridade, index=matriz_utilizade.index, columns=matriz_utilizade.index)

# Mostrar as primeiras linhas do DataFrame de similaridade
print(df_similiaridade.head())

# Representar os dados de similaridade em forma de gráfico de calor
plt.figure(figsize=(12, 10))  # Aumentar o tamanho da figura
sns.heatmap(df_similiaridade, annot=True, fmt=".2%", cmap='RdYlGn', cbar_kws={'format': '%.0f%%'}, annot_kws={"size": 8})
plt.title('Matriz de Similaridade entre Técnicos', fontsize=16)
plt.xlabel('Técnico', fontsize=14)
plt.ylabel('Técnico', fontsize=14)
plt.xticks(rotation=45, ha='right', fontsize=10)  # Rotacionar os labels do eixo x para melhor visualização
plt.yticks(rotation=0, fontsize=10)  # Ajustar os labels do eixo y
plt.tight_layout()  # Ajustar layout para evitar cortes
plt.show()
