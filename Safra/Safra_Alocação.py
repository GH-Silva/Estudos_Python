# Bibliotecas importadas
import os
import re
import pandas as pd
import pyodbc as odbc

# Pega usuário de rede
usuario = os.getenv("USERNAME")

# Conexão com o DW
path = fr'CAMINHO DO ARQUIVO COM O LOGIN E SENHA'
os.chdir(path)
df_senhas = pd.read_excel('NOME_DO_ARQUIVO.xlsx')
server = df_senhas.iloc[0,0]
database = df_senhas.iloc[0,1]
username = df_senhas.iloc[0,2]
password = df_senhas.iloc[0,3]
conn = odbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')



# Base 1 - Cálculo de Inadimplência
# Seleciona a última data da base -> ela não entra na análise
script = """
select max(Data_Ref) from Base_Historica
"""
df = pd.read_sql(script, conn)
data_retirar = str(df.iloc[0,0])

script = """
SELECT
    a.Data_Ref,
    a.Data_Aloc,
    a.ID_Cota,
    a.Macrosituacao,
    b.Sit_Contrato,
    b.Data_Cancel,
    a.Grana_Total
FROM Base_Historica a
    left join Alocacoes b on a.ID_Cota = b.ID_Cota
where a.Data_Ref != ?
and a.Data_Aloc >= '2024-01-01'
"""
# Carregando a base
df = pd.read_sql(script, conn, params = [data_retirar])

# Base 2 - Cálculo de Cancelamento
script = """
select
    a.ID_Cota,
    a.Data_Aloc,
    a.Data_Cancel,
    a.Sit_Contrato,
    a.Grana_Total
from Alocacoes a
    where a.Data_Aloc >= '2024-01-01'
"""
df2 = pd.read_sql(script, conn)

# Base 3 - Motivo Cancelamento
script = """
select
    a.ID_Cota,
    a.Data_Cancel,
    a.Motivo_Cancelamento
from Cancelamento a
"""
df3 = pd.read_sql(script, conn)

# União do df2 e df3
df2 = df2.merge(df3, how='left', on=['ID_Cota', 'Data_Cancel'])
x1 = df2[df2['Motivo_Cancelamento'].isna()]
x2 = df2[df2['Motivo_Cancelamento'] == 'INADIMPLÊNCIA']
df2 = pd.concat([x1, x2])
df2 = df2.drop_duplicates(subset=['ID_Cota'])




# Análise de Inadimplência por Safra
df_final = df.copy()

# Convertendo as colunas para datetime
df_final['Data_Ref'] = pd.to_datetime(df_final['Data_Ref'])
df_final['Data_Aloc'] = pd.to_datetime(df_final['Data_Aloc'])

# Criando uma coluna de ano-mês para 'DT_Alocacao' e 'DT_Referencia'
df_final['Ano_Mes_Alocacao'] = df_final['Data_Aloc'].dt.to_period('M')
df_final['Ano_Mes_Referencia'] = df_final['Data_Aloc'].dt.to_period('M')

# Vamos agora calcular o percentual de inadimplentes para cada combinação
# Primeiramente, criaremos uma coluna para marcar se está "Inadimplente"
df_final['Inadimplente'] = df_final['Macrosituacao'] == 'Inadimplente'

# Função para calcular a diferença em meses entre 'DT_Alocacao' e 'DT_Referencia'
def calcular_mes_diff(row):
    return (row['Data_Ref'].year - row['Data_Aloc'].year) * 12 + row['Data_Ref'].month - row['Data_Aloc'].month

# Adiciona a coluna com a diferença em meses
df_final['Mes_Diff'] = df_final.apply(calcular_mes_diff, axis=1)

# Agora vamos criar a matriz
# A ideia é calcular, para cada 'DT_Alocacao', o percentual de inadimplência para cada 'DT_Referencia'

# Matriz de percentual de inadimplentes por mês de DT_Alocacao e DT_Referencia
matriz = pd.pivot_table(df_final, 
                       values='Grana_Total', 
                       index='Ano_Mes_Alocacao', 
                       columns='Mes_Diff', 
                       aggfunc=lambda x: (x[df_final['Inadimplente']].sum() / x.sum()) * 100)

# Arredondando para duas casas decimais
matriz = matriz.round(1)

# Retirando M0
#matriz = matriz.drop(columns=['M0'])

# Renomeando as colunas para 'M1', 'M2', 'M3', etc.
matriz.columns = ['M' + str(i) for i in matriz.columns]

# Retirando o index
matriz = matriz.reset_index()

# Substituindo NaN por "-"
matriz = matriz.fillna('-')

# Retirando coluna M0
matriz = matriz.drop(columns=['M0'])

# Tratando coluna final
matriz = matriz.rename(columns = {'Ano_Mes_Alocacao': 'Safra Alocação'})




