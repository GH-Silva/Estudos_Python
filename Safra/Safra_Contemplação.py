import os
import pandas as pd
import pyodbc as odbc
from datetime import datetime
from re import match

# Pega nome do usuário
usuario = os.getenv("USERNAME") or os.getlogin()

# Conexão com o banco de dados
path = fr'CAMINHO DO ARQUIVO'
os.chdir(path)
df_senhas = pd.read_excel('NOME_DO_ARQUIVO_SENHA.xlsx')
server, database, username, password = df_senhas.iloc[0, 0:4]

conn = odbc.connect(
    f'DRIVER={{BANCO DE DADOS}};'
    f'SERVER={server};DATABASE={database};UID={username};PWD={password}'
)

# Consulta para buscar a data de referência
script = "SELECT MAX(Data_Ref) FROM Base_Historica"
df = pd.read_sql(script, conn)
data_retirar = str(df.iloc[0, 0])

# Consulta principal
script = """
SELECT
    c.Data_Contemplacao,
    a.Sit_Contemplacao,
    a.Data_Ref,
    a.Data_Aloc,
    a.ID_Cota,
    a.Macrosituacao,
    b.Sit_Contrato,
    b.Data_Cancel,
    a.Grana_Total
FROM Base_Historica a
LEFT JOIN Alocacoes b ON a.ID_Cota = b.ID_Cota
LEFT JOIN Contemplacao c ON a.ID_Cota = c.ID_Cota
WHERE a.Data_Aloc != ? AND a.Data_Aloc >= '2024-01-01'
"""
df = pd.read_sql(script, conn, params=[data_retirar])

# Consulta de cancelamentos
script = """
SELECT
    c.Data_Contemplacao,
    a.ID_Cota,
    a.Data_Aloc,
    a.Data_Cancel,
    a.Sit_Contrato,
    a.Grana_Total
FROM Alocacoes a
LEFT JOIN Contemplacao c ON a.ID_Cota = c.ID_Cota AND a.Versao = c.Versao
WHERE a.Data_Aloc >= '2024-01-01'
"""
df2 = pd.read_sql(script, conn)

# Consulta de motivos de cancelamento
script = """
SELECT ID_Cota, Data_Cancel, Motivo_Cancelamento
FROM CancelamentoDECotas
"""
df3 = pd.read_sql(script, conn)

# Junta cancelamentos com motivos
df2 = df2.merge(df3, how='left', on=['ID_Cota', 'Data_Cancel'])
df2 = pd.concat([
    df2[df2['Motivo_Cancelamento'].isna()],
    df2[df2['Motivo_Cancelamento'] == 'INADIMPLÊNCIA']
]).drop_duplicates(subset=['ID_Cota'])

# Análise de inadimplência
df_final = df.copy()
df_final['Data_Ref'] = pd.to_datetime(df_final['Data_Ref'])
df_final['Data_Contemplacao'] = pd.to_datetime(df_final['Data_Contemplacao'])
df_final['Ano_Mes_Contemplacao'] = df_final['Data_Contemplacao'].dt.to_period('M')
df_final['Inadimplente'] = df_final['Macrosituacao'] == 'Inadimplente'

# Calcula diferença de meses entre referência e contemplação
df_final['Mes_Diff'] = df_final.apply(
    lambda row: (row['Data_Ref'].year - row['Data_Contemplacao'].year) * 12 +
                (row['Data_Ref'].month - row['Data_Contemplacao'].month),
    axis=1
)

# Matriz de inadimplência
matriz = pd.pivot_table(
    df_final,
    values='Grana_Total',
    index='Ano_Mes_Contemplacao',
    columns='Mes_Diff',
    aggfunc=lambda x: (x[df_final.loc[x.index, 'Inadimplente']].sum() / x.sum()) * 100
).round(1)

# Renomeia apenas as colunas de Mes_Diff
matriz.columns = ['M' + str(int(col)) for col in matriz.columns]

# Reset index e renomeia a coluna da safra
matriz = matriz.reset_index()
matriz = matriz.rename(columns={'Ano_Mes_Contemplacao': 'Safra Contemplacao'})

# Remove safra específica
matriz = matriz[matriz['Safra Contemplacao'] != '2025-03']

# Filtra colunas desejadas
colunas_desejadas = ['Safra Contemplacao'] + [f'M{i}' for i in range(1, 16)]
matriz = matriz[[col for col in colunas_desejadas if col in matriz.columns]]

# Remove último valor de M2 até M15
for col in matriz.columns:
    if match(fr'M([2-9]|1[0-5])$', col):
        idx = matriz[col].last_valid_index()
        if pd.notna(matriz.at[idx, col]):
            matriz.at[idx, col] = None

# Nome do arquivo com data
data_hoje = datetime.today().strftime('%Y-%m-%d')
nome_arquivo = f'Matriz_Inadimplencia_Safra_{data_hoje}.xlsx'

# Caminho de destino
pasta_destino = fr'CAMINHO PARA SALVAR O ARQUIVO'
caminho_completo = os.path.join(pasta_destino, nome_arquivo)

# Salvar Excel
try:
    matriz.to_excel(caminho_completo, index=False)
    print(f"✅ Arquivo salvo com sucesso em: {caminho_completo}")
except Exception as e:
    print(f"❌ Erro ao salvar o arquivo: {e}")



