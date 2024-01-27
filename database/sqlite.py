import sqlite3
from utils.data_operations import read_csv

conn = sqlite3.connect('database/db.db')


df = read_csv("data/INMET_SE_SP_A701_SAO PAULO - MIRANTE_01-01-2023_A_31-12-2023.CSV")

df.to_sql('weather', conn, if_exists='replace', index=False)

# Verifique se a tabela foi criada corretamente
cursor = conn.cursor()

# Adicione este trecho antes de qualquer outra consulta
with sqlite3.connect('database/db.db') as conn:
    cursor = conn.cursor()
    # Obtém uma lista de todas as tabelas no banco de dados
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

# Exibe o nome de todas as tabelas
print("Tabelas disponíveis:")
for table in tables:
    print(table[0])

print(cursor.fetchall())
# Adicione este trecho depois de obter a lista de tabelas
table_name = 'cidade'
cursor.execute(f"PRAGMA table_info({table_name});")
table_info = cursor.fetchall()

# Exibe informações sobre a tabela 'clima'
print(f"Informações sobre a tabela '{table_name}':")
for info in table_info:
    print(info)
