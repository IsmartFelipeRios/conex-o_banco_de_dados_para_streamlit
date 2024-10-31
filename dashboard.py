import streamlit as st
import pyodbc
import os
from azure import identity
import struct
import pandas as pd
from github import Github

# String de conexão
connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:ismart-server.database.windows.net,1433;Database=ismart-db;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

def query_to_parquet(query, file_name="resultado.parquet"):
    try:
        # Obter credenciais e conectar ao banco de dados
        credential = identity.DefaultAzureCredential(
            exclude_interactive_browser_credential=False
        )
        token_bytes = credential.get_token(
            "https://database.windows.net/.default"
        ).token.encode("UTF-16-LE")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        conn = pyodbc.connect(
            connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}
        )
        
        # Executar a consulta e armazenar o resultado em um DataFrame
        df = pd.read_sql_query(query, conn)
        
        # Salvar o DataFrame como arquivo parquet
        df.to_parquet(file_name, index=False)
        print(f"Arquivo salvo como {file_name}")
        
        # Fechar a conexão
        conn.close()
        
        return file_name
        
    except Exception as e:
        print(f"Erro ao executar a consulta: {e}")
        return None

def upload_to_github(query, repositorio_nome, nome_arquivo, github_token):


    # Executar a consulta e salvar o resultado em parquet
    file_path = query_to_parquet(query)

    if file_path:
        # Autenticar no GitHub
        token = Github(github_token)
        repositorio = token.get_repo(repositorio_nome)

        # Ler o arquivo parquet em modo binário
        with open(file_path, "rb") as file:
            content = file.read()

        # Cria ou atualiza o arquivo no repositório
        try:
            contents = repositorio.get_contents(nome_arquivo)
            repositorio.update_file(contents.path, "Atualizando o arquivo parquet", content, contents.sha)
            print("Arquivo atualizado com sucesso!")
        except:
            repositorio.create_file(nome_arquivo, "Criando o arquivo parquet", content)
            print("Arquivo criado com sucesso!")

# Interface do Streamlit
st.title("Atualizar Dados e Dashboard")
repositorio_nome = st.text_input("Repositório (usuário/repo)", "IsmartFelipeRios/conex-o_banco_de_dados_para_streamlit")
nome_arquivo = st.text_input("Nome do arquivo no repositório", "resultado.parquet")
consultaSQL = st.text_area("Consulta SQL", "SELECT TOP 10 Nome, RA, Projeto FROM dbo.Aluno WHERE Projeto LIKE 'Ensino Superior'")

github_token = st.text_input("Token do GitHub", type="password")

# Botão para executar a função de upload
if st.button("Atualizar"):
    if github_token:
        upload_to_github(consultaSQL, repositorio_nome, nome_arquivo, github_token)
    else:
        st.error("Por favor, insira o token do GitHub.")

# Exibir DataFrame se existir o arquivo
try:
    st.title("DataFrame")
    df = pd.read_parquet('resultado.parquet')
    st.dataframe(df)
except FileNotFoundError:
    st.text("Sem arquivo para um DataFrame")
