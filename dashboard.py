import streamlit as st
import pyodbc
from azure import identity
import struct
import pandas as pd
import requests
import os
from dotenv import load_dotenv
from github import Github

# String de conexão
connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:ismart-server.database.windows.net,1433;Database=ismart-db;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

# Autenticação usando o token do GitHub
github_token = "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"
g = Github(github_token)

consultaSQL = "SELECT TOP 10 Nome, RA, Projeto FROM dbo.Aluno WHERE Projeto LIKE 'Ensino Superior'"

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

# Executar a consulta e salvar o resultado em parquet
file_path = query_to_parquet(consultaSQL)

if file_path:
    # Seleciona o repositório (use o formato "usuario/nome_repositorio")
    repo = g.get_repo("IsmartFelipeRios/conex-o_banco_de_dados_para_streamlit")

    # Caminho no repositório e mensagem de commit
    repo_path = "resultado.parquet"  # Caminho do arquivo no repositório

    # Ler o arquivo parquet em modo binário
    with open(file_path, "rb") as file:
        content = file.read()

    # Cria ou atualiza o arquivo no repositório
    try:
        contents = repo.get_contents(repo_path)
        repo.update_file(contents.path, "Atualizando o arquivo parquet", content, contents.sha)
        print("Arquivo atualizado com sucesso!")
    except:
        repo.create_file(repo_path, "Criando o arquivo parquet", content)
        print("Arquivo criado com sucesso!")
